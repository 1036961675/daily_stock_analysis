# 价格变动日志 (Price Change Log)

## 概述

基于每日价格快照 `dwd.disc_nio_dp_price_price_1d_f`，通过 **T vs T-1 日环比** 比对，生成按月展开的价格变动日志表 `dwd.disc_nio_dp_price_change_log`，支撑 BOM 成本月度对比分析。

**核心特点**：
- **先展开月份，再比对归因** — 在 `effect_month` 粒度上做差异检测，能更清晰地看出每个月的价格变化轨迹
- **同内容换号不算变动** — 比对键为业务键（而非 `gps_price_confirm_no`），仅 gps 流水号变但价格/区间/来源完全一致时不产出记录
- **每日独立运行** — 只看当天 vs 前一天的差异，按 `snapshot_date` 分区写入，天然幂等

## 文件说明

| 文件 | 用途 |
|------|------|
| `price_change_log_ddl.sql` | 结果表建表语句（分区表） |
| `price_change_log_init.sql` | 初始化说明（新架构无需初始化数据） |
| `price_change_log_etl.sql` | 每日 ETL（核心） |
| `price_change_log_bom_query.sql` | BOM 成本对比查询示例 |

## 核心设计

### 物料号结构

```
零件号 = 前缀(8位) + [颜色(5位)] + 版本(2位大写字母)
示例:   12345678 AB         → 无颜色, 版本=AB    (总长10位)
        12345678 RED01 AC   → 颜色=RED01, 版本=AC (总长15位)
```

版本排序: `AA < AB < ... < AZ < BA < BB < ... < ZZ`（与字典序一致）

### ETL 处理流程

```
当日快照(T)                    前日快照(T-1)
     ↓                              ↓
 [按月 EXPLODE]                 [按月 EXPLODE]
     ↓                              ↓
 [业务键+月份去重]              [业务键+月份去重]
     ↓                              ↓
     └──── FULL OUTER JOIN ─────────┘
                  ↓
      [过滤: 同内容换号 → 跳过]
                  ↓
      [分类: 新增 / 消失 / 变更]
                  ↓
      [归因: 5维度 LEFT JOIN]
                  ↓
      INSERT OVERWRITE PARTITION (snapshot_date)
```

### 比对逻辑（核心变更）

**业务键**：`material_code + factory_code + vendor_code + supply_status + effect_month`

在每月粒度上做 FULL OUTER JOIN：
- **左侧（当日）有、右侧（前日）无** → `change_type = '新增'`
- **左侧无、右侧有** → `change_type = '消失'`
- **两侧均有、价格/区间/来源不同** → `change_type = '变更'`
- **两侧均有、内容完全一致**（即使 gps_price_confirm_no 不同）→ **不产出记录**

### 变更归因规则

| 归因类型 | 判定维度（每月粒度） | 条件 |
|----------|----------------------|------|
| 零件新增 | prefix + color + supply_status + month | 前日该月不存在 |
| 零件升版 | prefix + color + supply_status + month | 无当前版本但有更早版本 |
| 供应商切换 | prefix + color + version + supply_status + month | 有记录但无当前供应商 |
| 工厂切换 | prefix + color + version + vendor + month | 有记录但无当前工厂 |
| 价格变更 | 同业务键 | 物料价或物流价不一致 |

归因可多值命中，`change_reason` 为 `ARRAY<STRING>`。

消失类型的 `change_reason` 固定为 `ARRAY('消失')`。

### 结果表字段

| 字段名 | 类型 | 描述 |
|--------|------|------|
| part_prefix | STRING | 零件前8位 |
| part_color | STRING | 颜色(5位, 无颜色为空串) |
| part_version | STRING | 版本(2位, AA~ZZ) |
| material_code | STRING | 完整物料号 |
| vendor_code | STRING | 供应商编码 |
| factory_code | STRING | 工厂编码 |
| supply_status | STRING | 采购类型 Buy/CS/DB |
| effect_month | STRING | 生效月份 yyyy-MM |
| gps_price_confirm_no | STRING | 当日价格流水号 |
| effect_date | STRING | 当日生效起始日 |
| expire_date | STRING | 当日生效截止日 |
| gps_create_time | STRING | 当日 GPS 创建时间 |
| price_source | STRING | 当日价格来源 |
| material_price_amount | DECIMAL(20,4) | 当日物料价 |
| logistic_price_amount | DECIMAL(20,4) | 当日物流价 |
| prev_gps_price_confirm_no | STRING | 前日价格流水号 |
| prev_effect_date | STRING | 前日生效起始日 |
| prev_expire_date | STRING | 前日生效截止日 |
| prev_gps_create_time | STRING | 前日 GPS 创建时间 |
| prev_price_source | STRING | 前日价格来源 |
| prev_material_price_amount | DECIMAL(20,4) | 前日物料价 |
| prev_logistic_price_amount | DECIMAL(20,4) | 前日物流价 |
| change_type | STRING | 变动类型: 新增/消失/变更 |
| change_reason | ARRAY\<STRING\> | 一级归因类型 |
| snapshot_date | STRING | 分区字段, 快照比对日期 |

### 使用方式（BOM 成本对比）

每月25日查询本月发生的价格变动:

```sql
SELECT *
FROM dwd.disc_nio_dp_price_change_log
WHERE snapshot_date >  '2025-03-25'
  AND snapshot_date <= '2025-04-25'
  AND effect_month  =  '2025-04'
ORDER BY part_prefix, part_color, factory_code, vendor_code
```

**对账匹配键**: `part_prefix + part_color + factory_code + vendor_code + supply_status`

通过 `material_price_amount - prev_material_price_amount` 即可直接计算变动金额。

## 运行步骤

1. **建表**: 执行 `price_change_log_ddl.sql`
2. **日常调度**: 每日调度 `price_change_log_etl.sql`

无需初始化脚本。首日运行时若 T-1 快照不存在，所有当日价格均判定为"新增"。

## 幂等与重跑

- **天然幂等**: `INSERT OVERWRITE PARTITION (snapshot_date = '...')` 覆盖写入，同日重跑结果一致
- **分区独立**: 各 `snapshot_date` 分区互不影响，可单独重跑任意一天
- **无需初始化**: 不依赖历史累积状态

## 注意事项

- 仅 `gps_price_confirm_no` 变更但价格/区间/来源完全一致时，不产出记录
- `effect_month` 的 explode 边界为闭-闭区间（首尾月均包含），跨年自动处理
- 同一业务键+月份在同一快照中有多条记录时，取最新一条（排序键: `effect_date DESC, gps_create_time DESC, gps_price_confirm_no DESC`）
- `price_source = 'SOURCING_ADD'` 为非正式价（谈判价），与正式价可在同一组合中并存
