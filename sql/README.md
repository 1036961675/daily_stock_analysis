# 价格变动日志 (Price Change Log)

## 概述

基于每日价格快照 `dwd.disc_nio_dp_price_price_1d_f`，通过日对日差异比对，生成按月展开的价格变动日志表 `dwd.disc_nio_dp_price_change_log`，支撑 BOM 成本月度对比分析。

## 文件说明

| 文件 | 用途 |
|------|------|
| `price_change_log_ddl.sql` | 结果表建表语句 |
| `price_change_log_init.sql` | 首次全量初始化脚本 |
| `price_change_log_etl.sql` | 每日增量 ETL（核心） |
| `price_change_log_bom_query.sql` | BOM 成本对比查询示例 |

## 核心设计

### 物料号结构

```
零件号 = 前缀(8位) + [颜色(5位)] + 版本(2位大写字母)
示例:   12345678 AB         → 无颜色, 版本=AB    (总长10位)
        12345678 RED01 AC   → 颜色=RED01, 版本=AC (总长15位)
```

版本排序: `AA < AB < ... < AZ < BA < BB < ... < ZZ`（与字典序一致）

### 正式价与非正式价

| 类型 | 条件 | 业务含义 |
|------|------|----------|
| 非正式价（谈判价） | `price_source = 'SOURCING_ADD'` | 招标确定供应商后的谈判价（签合同前） |
| 正式价（合同价） | `price_source <> 'SOURCING_ADD'` | 与供应商的合同价 |

**非正式价去重规则**: 按「业务件 + 价格生效区间」唯一：
- 业务件 = `material_code + factory_code + vendor_code + supply_status`
- 区间 = `effect_date + expire_date`
- 全历史中首次出现时写入日志，后续快照重复出现则忽略

### 变更归因规则

按照以下优先级判断（可同时命中多个，建议优先级：新增/升版 > 供应商/工厂切换 > 纯价格变更）:

| 归因类型 | 判定维度 | 条件 |
|----------|----------|------|
| 零件新增 | prefix + color + supply_status | 前一快照中完全不存在 |
| 零件升版 | prefix + color + supply_status | 无当前版本但有更早版本 |
| 供应商切换 | prefix + color + version + supply_status | 有记录但无当前供应商 |
| 工厂切换 | prefix + color + version + vendor | 有记录但无当前工厂 |
| 价格变更 | prefix + color + factory + vendor + supply_status | 物料价或物流价不一致 |
| 价格续期 | - | 以上均不命中时补充 |

### price_void_date（价格消失日/接替日）

**接替规则**: 同一业务分组（`material_code + factory_code + vendor_code + supply_status`）下：
- 按排序键 `effect_date ASC, gps_create_time ASC, gps_price_confirm_no ASC` 确定后继
- 后继记录的 `price_create_date` 即为前序记录的 `price_void_date`
- 正式价从快照中消失时，`price_void_date` = 消失日期

### ETL 处理流程

```
每日快照(T) vs 前一快照(T-n)
         ↓
  [新增价格识别] + [消失价格识别]
         ↓
  [SOURCING_ADD去重] ← 按业务件+区间排除已入库记录
         ↓
  [变更归因分类]  ← 5维度 LEFT JOIN
         ↓
  [变更来源匹配]  ← 前一快照中按优先级匹配
         ↓
  [月份展开]      ← EXPLODE(SEQUENCE), 首尾月均含(闭-闭)
         ↓
  [合并写入]      ← 已有日志(更新void_date) UNION ALL 新增记录
         ↓
  [接替式void_date] ← LEAD窗口函数计算后继接替日期
```

### 使用方式（BOM 成本对比）

每月25日生成 BOM 成本时，通过如下方式查询价格变动:

```sql
SELECT *
FROM dwd.disc_nio_dp_price_change_log
WHERE effect_month = '2025-04'
  AND (
      (price_create_date > '2025-03-25' AND price_create_date <= '2025-04-25')
      OR
      (price_void_date > '2025-03-25' AND price_void_date <= '2025-04-25')
  )
```

**对账匹配键**: `SUBSTR(material_code,1,8) + 颜色(5位) + factory_code + vendor_code + supply_status`

通过 `change_source_pricea/priceb` 与 `material_price_amount/logistic_price_amount` 对比即可计算变动金额。

## 结果表字段

| 字段名 | 类型 | 业务描述 |
|--------|------|----------|
| gps_price_confirm_no | STRING | 价格流水号 |
| material_code | STRING | 物料号 |
| vendor_code | STRING | 合作伙伴编码(对应源表 gps_vendor_code) |
| factory_code | STRING | 工厂编码 |
| supply_status | STRING | 采购类型 Buy/CS/DB |
| effect_month | STRING | 价格生效月份(yyyy-MM, 由生效区间按月展开) |
| effect_date | STRING | 价格生效起始日期(源表 effect_time 截取至日) |
| expire_date | STRING | 价格生效截止日期(源表 expire_time 截取至日) |
| price_create_date | STRING | 价格产生日期(首次出现在快照中的日期) |
| price_void_date | STRING | 价格消失日期(被接替或从快照消失, NULL=当前有效) |
| gps_create_time | STRING | GPS价格创建时间(源系统录入时间) |
| price_source | STRING | 价格来源(SOURCING_ADD=非正式价, 其他=正式价) |
| material_price_amount | DECIMAL(20,4) | 物料价格 |
| logistic_price_amount | DECIMAL(20,4) | 物流价格 |
| change_reason | ARRAY\<STRING\> | 一级归因类型 |
| change_source_price_confirm_no | STRING | 变更来源价格流水号 |
| change_source_pricea | STRING | 变更来源物料价格 |
| change_source_priceb | STRING | 变更来源物流价格 |

## 运行步骤

1. **建表**: 执行 `price_change_log_ddl.sql`
2. **初始化**: 修改 `price_change_log_init.sql` 中的基准日期，执行全量初始化
3. **日常调度**: 每日调度 `price_change_log_etl.sql`

## 幂等与重跑

- **ETL 幂等**: 新增记录通过 `NOT EXISTS` 过滤已存在的 `(gps_price_confirm_no, effect_month)` 组合
- **SOURCING_ADD 幂等**: 按「业务件 + 区间」去重，同一组合仅首次入日志
- **void_date 幂等**: 每次 ETL 重新计算接替关系，确保结果一致
- **初始化 幂等**: 使用 `INSERT OVERWRITE`，可安全重跑

## 注意事项

- `price_source = 'SOURCING_ADD'` 类非正式价格的有效期可能与正式价格重叠
- 非正式价按「业务件+区间」仅首次出现时入日志，后续快照重复出现不会重复记录
- 消失的价格通过 `price_void_date` 标记消失日期，不删除历史记录
- `gps_create_time` 是源系统录入时间，`price_create_date` 是快照中首次出现日期，两者含义不同
- `effect_month` 的 explode 边界为闭-闭区间（首尾月均包含），跨年自动处理
