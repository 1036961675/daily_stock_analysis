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
示例:   12345678 AB         → 无颜色, 版本=AB
        12345678 RED01 AC   → 颜色=RED01, 版本=AC
```

版本排序: `AA < AB < ... < AZ < BA < BB < ... < ZZ`

### 变更归因规则

按照以下优先级判断（可同时命中多个）:

| 归因类型 | 判定维度 | 条件 |
|----------|----------|------|
| 零件新增 | prefix + color + supply_status | 前一快照中完全不存在 |
| 零件升版 | prefix + color + supply_status | 无当前版本但有更早版本 |
| 供应商切换 | prefix + color + version + supply_status | 有记录但无当前供应商 |
| 工厂切换 | prefix + color + version + vendor | 有记录但无当前工厂 |
| 价格变更 | prefix + color + factory + vendor + supply_status | 物料价或物流价不一致 |

### ETL 处理流程

```
每日快照(T) vs 前一快照(T-n)
         ↓
  [新增价格识别] + [消失价格识别]
         ↓
  [变更归因分类]  ← 5维度LEFT JOIN
         ↓
  [变更来源匹配]  ← 前一快照中按优先级匹配
         ↓
  [月份展开]      ← EXPLODE(SEQUENCE)
         ↓
  [合并写入]      ← 已有日志(更新void_date) UNION ALL 新增记录
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

通过 `change_source_pricea/priceb` 与当前价格对比即可计算变动金额。

## 运行步骤

1. **建表**: 执行 `price_change_log_ddl.sql`
2. **初始化**: 修改 `price_change_log_init.sql` 中的基准日期，执行全量初始化
3. **日常调度**: 每日调度 `price_change_log_etl.sql`

## 注意事项

- `price_source = 'SOURCING_ADD'` 类非正式价格有效期可能与正式价格重叠，在日志中两者均会被记录。归因时会正常识别为"价格变更"
- 消失的价格通过 `price_void_date` 标记消失日期，不删除历史记录
- ETL 支持重跑幂等，重复记录通过 `NOT EXISTS` 过滤
