-- =====================================================================
-- BOM 成本对比 - 查询两次快照之间的月度价格变动
-- =====================================================================
-- 场景: 每月25日对比上月与本月的 BOM 成本差异
-- 匹配键: 零件前八位 + 颜色(5位) + 工厂 + 供应商 + 采购类型
-- 参数:
--   ${prev_snapshot_date} → 上月快照日期 (如 2025-03-25)
--   ${curr_snapshot_date} → 本月快照日期 (如 2025-04-25)
--   ${target_month}       → 目标生效月份 (如 2025-04)
-- =====================================================================
--
-- 查询逻辑:
--   在 [prev_snapshot_date, curr_snapshot_date] 范围内的各分区中,
--   找出 target_month 维度上发生过变动的记录,
--   输出当日/前日价格对比和变动金额。
-- =====================================================================

WITH changes_in_range AS (
    SELECT
        part_prefix,
        part_color,
        part_version,
        material_code,
        vendor_code,
        factory_code,
        supply_status,
        effect_month,
        change_type,
        change_reason,
        snapshot_date,

        gps_price_confirm_no,
        material_price_amount,
        logistic_price_amount,
        price_source,

        prev_gps_price_confirm_no,
        prev_material_price_amount,
        prev_logistic_price_amount,
        prev_price_source,

        ROW_NUMBER() OVER (
            PARTITION BY part_prefix, part_color, factory_code, vendor_code,
                         supply_status, effect_month, change_type
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM dwd.disc_nio_dp_price_change_log
    WHERE snapshot_date >  '${prev_snapshot_date}'
      AND snapshot_date <= '${curr_snapshot_date}'
      AND effect_month  =  '${target_month}'
)

SELECT
    part_prefix,
    part_color,
    factory_code,
    vendor_code,
    supply_status,
    material_code,
    effect_month,
    change_type,
    change_reason,
    snapshot_date                            AS change_date,

    -- 当日价格
    material_price_amount                   AS curr_material_price,
    logistic_price_amount                   AS curr_logistic_price,
    price_source                            AS curr_price_source,

    -- 前日价格
    prev_material_price_amount              AS prev_material_price,
    prev_logistic_price_amount              AS prev_logistic_price,
    prev_price_source,

    -- 变动金额
    COALESCE(material_price_amount, 0)
        - COALESCE(prev_material_price_amount, 0)
                                            AS material_price_diff,
    COALESCE(logistic_price_amount, 0)
        - COALESCE(prev_logistic_price_amount, 0)
                                            AS logistic_price_diff

FROM changes_in_range
WHERE rn = 1

ORDER BY part_prefix, part_color, factory_code, vendor_code, supply_status
;


-- =====================================================================
-- 汇总视图: 按匹配键维度统计月度变动
-- =====================================================================

-- SELECT
--     part_prefix,
--     part_color,
--     factory_code,
--     vendor_code,
--     supply_status,
--     COLLECT_SET(change_type)             AS change_types,
--     COUNT(*)                             AS change_cnt,
--     SUM(material_price_diff)             AS total_material_diff,
--     SUM(logistic_price_diff)             AS total_logistic_diff
-- FROM (上述查询) t
-- GROUP BY part_prefix, part_color, factory_code, vendor_code, supply_status
-- ORDER BY ABS(total_material_diff + total_logistic_diff) DESC
-- ;


-- =====================================================================
-- 查看某日所有变动明细 (简单用法)
-- =====================================================================

-- SELECT *
-- FROM dwd.disc_nio_dp_price_change_log
-- WHERE snapshot_date = '2025-04-25'
--   AND effect_month  = '2025-04'
-- ORDER BY change_type, part_prefix, part_color
-- ;
