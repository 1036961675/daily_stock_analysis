-- =====================================================================
-- BOM 成本对比 - 查询两次快照之间的价格变动
-- =====================================================================
-- 场景: 每月25日对比上月与本月的BOM成本差异
-- 匹配键: 零件前八位 + 颜色号(5位) + 工厂 + 供应商 + 采购类型
-- 参数:
--   ${prev_snapshot_date} → 上月快照日期 (如 2025-03-25)
--   ${curr_snapshot_date} → 本月快照日期 (如 2025-04-25)
--   ${target_month}       → 价格生效月份 (如 2025-04)
-- =====================================================================

WITH bom_price_changes AS (
    SELECT
        SUBSTR(material_code, 1, 8)     AS part_prefix,
        CASE WHEN LENGTH(material_code) > 10
             THEN SUBSTR(material_code, 9, LENGTH(material_code) - 10)
             ELSE '' END                AS part_color,
        factory_code,
        vendor_code,
        supply_status,
        material_code,
        gps_price_confirm_no,
        effect_month,
        effect_date,
        expire_date,
        change_reason,
        change_source_price_confirm_no,
        change_source_pricea,
        change_source_priceb,
        price_create_date,
        price_void_date,
        gps_create_time,
        price_source,
        material_price_amount,
        logistic_price_amount
    FROM dwd.disc_nio_dp_price_change_log
    WHERE effect_month = '${target_month}'
      AND (
            (price_create_date > '${prev_snapshot_date}' AND price_create_date <= '${curr_snapshot_date}')
            OR
            (price_void_date > '${prev_snapshot_date}' AND price_void_date <= '${curr_snapshot_date}')
      )
)

SELECT
    bpc.part_prefix,
    bpc.part_color,
    bpc.factory_code,
    bpc.vendor_code,
    bpc.supply_status,
    bpc.material_code,
    bpc.gps_price_confirm_no,
    bpc.effect_month,
    bpc.effect_date,
    bpc.expire_date,
    bpc.change_reason,
    bpc.price_create_date,
    bpc.price_void_date,
    bpc.price_source,
    bpc.material_price_amount            AS curr_material_price,
    bpc.logistic_price_amount            AS curr_logistic_price,
    bpc.change_source_pricea             AS prev_material_price,
    bpc.change_source_priceb             AS prev_logistic_price,
    COALESCE(bpc.material_price_amount, 0)
        - COALESCE(CAST(bpc.change_source_pricea AS DECIMAL(20,4)), 0)
                                         AS material_price_diff,
    COALESCE(bpc.logistic_price_amount, 0)
        - COALESCE(CAST(bpc.change_source_priceb AS DECIMAL(20,4)), 0)
                                         AS logistic_price_diff

FROM bom_price_changes bpc

ORDER BY bpc.part_prefix, bpc.part_color, bpc.factory_code, bpc.vendor_code, bpc.supply_status
;


-- =====================================================================
-- 汇总视图: 按零件+工厂+供应商+采购类型维度, 统计月度变动
-- =====================================================================

-- SELECT
--     part_prefix,
--     part_color,
--     factory_code,
--     vendor_code,
--     supply_status,
--     FLATTEN(COLLECT_SET(change_reason)) AS all_reasons,
--     COUNT(*)                            AS change_cnt,
--     SUM(material_price_diff)            AS total_material_diff,
--     SUM(logistic_price_diff)            AS total_logistic_diff
-- FROM (上述查询) t
-- GROUP BY part_prefix, part_color, factory_code, vendor_code, supply_status
-- ORDER BY ABS(total_material_diff + total_logistic_diff) DESC
-- ;
