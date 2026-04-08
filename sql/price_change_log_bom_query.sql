-- =====================================================================
-- BOM 成本对比 - 查询两次快照之间的价格变动
-- =====================================================================
-- 场景: 每月25日对比上月与本月的BOM成本差异
-- 匹配键: 零件前八位 + 颜色号(5位) + 工厂 + 供应商
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
        material_code,
        gps_price_confirm_no,
        effect_month,
        change_reason,
        change_source_price_confirm_no,
        change_source_pricea,
        change_source_priceb,
        price_create_date,
        price_void_date,
        gps_create_time
    FROM dwd.disc_nio_dp_price_change_log
    WHERE effect_month = '${target_month}'
      AND (
            -- 在两次快照之间新产生的价格
            (price_create_date > '${prev_snapshot_date}' AND price_create_date <= '${curr_snapshot_date}')
            OR
            -- 在两次快照之间消失的价格
            (price_void_date > '${prev_snapshot_date}' AND price_void_date <= '${curr_snapshot_date}')
      )
)

SELECT
    bpc.part_prefix,
    bpc.part_color,
    bpc.factory_code,
    bpc.vendor_code,
    bpc.material_code,
    bpc.gps_price_confirm_no,
    bpc.effect_month,
    bpc.change_reason,
    bpc.price_create_date,
    bpc.price_void_date,
    -- 当前价格 (需关联源表获取, 或直接在log表添加当前价格字段)
    cur.material_price_amount        AS curr_material_price,
    cur.logistic_price_amount        AS curr_logistic_price,
    -- 变更来源价格
    bpc.change_source_pricea         AS prev_material_price,
    bpc.change_source_priceb         AS prev_logistic_price,
    -- 变动金额
    COALESCE(CAST(cur.material_price_amount AS DECIMAL(20,4)), 0)
        - COALESCE(CAST(bpc.change_source_pricea AS DECIMAL(20,4)), 0)
                                     AS material_price_diff,
    COALESCE(CAST(cur.logistic_price_amount AS DECIMAL(20,4)), 0)
        - COALESCE(CAST(bpc.change_source_priceb AS DECIMAL(20,4)), 0)
                                     AS logistic_price_diff

FROM bom_price_changes bpc
LEFT JOIN (
    SELECT gps_price_confirm_no, material_price_amount, logistic_price_amount
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime = '${curr_snapshot_date}'
) cur ON bpc.gps_price_confirm_no = cur.gps_price_confirm_no

ORDER BY bpc.part_prefix, bpc.part_color, bpc.factory_code, bpc.vendor_code
;


-- =====================================================================
-- 汇总视图: 按零件+工厂+供应商维度, 统计月度变动
-- =====================================================================
-- 可用于BOM成本分析仪表盘

-- SELECT
--     part_prefix,
--     part_color,
--     factory_code,
--     vendor_code,
--     change_reason,
--     COUNT(*)                         AS change_cnt,
--     SUM(material_price_diff)         AS total_material_diff,
--     SUM(logistic_price_diff)         AS total_logistic_diff
-- FROM (上述bom_price_changes查询) t
-- GROUP BY part_prefix, part_color, factory_code, vendor_code, change_reason
-- ORDER BY ABS(total_material_diff + total_logistic_diff) DESC
-- ;
