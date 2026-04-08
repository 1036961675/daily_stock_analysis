-- =====================================================================
-- 价格变动日志 - 首次全量初始化
-- =====================================================================
-- 说明: 选择一个基准日快照, 将所有价格全量插入日志表, 统一归因为"零件新增"
-- 使用: 修改下方 @init_date 为你选定的基准日期(该日期需有完整快照数据)
-- 运行完毕后, 从基准日+1开始每日调度 price_change_log_etl.sql
-- =====================================================================

-- SET @init_date = '2025-01-02'; -- 按实际情况修改

INSERT OVERWRITE TABLE dwd.disc_nio_dp_price_change_log
SELECT
    a.gps_price_confirm_no,
    a.material_code,
    a.gps_vendor_code                       AS vendor_code,
    a.factory_code,
    a.supply_status,
    DATE_FORMAT(m, 'yyyy-MM')               AS effect_month,
    SUBSTR(a.effect_time, 1, 10)            AS effect_date,
    SUBSTR(a.expire_time, 1, 10)            AS expire_date,
    a.datetime                              AS price_create_date,
    CAST(NULL AS STRING)                    AS price_void_date,
    a.create_time                           AS gps_create_time,
    a.price_source,
    CAST(a.material_price_amount AS DECIMAL(20,4))  AS material_price_amount,
    CAST(a.logistic_price_amount AS DECIMAL(20,4))  AS logistic_price_amount,
    ARRAY('零件新增')                        AS change_reason,
    CAST(NULL AS STRING)                    AS change_source_price_confirm_no,
    CAST(NULL AS STRING)                    AS change_source_pricea,
    CAST(NULL AS STRING)                    AS change_source_priceb
FROM dwd.disc_nio_dp_price_price_1d_f a
LATERAL VIEW EXPLODE(
    SEQUENCE(
        CAST(CONCAT(SUBSTR(SUBSTR(a.effect_time,1,10), 1, 7), '-01') AS DATE),
        CAST(CONCAT(SUBSTR(SUBSTR(a.expire_time,1,10), 1, 7), '-01') AS DATE),
        INTERVAL 1 MONTH
    )
) months AS m
WHERE a.datetime = '${init_date}'
  AND SUBSTR(a.effect_time, 1, 10) >= '2025-01-01'
;
