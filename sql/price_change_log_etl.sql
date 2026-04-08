-- =====================================================================
-- 价格变动日志 ETL (每日增量)
-- =====================================================================
-- 调度频率: 每日
-- 调度参数:
--   $[yyyy-MM-dd]   → 当日日期 (run_date)
--   $[yyyy-MM-dd-1] → 昨日日期
--   $[yyyy-MM-dd-4] → 4天前日期
-- 输入: dwd.disc_nio_dp_price_price_1d_f (每日价格快照)
-- 输出: dwd.disc_nio_dp_price_change_log  (价格变动日志)
-- 引擎: Spark SQL (兼容性注释见文末)
--
-- 处理逻辑:
--   1) 对比"今日快照"与"前一快照", 识别新增/消失价格
--   2) 对新增价格进行归因分类 (零件新增/升版/供应商切换/工厂切换/价格变更)
--   3) 匹配变更来源 (被替代的前一版价格)
--   4) 按价格有效月份展开 (explode)
--   5) 合并入结果表 (已有记录 + 新增记录, 并通过接替关系计算void_date)
--
-- SOURCING_ADD 处理:
--   非正式价(price_source='SOURCING_ADD')按 业务件+区间 去重,
--   全历史仅首次出现时写入日志; 后续快照重复出现则忽略。
--
-- price_void_date(接替关系):
--   同一业务分组下, 后续记录出现时, 前序记录被接替;
--   正式价从快照消失时, 亦视为消失(void_date = 消失日期)。
--   接替排序键: effect_date ASC, gps_create_time ASC, gps_price_confirm_no ASC
-- =====================================================================

INSERT OVERWRITE TABLE dwd.disc_nio_dp_price_change_log

WITH

-- =============================================================
-- 第一步: 数据准备 - 获取今日快照 & 前一快照
-- =============================================================

prev_dt AS (
    SELECT COALESCE(MAX(datetime), '1970-01-01') AS dt
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime >= '$[yyyy-MM-dd-4]'
      AND datetime <= '$[yyyy-MM-dd-1]'
),

curr AS (
    SELECT
        gps_price_confirm_no,
        material_code,
        gps_vendor_code                     AS vendor_code,
        factory_code,
        supply_status,
        SUBSTR(effect_time, 1, 10)          AS effect_date,
        SUBSTR(expire_time, 1, 10)          AS expire_date,
        create_time                         AS gps_create_time,
        price_source,
        CAST(material_price_amount  AS DECIMAL(20,4)) AS material_price_amount,
        CAST(logistic_price_amount  AS DECIMAL(20,4)) AS logistic_price_amount,
        SUBSTR(material_code, 1, 8)         AS part_prefix,
        CASE WHEN LENGTH(material_code) > 10
             THEN SUBSTR(material_code, 9, LENGTH(material_code) - 10)
             ELSE '' END                    AS part_color,
        SUBSTR(material_code, -2)           AS part_version
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime = '$[yyyy-MM-dd]'
      AND SUBSTR(effect_time, 1, 10) >= '2025-01-01'
),

prev AS (
    SELECT
        gps_price_confirm_no,
        material_code,
        gps_vendor_code                     AS vendor_code,
        factory_code,
        supply_status,
        SUBSTR(effect_time, 1, 10)          AS effect_date,
        SUBSTR(expire_time, 1, 10)          AS expire_date,
        create_time                         AS gps_create_time,
        price_source,
        CAST(material_price_amount  AS DECIMAL(20,4)) AS material_price_amount,
        CAST(logistic_price_amount  AS DECIMAL(20,4)) AS logistic_price_amount,
        SUBSTR(material_code, 1, 8)         AS part_prefix,
        CASE WHEN LENGTH(material_code) > 10
             THEN SUBSTR(material_code, 9, LENGTH(material_code) - 10)
             ELSE '' END                    AS part_color,
        SUBSTR(material_code, -2)           AS part_version
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime = (SELECT dt FROM prev_dt)
      AND SUBSTR(effect_time, 1, 10) >= '2025-01-01'
),

-- =============================================================
-- 第二步: 变动检测 - 识别新增 & 消失价格
-- =============================================================

new_prices_raw AS (
    SELECT c.*
    FROM curr c
    WHERE NOT EXISTS (
        SELECT 1 FROM prev p
        WHERE p.gps_price_confirm_no = c.gps_price_confirm_no
    )
),

-- SOURCING_ADD 去重: 排除已存在于日志中的 业务件+区间 组合
new_prices AS (
    SELECT n.*
    FROM new_prices_raw n
    WHERE NOT (
        n.price_source = 'SOURCING_ADD'
        AND EXISTS (
            SELECT 1
            FROM dwd.disc_nio_dp_price_change_log el
            WHERE el.vendor_code   = n.vendor_code
              AND el.factory_code  = n.factory_code
              AND el.material_code = n.material_code
              AND el.supply_status = n.supply_status
              AND el.effect_date   = n.effect_date
              AND el.expire_date   = n.expire_date
              AND el.price_source  = 'SOURCING_ADD'
        )
    )
),

void_ids AS (
    SELECT p.gps_price_confirm_no
    FROM prev p
    WHERE NOT EXISTS (
        SELECT 1 FROM curr c
        WHERE c.gps_price_confirm_no = p.gps_price_confirm_no
    )
),

-- =============================================================
-- 第三步: 变更归因 - 基于前一快照维度聚合判断变更类型
-- =============================================================

prev_part_versions AS (
    SELECT
        part_prefix, part_color, supply_status,
        COLLECT_SET(part_version) AS version_set
    FROM prev
    GROUP BY part_prefix, part_color, supply_status
),

prev_vendor_set AS (
    SELECT
        part_prefix, part_color, part_version, supply_status,
        COLLECT_SET(vendor_code) AS vendor_set
    FROM prev
    GROUP BY part_prefix, part_color, part_version, supply_status
),

prev_factory_set AS (
    SELECT
        part_prefix, part_color, part_version, vendor_code,
        COLLECT_SET(factory_code) AS factory_set
    FROM prev
    GROUP BY part_prefix, part_color, part_version, vendor_code
),

prev_price_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY part_prefix, part_color, factory_code, vendor_code, supply_status
            ORDER BY effect_date DESC, gps_create_time DESC, gps_price_confirm_no DESC
        ) AS rn
    FROM prev
),

new_classified AS (
    SELECT
        n.*,

        CASE WHEN pv.part_prefix IS NULL
             THEN 1 ELSE 0
        END AS is_part_new,

        CASE WHEN pv.part_prefix IS NOT NULL
                  AND NOT ARRAY_CONTAINS(pv.version_set, n.part_version)
                  AND SIZE(FILTER(pv.version_set, v -> v < n.part_version)) > 0
             THEN 1 ELSE 0
        END AS is_version_upgrade,

        CASE WHEN vs.part_prefix IS NOT NULL
                  AND NOT ARRAY_CONTAINS(vs.vendor_set, n.vendor_code)
             THEN 1 ELSE 0
        END AS is_vendor_switch,

        CASE WHEN fs.part_prefix IS NOT NULL
                  AND NOT ARRAY_CONTAINS(fs.factory_set, n.factory_code)
             THEN 1 ELSE 0
        END AS is_factory_switch,

        CASE WHEN pr.part_prefix IS NOT NULL
                  AND (COALESCE(pr.material_price_amount, 0) != COALESCE(n.material_price_amount, 0)
                    OR COALESCE(pr.logistic_price_amount, 0) != COALESCE(n.logistic_price_amount, 0))
             THEN 1 ELSE 0
        END AS is_price_change

    FROM new_prices n

    LEFT JOIN prev_part_versions pv
        ON  n.part_prefix   = pv.part_prefix
        AND n.part_color    = pv.part_color
        AND n.supply_status = pv.supply_status

    LEFT JOIN prev_vendor_set vs
        ON  n.part_prefix   = vs.part_prefix
        AND n.part_color    = vs.part_color
        AND n.part_version  = vs.part_version
        AND n.supply_status = vs.supply_status

    LEFT JOIN prev_factory_set fs
        ON  n.part_prefix   = fs.part_prefix
        AND n.part_color    = fs.part_color
        AND n.part_version  = fs.part_version
        AND n.vendor_code   = fs.vendor_code

    LEFT JOIN prev_price_ranked pr
        ON  n.part_prefix   = pr.part_prefix
        AND n.part_color    = pr.part_color
        AND n.factory_code  = pr.factory_code
        AND n.vendor_code   = pr.vendor_code
        AND n.supply_status = pr.supply_status
        AND pr.rn = 1
),

-- =============================================================
-- 第四步: 变更来源匹配 - 找到被替代/对比的前一版价格
-- =============================================================
-- 优先级:
--   1. 同(prefix+color+factory+vendor+supply) → 价格变更来源
--   2. 同(prefix+color+version+vendor)不同factory → 工厂切换来源
--   3. 同(prefix+color+version+supply)不同vendor → 供应商切换来源
--   4. 同(prefix+color+supply) 更早版本 → 零件升版来源
change_source_ranked AS (
    SELECT
        n.gps_price_confirm_no,
        p.gps_price_confirm_no              AS src_id,
        CAST(p.material_price_amount AS STRING) AS src_pricea,
        CAST(p.logistic_price_amount AS STRING) AS src_priceb,
        ROW_NUMBER() OVER (
            PARTITION BY n.gps_price_confirm_no
            ORDER BY
                CASE
                    WHEN p.factory_code  = n.factory_code
                     AND p.vendor_code   = n.vendor_code
                     AND p.supply_status = n.supply_status THEN 1
                    WHEN p.part_version  = n.part_version
                     AND p.vendor_code   = n.vendor_code   THEN 2
                    WHEN p.part_version  = n.part_version
                     AND p.supply_status = n.supply_status THEN 3
                    ELSE 4
                END,
                p.effect_date     DESC,
                p.gps_create_time DESC,
                p.gps_price_confirm_no DESC
        ) AS rn
    FROM new_classified n
    JOIN prev p
        ON  n.part_prefix = p.part_prefix
        AND n.part_color  = p.part_color
    WHERE n.is_part_new = 0
      AND (
            (p.factory_code = n.factory_code AND p.vendor_code = n.vendor_code AND p.supply_status = n.supply_status)
         OR (p.part_version = n.part_version AND p.vendor_code = n.vendor_code)
         OR (p.part_version = n.part_version AND p.supply_status = n.supply_status)
         OR (p.supply_status = n.supply_status AND p.part_version < n.part_version)
      )
),

best_source AS (
    SELECT gps_price_confirm_no, src_id, src_pricea, src_priceb
    FROM change_source_ranked
    WHERE rn = 1
),

-- =============================================================
-- 第五步: 月份展开 - 按价格有效期逐月展开
-- =============================================================
-- 边界规则: 首尾月均包含(闭-闭), 基于 SEQUENCE 按自然月生成

new_exploded AS (
    SELECT
        nc.gps_price_confirm_no,
        nc.material_code,
        nc.vendor_code,
        nc.factory_code,
        nc.supply_status,
        DATE_FORMAT(m, 'yyyy-MM')           AS effect_month,
        nc.effect_date,
        nc.expire_date,
        '$[yyyy-MM-dd]'                     AS price_create_date,
        CAST(NULL AS STRING)                AS price_void_date,
        nc.gps_create_time,
        nc.price_source,
        nc.material_price_amount,
        nc.logistic_price_amount,
        FILTER(ARRAY(
            IF(nc.is_part_new        = 1, '零件新增',   NULL),
            IF(nc.is_version_upgrade = 1, '零件升版',   NULL),
            IF(nc.is_vendor_switch   = 1, '供应商切换', NULL),
            IF(nc.is_factory_switch  = 1, '工厂切换',   NULL),
            IF(nc.is_price_change    = 1, '价格变更',   NULL)
        ), x -> x IS NOT NULL)              AS change_reason,
        bs.src_id                           AS change_source_price_confirm_no,
        bs.src_pricea                       AS change_source_pricea,
        bs.src_priceb                       AS change_source_priceb
    FROM new_classified nc
    LATERAL VIEW EXPLODE(
        SEQUENCE(
            CAST(CONCAT(SUBSTR(nc.effect_date, 1, 7), '-01') AS DATE),
            CAST(CONCAT(SUBSTR(nc.expire_date, 1, 7), '-01') AS DATE),
            INTERVAL 1 MONTH
        )
    ) months AS m
    LEFT JOIN best_source bs
        ON nc.gps_price_confirm_no = bs.gps_price_confirm_no
),

-- =============================================================
-- 第六步: 无归因命中时补充"价格续期"
-- =============================================================

new_with_fallback AS (
    SELECT
        ne.gps_price_confirm_no,
        ne.material_code,
        ne.vendor_code,
        ne.factory_code,
        ne.supply_status,
        ne.effect_month,
        ne.effect_date,
        ne.expire_date,
        ne.price_create_date,
        ne.price_void_date,
        ne.gps_create_time,
        ne.price_source,
        ne.material_price_amount,
        ne.logistic_price_amount,
        CASE WHEN SIZE(ne.change_reason) = 0
             THEN ARRAY('价格续期')
             ELSE ne.change_reason
        END                                 AS change_reason,
        ne.change_source_price_confirm_no,
        ne.change_source_pricea,
        ne.change_source_priceb
    FROM new_exploded ne
),

-- =============================================================
-- 第七步: 接替式 void_date 计算
-- =============================================================
-- 同一业务分组(material_code + factory_code + vendor_code + supply_status)下,
-- 按排序键(effect_date ASC, gps_create_time ASC, gps_price_confirm_no ASC),
-- 后一条记录的 price_create_date 即为前一条的 void_date。
-- 同时, 正式价从快照消失(void_ids)也需标记 void_date。

existing_updated AS (
    SELECT
        log.gps_price_confirm_no,
        log.material_code,
        log.vendor_code,
        log.factory_code,
        log.supply_status,
        log.effect_month,
        log.effect_date,
        log.expire_date,
        log.price_create_date,
        COALESCE(
            log.price_void_date,
            CASE WHEN vi.gps_price_confirm_no IS NOT NULL THEN '$[yyyy-MM-dd]' END
        ) AS price_void_date,
        log.gps_create_time,
        log.price_source,
        log.material_price_amount,
        log.logistic_price_amount,
        log.change_reason,
        log.change_source_price_confirm_no,
        log.change_source_pricea,
        log.change_source_priceb
    FROM dwd.disc_nio_dp_price_change_log log
    LEFT JOIN void_ids vi
        ON log.gps_price_confirm_no = vi.gps_price_confirm_no
),

-- 合并已有记录与新增记录, 然后用窗口函数计算接替 void_date
all_records AS (
    SELECT * FROM existing_updated

    UNION ALL

    SELECT
        ne.gps_price_confirm_no,
        ne.material_code,
        ne.vendor_code,
        ne.factory_code,
        ne.supply_status,
        ne.effect_month,
        ne.effect_date,
        ne.expire_date,
        ne.price_create_date,
        ne.price_void_date,
        ne.gps_create_time,
        ne.price_source,
        ne.material_price_amount,
        ne.logistic_price_amount,
        ne.change_reason,
        ne.change_source_price_confirm_no,
        ne.change_source_pricea,
        ne.change_source_priceb
    FROM new_with_fallback ne
    WHERE NOT EXISTS (
        SELECT 1
        FROM dwd.disc_nio_dp_price_change_log el
        WHERE el.gps_price_confirm_no = ne.gps_price_confirm_no
          AND el.effect_month         = ne.effect_month
    )
),

-- 在业务分组+月份维度内, 用 LEAD 窗口获取下一条记录的 price_create_date 作为接替日期
with_succession AS (
    SELECT
        ar.*,
        LEAD(ar.price_create_date) OVER (
            PARTITION BY ar.material_code, ar.factory_code, ar.vendor_code,
                         ar.supply_status, ar.effect_month
            ORDER BY ar.effect_date ASC, ar.gps_create_time ASC, ar.gps_price_confirm_no ASC
        ) AS successor_create_date
    FROM all_records ar
)

-- =============================================================
-- 最终输出: 如果当前记录无 void_date 但有后继记录, 用后继的 price_create_date 作为 void_date
-- =============================================================

SELECT
    gps_price_confirm_no,
    material_code,
    vendor_code,
    factory_code,
    supply_status,
    effect_month,
    effect_date,
    expire_date,
    price_create_date,
    COALESCE(price_void_date, successor_create_date) AS price_void_date,
    gps_create_time,
    price_source,
    material_price_amount,
    logistic_price_amount,
    change_reason,
    change_source_price_confirm_no,
    change_source_pricea,
    change_source_priceb
FROM with_succession
;


-- =====================================================================
-- Hive 兼容性说明 (如运行环境为Hive而非Spark SQL):
-- =====================================================================
--
-- 1. FILTER(ARRAY(...), x -> x IS NOT NULL)
--    Hive替代:
--      SELECT COLLECT_LIST(reason) FROM (
--          SELECT EXPLODE(ARRAY(IF(...), IF(...), ...)) AS reason
--      ) t WHERE reason IS NOT NULL
--    或使用自定义UDF
--
-- 2. SEQUENCE(start_date, end_date, INTERVAL 1 MONTH)
--    Hive替代 (利用POSEXPLODE生成序号, ADD_MONTHS逐月推移):
--      LATERAL VIEW POSEXPLODE(SPLIT(SPACE(119), ' ')) months AS pos, val
--      WHERE ADD_MONTHS(
--          CAST(CONCAT(SUBSTR(effect_date,1,7),'-01') AS DATE), pos
--      ) <= CAST(CONCAT(SUBSTR(expire_date,1,7),'-01') AS DATE)
--    然后: DATE_FORMAT(ADD_MONTHS(..., pos), 'yyyy-MM') AS effect_month
--
-- 3. SIZE(FILTER(array, v -> v < x))
--    Hive替代:
--      EXISTS (SELECT 1 FROM (SELECT EXPLODE(version_set) AS v) t WHERE t.v < n.part_version)
--    或预计算 MIN(part_version) 并比较
--
-- =====================================================================
-- 初始化说明:
-- =====================================================================
-- 首次运行时 dwd.disc_nio_dp_price_change_log 表为空:
--   - Part A (已有记录) 为空
--   - 所有当日快照价格被识别为"新增", 若无前一快照则全部归因为"零件新增"
--   - 建议首次运行前手动初始化: 选择一个基准日期的快照全量插入
--
-- 建议初始化方式:
--   1. 选定基准日 (如 2025-01-01 或最早可用快照日期)
--   2. 将该日快照全量处理为日志 (所有记录归因为"零件新增", change_source为NULL)
--   3. 从基准日+1开始每日增量运行本ETL
-- =====================================================================
