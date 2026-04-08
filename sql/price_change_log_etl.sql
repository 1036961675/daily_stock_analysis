-- =====================================================================
-- 价格变动日志 ETL (每日 T vs T-1)
-- =====================================================================
-- 调度频率: 每日
-- 调度参数:
--   $[yyyy-MM-dd]   → 当日日期
--   $[yyyy-MM-dd-1] → 昨日日期
--   $[yyyy-MM-dd-4] → 4天前日期
-- 输入: dwd.disc_nio_dp_price_price_1d_f  (每日价格快照)
-- 输出: dwd.disc_nio_dp_price_change_log   (价格变动日志, 按 snapshot_date 分区)
-- 引擎: Spark SQL
--
-- 核心思路:
--   1) T 和 T-1 快照各自先按月 EXPLODE
--   2) 在每月粒度的业务键上做 FULL OUTER JOIN
--   3) 过滤:
--      a) 仅 gps_price_confirm_no 变了但价格/区间/来源一样 → 跳过(不算变动)
--      b) 真正有差异(新增/消失/价格变更) → 产出记录
--   4) 对产出记录做归因分类
--
-- 幂等性: INSERT OVERWRITE 到当日分区, 重跑安全
-- =====================================================================

INSERT OVERWRITE TABLE dwd.disc_nio_dp_price_change_log
PARTITION (snapshot_date = '$[yyyy-MM-dd]')

WITH

-- =============================================================
-- 第一步: 确定前一快照日期
-- =============================================================

prev_dt AS (
    SELECT COALESCE(MAX(datetime), '1970-01-01') AS dt
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime >= '$[yyyy-MM-dd-4]'
      AND datetime <  '$[yyyy-MM-dd]'
),

-- =============================================================
-- 第二步: 当日快照 → 解析物料号 → 按月 EXPLODE
-- =============================================================
-- 业务键粒度: material_code + factory_code + vendor_code + supply_status + effect_month
-- 同一业务键在同一快照中可能有多条(不同 gps_price_confirm_no), 取最新一条

curr_raw AS (
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

curr_exploded AS (
    SELECT
        c.*,
        DATE_FORMAT(m, 'yyyy-MM')           AS effect_month
    FROM curr_raw c
    LATERAL VIEW EXPLODE(
        SEQUENCE(
            CAST(CONCAT(SUBSTR(c.effect_date, 1, 7), '-01') AS DATE),
            CAST(CONCAT(SUBSTR(c.expire_date, 1, 7), '-01') AS DATE),
            INTERVAL 1 MONTH
        )
    ) months AS m
),

-- 同一业务键+月份下保留最新一条 (按 effect_date DESC, gps_create_time DESC, gps_price_confirm_no DESC)
curr_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY material_code, factory_code, vendor_code, supply_status, effect_month
            ORDER BY effect_date DESC, gps_create_time DESC, gps_price_confirm_no DESC
        ) AS rn
    FROM curr_exploded
),

curr AS (
    SELECT * FROM curr_ranked WHERE rn = 1
),

-- =============================================================
-- 第三步: 前日快照 → 同样解析 → 按月 EXPLODE → 去重
-- =============================================================

prev_raw AS (
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

prev_exploded AS (
    SELECT
        p.*,
        DATE_FORMAT(m, 'yyyy-MM')           AS effect_month
    FROM prev_raw p
    LATERAL VIEW EXPLODE(
        SEQUENCE(
            CAST(CONCAT(SUBSTR(p.effect_date, 1, 7), '-01') AS DATE),
            CAST(CONCAT(SUBSTR(p.expire_date, 1, 7), '-01') AS DATE),
            INTERVAL 1 MONTH
        )
    ) months AS m
),

prev_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY material_code, factory_code, vendor_code, supply_status, effect_month
            ORDER BY effect_date DESC, gps_create_time DESC, gps_price_confirm_no DESC
        ) AS rn
    FROM prev_exploded
),

prev AS (
    SELECT * FROM prev_ranked WHERE rn = 1
),

-- =============================================================
-- 第四步: FULL OUTER JOIN → 比对差异
-- =============================================================
-- 业务键: material_code + factory_code + vendor_code + supply_status + effect_month
-- 过滤逻辑:
--   - 仅 gps_price_confirm_no 变但价格/区间/来源均相同 → 跳过
--   - 真正差异 → 归为 新增 / 消失 / 变更

joined AS (
    SELECT
        COALESCE(c.part_prefix,   p.part_prefix)   AS part_prefix,
        COALESCE(c.part_color,    p.part_color)    AS part_color,
        COALESCE(c.part_version,  p.part_version)  AS part_version,
        COALESCE(c.material_code, p.material_code) AS material_code,
        COALESCE(c.vendor_code,   p.vendor_code)   AS vendor_code,
        COALESCE(c.factory_code,  p.factory_code)  AS factory_code,
        COALESCE(c.supply_status, p.supply_status) AS supply_status,
        COALESCE(c.effect_month,  p.effect_month)  AS effect_month,

        -- 当日价格
        c.gps_price_confirm_no,
        c.effect_date,
        c.expire_date,
        c.gps_create_time,
        c.price_source,
        c.material_price_amount,
        c.logistic_price_amount,

        -- 前日价格
        p.gps_price_confirm_no              AS prev_gps_price_confirm_no,
        p.effect_date                       AS prev_effect_date,
        p.expire_date                       AS prev_expire_date,
        p.gps_create_time                   AS prev_gps_create_time,
        p.price_source                      AS prev_price_source,
        p.material_price_amount             AS prev_material_price_amount,
        p.logistic_price_amount             AS prev_logistic_price_amount,

        CASE
            WHEN p.material_code IS NULL THEN '新增'
            WHEN c.material_code IS NULL THEN '消失'
            ELSE '变更'
        END AS change_type

    FROM curr c
    FULL OUTER JOIN prev p
        ON  c.material_code = p.material_code
        AND c.factory_code  = p.factory_code
        AND c.vendor_code   = p.vendor_code
        AND c.supply_status = p.supply_status
        AND c.effect_month  = p.effect_month
),

-- 过滤: 同内容换号不算变动 (gps 变了但实质内容一样 → 排除)
real_changes AS (
    SELECT *
    FROM joined
    WHERE NOT (
        change_type = '变更'
        AND COALESCE(material_price_amount, 0) = COALESCE(prev_material_price_amount, 0)
        AND COALESCE(logistic_price_amount, 0) = COALESCE(prev_logistic_price_amount, 0)
        AND COALESCE(effect_date, '')          = COALESCE(prev_effect_date, '')
        AND COALESCE(expire_date, '')          = COALESCE(prev_expire_date, '')
        AND COALESCE(price_source, '')         = COALESCE(prev_price_source, '')
    )
),

-- =============================================================
-- 第五步: 归因分类 (在展开后的月粒度上)
-- =============================================================
-- 维度聚合基于 prev 展开后的月粒度数据

-- 维度1: prefix+color+supply_status+month 存在性 → 零件新增 / 升版
prev_part_group AS (
    SELECT
        part_prefix, part_color, supply_status, effect_month,
        COLLECT_SET(part_version) AS version_set
    FROM prev
    GROUP BY part_prefix, part_color, supply_status, effect_month
),

-- 维度2: prefix+color+version+supply_status+month 供应商集合 → 供应商切换
prev_vendor_group AS (
    SELECT
        part_prefix, part_color, part_version, supply_status, effect_month,
        COLLECT_SET(vendor_code) AS vendor_set
    FROM prev
    GROUP BY part_prefix, part_color, part_version, supply_status, effect_month
),

-- 维度3: prefix+color+version+vendor+month 工厂集合 → 工厂切换
prev_factory_group AS (
    SELECT
        part_prefix, part_color, part_version, vendor_code, effect_month,
        COLLECT_SET(factory_code) AS factory_set
    FROM prev
    GROUP BY part_prefix, part_color, part_version, vendor_code, effect_month
),

classified AS (
    SELECT
        rc.*,

        -- ① 零件新增: (prefix+color+supply_status+month) 前日不存在
        CASE WHEN rc.change_type IN ('新增', '变更')
              AND pg.part_prefix IS NULL
             THEN 1 ELSE 0
        END AS is_part_new,

        -- ② 零件升版: 前日有此零件组但无当前版本, 且有更早版本
        CASE WHEN rc.change_type IN ('新增', '变更')
              AND pg.part_prefix IS NOT NULL
              AND NOT ARRAY_CONTAINS(pg.version_set, rc.part_version)
              AND SIZE(FILTER(pg.version_set, v -> v < rc.part_version)) > 0
             THEN 1 ELSE 0
        END AS is_version_upgrade,

        -- ③ 供应商切换: (prefix+color+version+supply_status+month) 有记录但无当前供应商
        CASE WHEN rc.change_type IN ('新增', '变更')
              AND vg.part_prefix IS NOT NULL
              AND NOT ARRAY_CONTAINS(vg.vendor_set, rc.vendor_code)
             THEN 1 ELSE 0
        END AS is_vendor_switch,

        -- ④ 工厂切换: (prefix+color+version+vendor+month) 有记录但无当前工厂
        CASE WHEN rc.change_type IN ('新增', '变更')
              AND fg.part_prefix IS NOT NULL
              AND NOT ARRAY_CONTAINS(fg.factory_set, rc.factory_code)
             THEN 1 ELSE 0
        END AS is_factory_switch,

        -- ⑤ 价格变更: 同业务键价格不一致 (仅 change_type='变更' 时有意义)
        CASE WHEN rc.change_type = '变更'
              AND (COALESCE(rc.material_price_amount, 0) != COALESCE(rc.prev_material_price_amount, 0)
                OR COALESCE(rc.logistic_price_amount, 0) != COALESCE(rc.prev_logistic_price_amount, 0))
             THEN 1 ELSE 0
        END AS is_price_change

    FROM real_changes rc

    LEFT JOIN prev_part_group pg
        ON  rc.part_prefix   = pg.part_prefix
        AND rc.part_color    = pg.part_color
        AND rc.supply_status = pg.supply_status
        AND rc.effect_month  = pg.effect_month

    LEFT JOIN prev_vendor_group vg
        ON  rc.part_prefix   = vg.part_prefix
        AND rc.part_color    = vg.part_color
        AND rc.part_version  = vg.part_version
        AND rc.supply_status = vg.supply_status
        AND rc.effect_month  = vg.effect_month

    LEFT JOIN prev_factory_group fg
        ON  rc.part_prefix   = fg.part_prefix
        AND rc.part_color    = fg.part_color
        AND rc.part_version  = fg.part_version
        AND rc.vendor_code   = fg.vendor_code
        AND rc.effect_month  = fg.effect_month
)

-- =============================================================
-- 最终输出
-- =============================================================

SELECT
    part_prefix,
    part_color,
    part_version,
    material_code,
    vendor_code,
    factory_code,
    supply_status,
    effect_month,

    gps_price_confirm_no,
    effect_date,
    expire_date,
    gps_create_time,
    price_source,
    material_price_amount,
    logistic_price_amount,

    prev_gps_price_confirm_no,
    prev_effect_date,
    prev_expire_date,
    prev_gps_create_time,
    prev_price_source,
    prev_material_price_amount,
    prev_logistic_price_amount,

    change_type,
    CASE
        WHEN change_type = '消失' THEN ARRAY('消失')
        ELSE FILTER(ARRAY(
            IF(is_part_new        = 1, '零件新增',   NULL),
            IF(is_version_upgrade = 1, '零件升版',   NULL),
            IF(is_vendor_switch   = 1, '供应商切换', NULL),
            IF(is_factory_switch  = 1, '工厂切换',   NULL),
            IF(is_price_change    = 1, '价格变更',   NULL)
        ), x -> x IS NOT NULL)
    END AS change_reason

FROM classified
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
--
-- 2. SEQUENCE(start_date, end_date, INTERVAL 1 MONTH)
--    Hive替代:
--      LATERAL VIEW POSEXPLODE(SPLIT(SPACE(119), ' ')) months AS pos, val
--      WHERE ADD_MONTHS(
--          CAST(CONCAT(SUBSTR(effect_date,1,7),'-01') AS DATE), pos
--      ) <= CAST(CONCAT(SUBSTR(expire_date,1,7),'-01') AS DATE)
--
-- 3. SIZE(FILTER(array, v -> v < x))
--    Hive替代:
--      EXISTS (SELECT 1 FROM (SELECT EXPLODE(version_set) AS v) t WHERE t.v < part_version)
-- =====================================================================
