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
--   5) 合并入结果表 (已有记录 + 新增记录, 并更新消失价格的void_date)
-- =====================================================================

INSERT OVERWRITE TABLE dwd.disc_nio_dp_price_change_log

WITH

-- =============================================================
-- 第一步: 数据准备 - 获取今日快照 & 前一快照
-- =============================================================

-- 前一快照日期: 在最近4天范围内取最新的一天
prev_dt AS (
    SELECT COALESCE(MAX(datetime), '1970-01-01') AS dt
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime >= '$[yyyy-MM-dd-4]'
      AND datetime <= '$[yyyy-MM-dd-1]'
),

-- 当日快照(解析物料号组件)
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
        CAST(material_price_amount  AS DECIMAL(20,4)) AS material_price_amount,
        CAST(logistic_price_amount  AS DECIMAL(20,4)) AS logistic_price_amount,
        -- 物料号拆解: 前缀8位 + 颜色5位(可选) + 版本2位
        SUBSTR(material_code, 1, 8)         AS part_prefix,
        CASE WHEN LENGTH(material_code) > 10
             THEN SUBSTR(material_code, 9, LENGTH(material_code) - 10)
             ELSE '' END                    AS part_color,
        SUBSTR(material_code, -2)           AS part_version
    FROM dwd.disc_nio_dp_price_price_1d_f
    WHERE datetime = '$[yyyy-MM-dd]'
      AND SUBSTR(effect_time, 1, 10) >= '2025-01-01'
),

-- 前一快照(同样解析)
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

-- 新增价格: 今日有, 前一快照没有
new_prices AS (
    SELECT c.*
    FROM curr c
    WHERE NOT EXISTS (
        SELECT 1 FROM prev p
        WHERE p.gps_price_confirm_no = c.gps_price_confirm_no
    )
),

-- 消失价格: 前一快照有, 今日没有
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

-- 维度1: 零件组 (prefix + color + supply_status) → 用于判断 零件新增/零件升版
prev_part_versions AS (
    SELECT
        part_prefix, part_color, supply_status,
        COLLECT_SET(part_version) AS version_set
    FROM prev
    GROUP BY part_prefix, part_color, supply_status
),

-- 维度2: 供应商组 (prefix + color + version + supply_status) → 用于判断 供应商切换
prev_vendor_set AS (
    SELECT
        part_prefix, part_color, part_version, supply_status,
        COLLECT_SET(vendor_code) AS vendor_set
    FROM prev
    GROUP BY part_prefix, part_color, part_version, supply_status
),

-- 维度3: 工厂组 (prefix + color + version + vendor) → 用于判断 工厂切换
prev_factory_set AS (
    SELECT
        part_prefix, part_color, part_version, vendor_code,
        COLLECT_SET(factory_code) AS factory_set
    FROM prev
    GROUP BY part_prefix, part_color, part_version, vendor_code
),

-- 维度4: 价格组 (prefix + color + factory + vendor + supply_status) 最新一条 → 用于判断 价格变更
prev_price_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY part_prefix, part_color, factory_code, vendor_code, supply_status
            ORDER BY gps_create_time DESC, expire_date DESC
        ) AS rn
    FROM prev
),

-- 新增价格归因分类
new_classified AS (
    SELECT
        n.*,

        -- ① 零件新增: (prefix, color, supply_status) 在前一快照不存在
        CASE WHEN pv.part_prefix IS NULL
             THEN 1 ELSE 0
        END AS is_part_new,

        -- ② 零件升版: 当前版本不在前一快照版本集合中, 但存在更早版本
        CASE WHEN pv.part_prefix IS NOT NULL
                  AND NOT ARRAY_CONTAINS(pv.version_set, n.part_version)
                  AND SIZE(FILTER(pv.version_set, v -> v < n.part_version)) > 0
             THEN 1 ELSE 0
        END AS is_version_upgrade,

        -- ③ 供应商切换: (prefix, color, version, supply_status) 有供应商但无当前供应商
        CASE WHEN vs.part_prefix IS NOT NULL
                  AND NOT ARRAY_CONTAINS(vs.vendor_set, n.vendor_code)
             THEN 1 ELSE 0
        END AS is_vendor_switch,

        -- ④ 工厂切换: (prefix, color, version, vendor) 有工厂但无当前工厂
        CASE WHEN fs.part_prefix IS NOT NULL
                  AND NOT ARRAY_CONTAINS(fs.factory_set, n.factory_code)
             THEN 1 ELSE 0
        END AS is_factory_switch,

        -- ⑤ 价格变更: (prefix, color, factory, vendor, supply_status) 最新价格与当前不同
        CASE WHEN pr.part_prefix IS NOT NULL
                  AND (COALESCE(pr.material_price_amount, 0) != COALESCE(n.material_price_amount, 0)
                    OR COALESCE(pr.logistic_price_amount, 0) != COALESCE(n.logistic_price_amount, 0))
             THEN 1 ELSE 0
        END AS is_price_change

    FROM new_prices n

    -- 零件新增 / 零件升版
    LEFT JOIN prev_part_versions pv
        ON  n.part_prefix   = pv.part_prefix
        AND n.part_color    = pv.part_color
        AND n.supply_status = pv.supply_status

    -- 供应商切换
    LEFT JOIN prev_vendor_set vs
        ON  n.part_prefix   = vs.part_prefix
        AND n.part_color    = vs.part_color
        AND n.part_version  = vs.part_version
        AND n.supply_status = vs.supply_status

    -- 工厂切换
    LEFT JOIN prev_factory_set fs
        ON  n.part_prefix   = fs.part_prefix
        AND n.part_color    = fs.part_color
        AND n.part_version  = fs.part_version
        AND n.vendor_code   = fs.vendor_code

    -- 价格变更 (取前一快照同组最新价格)
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

-- 候选来源: 按优先级在前一快照中匹配
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
                p.gps_create_time DESC,
                p.expire_date     DESC
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

new_exploded AS (
    SELECT
        nc.gps_price_confirm_no,
        nc.material_code,
        nc.vendor_code,
        nc.factory_code,
        nc.supply_status,
        DATE_FORMAT(m, 'yyyy-MM')           AS effect_month,
        '$[yyyy-MM-dd]'                     AS price_create_date,
        CAST(NULL AS STRING)                AS price_void_date,
        nc.gps_create_time,
        -- 构建归因数组(过滤掉NULL)
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
)

-- =============================================================
-- 第六步: 最终合并 → INSERT OVERWRITE
--   Part A: 已有日志 (更新消失价格的void_date)
--   Part B: 新增展开记录 (去重保障幂等)
-- =============================================================

SELECT
    gps_price_confirm_no,
    material_code,
    vendor_code,
    factory_code,
    supply_status,
    effect_month,
    price_create_date,
    price_void_date,
    gps_create_time,
    change_reason,
    change_source_price_confirm_no,
    change_source_pricea,
    change_source_priceb
FROM (
    -- Part A: 已有日志记录 (消失价格写入void_date, 已有void_date保持不变)
    SELECT
        log.gps_price_confirm_no,
        log.material_code,
        log.vendor_code,
        log.factory_code,
        log.supply_status,
        log.effect_month,
        log.price_create_date,
        COALESCE(
            log.price_void_date,
            CASE WHEN vi.gps_price_confirm_no IS NOT NULL THEN '$[yyyy-MM-dd]' END
        ) AS price_void_date,
        log.gps_create_time,
        log.change_reason,
        log.change_source_price_confirm_no,
        log.change_source_pricea,
        log.change_source_priceb
    FROM dwd.disc_nio_dp_price_change_log log
    LEFT JOIN void_ids vi
        ON log.gps_price_confirm_no = vi.gps_price_confirm_no

    UNION ALL

    -- Part B: 新增展开记录 (排除已存在于日志中的记录, 保障重跑幂等)
    SELECT
        ne.gps_price_confirm_no,
        ne.material_code,
        ne.vendor_code,
        ne.factory_code,
        ne.supply_status,
        ne.effect_month,
        ne.price_create_date,
        ne.price_void_date,
        ne.gps_create_time,
        ne.change_reason,
        ne.change_source_price_confirm_no,
        ne.change_source_pricea,
        ne.change_source_priceb
    FROM new_exploded ne
    WHERE NOT EXISTS (
        SELECT 1
        FROM dwd.disc_nio_dp_price_change_log el
        WHERE el.gps_price_confirm_no = ne.gps_price_confirm_no
          AND el.effect_month         = ne.effect_month
    )
) combined
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
