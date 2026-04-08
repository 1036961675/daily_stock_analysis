-- ===================================================================
-- 价格变动日志表 DDL
-- ===================================================================
-- 表名: dwd.disc_nio_dp_price_change_log
-- 说明: 每日 T vs T-1 比对, 先按月展开再归因, 按 snapshot_date 分区
-- 主键: snapshot_date + material_code + factory_code + vendor_code
--       + supply_status + effect_month (复合唯一)
-- ===================================================================

CREATE TABLE IF NOT EXISTS dwd.disc_nio_dp_price_change_log (

    -- =================== 物料解析 ===================
    part_prefix                 STRING          COMMENT '零件前8位',
    part_color                  STRING          COMMENT '颜色(5位, 无颜色为空串)',
    part_version                STRING          COMMENT '版本(2位大写字母, AA~ZZ)',

    -- =================== 业务键 ===================
    material_code               STRING          COMMENT '物料号',
    vendor_code                 STRING          COMMENT '供应商编码(源表 gps_vendor_code)',
    factory_code                STRING          COMMENT '工厂编码',
    supply_status               STRING          COMMENT '采购类型 Buy/CS/DB',
    effect_month                STRING          COMMENT '生效月份 yyyy-MM (由生效区间 explode 得到)',

    -- =================== 当日价格状态 (NULL=当日无此价格) ===================
    gps_price_confirm_no        STRING          COMMENT '当日价格流水号',
    effect_date                 STRING          COMMENT '当日生效起始日',
    expire_date                 STRING          COMMENT '当日生效截止日',
    gps_create_time             STRING          COMMENT '当日 GPS 创建时间(源系统 create_time)',
    price_source                STRING          COMMENT '当日价格来源(SOURCING_ADD=非正式价, 其他=正式价)',
    material_price_amount       DECIMAL(20,4)   COMMENT '当日物料价',
    logistic_price_amount       DECIMAL(20,4)   COMMENT '当日物流价',

    -- =================== 前日价格状态 (NULL=前日无此价格) ===================
    prev_gps_price_confirm_no   STRING          COMMENT '前日价格流水号',
    prev_effect_date            STRING          COMMENT '前日生效起始日',
    prev_expire_date            STRING          COMMENT '前日生效截止日',
    prev_gps_create_time        STRING          COMMENT '前日 GPS 创建时间',
    prev_price_source           STRING          COMMENT '前日价格来源',
    prev_material_price_amount  DECIMAL(20,4)   COMMENT '前日物料价',
    prev_logistic_price_amount  DECIMAL(20,4)   COMMENT '前日物流价',

    -- =================== 变动分类 ===================
    change_type                 STRING          COMMENT '变动类型: 新增/消失/变更',
    change_reason               ARRAY<STRING>   COMMENT '一级归因: 零件新增/零件升版/供应商切换/工厂切换/价格变更'
)
COMMENT '价格变动日志(按月展开, 仅 T vs T-1 差异, 按快照日期分区)'
PARTITIONED BY (snapshot_date STRING COMMENT '快照比对日期 yyyy-MM-dd = 当日日期')
;

-- ===================================================================
-- 设计要点:
--
-- 1. 比对逻辑:
--    先将 T 和 T-1 的快照各自按月 EXPLODE, 再在业务键
--    (material_code + factory_code + vendor_code + supply_status + effect_month)
--    上做 FULL OUTER JOIN, 检测差异。
--
-- 2. "同内容换号不算变动" (Change #2):
--    比对键为业务键, 不依赖 gps_price_confirm_no。
--    若 gps_price_confirm_no 变了但价格/生效区间/来源均未变 → 不产出记录。
--
-- 3. change_type 枚举:
--    - 新增: 当日有、前日无 (业务键+月 维度)
--    - 消失: 前日有、当日无
--    - 变更: 两日均有, 但物料价或物流价不同
--
-- 4. change_reason 枚举 (适用于 新增/变更):
--    - 零件新增:   (prefix+color+supply_status+month) 前日不存在
--    - 零件升版:   前日存在但无当前版本, 有更早版本
--    - 供应商切换: (prefix+color+version+supply_status+month) 前日无当前供应商
--    - 工厂切换:   (prefix+color+version+vendor+month) 前日无当前工厂
--    - 价格变更:   同业务键价格不一致
--
-- 5. 每日 ETL 完全独立, 按 snapshot_date 分区 INSERT OVERWRITE,
--    天然幂等, 重跑结果一致, 无需初始化脚本。
--
-- 6. 月份展开: 闭-闭区间 (首尾月均含), SEQUENCE 按自然月生成。
-- ===================================================================
