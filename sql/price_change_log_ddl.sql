-- ===================================================================
-- 价格变动日志表 DDL
-- ===================================================================
-- 表名: dwd.disc_nio_dp_price_change_log
-- 说明: 按月展开的价格变动日志，记录每条价格记录的变更归因及变更来源
-- 主键: gps_price_confirm_no + effect_month (复合主键)
-- ===================================================================

CREATE TABLE IF NOT EXISTS dwd.disc_nio_dp_price_change_log (
    gps_price_confirm_no        STRING          COMMENT '价格流水号',
    material_code               STRING          COMMENT '物料号(前8位零件号 + 颜色5位(可选) + 版本2位字母)',
    vendor_code                 STRING          COMMENT '合作伙伴编码(供应商, 对应源表 gps_vendor_code)',
    factory_code                STRING          COMMENT '工厂编码',
    supply_status               STRING          COMMENT '采购类型 Buy/CS/DB',
    effect_month                STRING          COMMENT '价格生效月份 yyyy-MM (由生效期间按月展开)',
    effect_date                 STRING          COMMENT '价格生效起始日期(源表 effect_time 截取至日)',
    expire_date                 STRING          COMMENT '价格生效截止日期(源表 expire_time 截取至日)',
    price_create_date           STRING          COMMENT '价格产生日期(该价格首次出现在快照中的日期)',
    price_void_date             STRING          COMMENT '价格消失日期(被同组后续价格接替的日期, NULL表示当前有效)',
    gps_create_time             STRING          COMMENT 'GPS价格创建时间(源系统 create_time, 与 price_create_date 区别: 前者为源系统录入时间, 后者为快照首次出现日期)',
    price_source                STRING          COMMENT '价格来源(SOURCING_ADD=非正式价/谈判价, 其他=正式价/合同价)',
    material_price_amount       DECIMAL(20,4)   COMMENT '物料价格',
    logistic_price_amount       DECIMAL(20,4)   COMMENT '物流价格',
    change_reason               ARRAY<STRING>   COMMENT '一级归因类型: 零件新增/零件升版/供应商切换/工厂切换/价格变更/价格续期',
    change_source_price_confirm_no  STRING      COMMENT '变更来源价格流水号(被替代/对比的前一版价格)',
    change_source_pricea        STRING          COMMENT '变更来源物料价格',
    change_source_priceb        STRING          COMMENT '变更来源物流价格'
)
COMMENT '价格变动日志表(按月展开) - 记录零件价格全生命周期变动'
;

-- ===================================================================
-- 字段说明:
--
-- 1. 变更归因(change_reason)枚举:
--    - 零件新增: 前八位+颜色+采购类型 在前一快照中完全不存在
--    - 零件升版: 前八位+颜色+采购类型 存在, 但无当前版本, 有更早版本
--    - 供应商切换: 前八位+颜色+版本+采购类型 存在, 但无当前供应商
--    - 工厂切换: 前八位+颜色+版本+供应商 存在, 但无当前工厂(有其他工厂)
--    - 价格变更: 前八位+颜色+工厂+供应商+采购类型 存在, 但物料价或物流价不同
--    - 价格续期: 以上均不满足(如新价格周期但金额不变)
--
-- 2. 版本规则:
--    - 2位大写字母: AA→AB→...→AZ→BA→BB→...→ZZ
--    - 字符串比较即可正确排序(与 ASCII/字典序一致)
--
-- 3. 物料号结构:
--    - 无颜色: 10位 = 前缀8位 + 版本2位
--    - 有颜色: 15位 = 前缀8位 + 颜色5位 + 版本2位
--
-- 4. price_void_date(接替规则):
--    同一业务分组(material_code + factory_code + vendor_code + supply_status)下,
--    当后续价格记录出现时, 本条价格被接替, void_date = 接替发生日期。
--    接替排序键: effect_date ASC, gps_create_time ASC, gps_price_confirm_no ASC
--    另: 正式价从快照中消失时, 亦视为消失(void_date = 消失日期)。
--
-- 5. SOURCING_ADD(非正式价)去重规则:
--    按 业务件(material_code+factory_code+vendor_code+supply_status) +
--    价格生效区间(effect_date+expire_date) 唯一;
--    全历史中首次出现时写入日志, 后续重复出现则忽略,
--    不因每日快照重复而重复记录。
--
-- 6. BOM成本对比使用方式:
--    - 匹配键: SUBSTR(material_code,1,8) + 颜色(5位) + factory_code
--              + vendor_code + supply_status
--    - 筛选条件: effect_month = 目标月份
--              AND price_create_date BETWEEN 上月25日 AND 本月25日
--    - 通过 change_source_pricea/priceb 与 material_price_amount/logistic_price_amount
--      对比即可计算价格变动金额
--
-- 7. effect_month explode 边界:
--    生效区间按自然月展开, 首尾月均包含(闭-闭), 跨年自动处理。
--    使用 SEQUENCE(effect_month_start, expire_month_start, INTERVAL 1 MONTH) 生成。
-- ===================================================================
