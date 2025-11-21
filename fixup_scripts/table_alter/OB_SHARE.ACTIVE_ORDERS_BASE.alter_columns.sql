-- 基于列差异的 ALTER TABLE 修补脚本: OB_SHARE.ACTIVE_ORDERS_BASE (源: ORA_TXN.ACTIVE_ORDERS_BASE)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

ALTER SESSION SET CURRENT_SCHEMA = OB_SHARE;

-- 列长度不匹配 (目标端长度需在 [ceil(src*1.5), ceil(src*2.5)] 区间)：
ALTER TABLE OB_SHARE.ACTIVE_ORDERS_BASE MODIFY (ORDER_CODE VARCHAR(45)); -- 源长度: 30, 目标长度: 30, 期望下限: 45
ALTER TABLE OB_SHARE.ACTIVE_ORDERS_BASE MODIFY (STATUS VARCHAR(2)); -- 源长度: 1, 目标长度: 1, 期望下限: 2
;
