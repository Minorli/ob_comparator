-- 基于列差异的 ALTER TABLE 修补脚本: OB_DW.F_ORDER_METRIC (源: ORA_SALES.ORDER_FACT)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 列长度不匹配 (目标端长度小于 ceil(源端长度 * 1.5))，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE OB_DW.F_ORDER_METRIC MODIFY (STATUS VARCHAR(2)); -- 源长度: 1, 目标长度: 1, 期望长度: 2;
