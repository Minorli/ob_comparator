-- 基于列差异的 ALTER TABLE 修补脚本: OB_DATA.EMPLOYEES (源: ORA_DATA.EMPLOYEES)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 源端存在而目标端缺失的列，将通过 ALTER TABLE ADD 补齐：
ALTER TABLE OB_DATA.EMPLOYEES ADD (HIRE_DATE DATE);

-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE OB_DATA.EMPLOYEES MODIFY (NAME VARCHAR(150)); -- 源长度: 100, 目标长度: 100, 期望长度: 150

-- 目标端存在而源端不存在的列，以下 DROP COLUMN 为建议操作，请谨慎执行：
-- ALTER TABLE OB_DATA.EMPLOYEES DROP COLUMN EMAIL;