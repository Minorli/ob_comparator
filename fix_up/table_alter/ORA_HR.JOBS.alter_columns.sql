-- 基于列差异的 ALTER TABLE 修补脚本: ORA_HR.JOBS (源: ORA_HR.JOBS)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

-- 源端存在而目标端缺失的列，将通过 ALTER TABLE ADD 补齐：
ALTER TABLE ORA_HR.JOBS ADD (MIN_SALARY NUMBER);

-- 列长度不匹配 (目标端长度不等于源端 * 1.5)，将通过 ALTER TABLE MODIFY 修正：
ALTER TABLE ORA_HR.JOBS MODIFY (JOB_TITLE VARCHAR(150)); -- 源长度: 100, 目标长度: 100, 期望长度: 150;
