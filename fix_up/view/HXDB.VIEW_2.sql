-- 修补缺失的 VIEW HXDB.VIEW_2 (源: HXDB.VIEW_2)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

CREATE OR REPLACE FORCE EDITIONABLE VIEW "HXDB"."VIEW_2" ("ID", "NAME3") AS 
  SELECT "ID","NAME3"
  FROM hr.table3;
