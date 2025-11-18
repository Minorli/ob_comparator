-- 修补缺失的 PACKAGE BODY OB_APP.PKG_ORDER_MGMT (源: ORA_SALES.PKG_ORDER_MGMT)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

CREATE OR REPLACE EDITIONABLE PACKAGE BODY "OB_APP"."PKG_ORDER_MGMT" AS
    PROCEDURE QUEUE_ORDER(p_order_id NUMBER, p_status VARCHAR2) IS
    BEGIN
        UPDATE OB_DW.F_ORDER_METRIC
           SET STATUS = p_status,
               UPDATED_AT = SYSDATE
         WHERE ORDER_ID = p_order_id;
    END;

    PROCEDURE CLOSE_ORDER(p_order_id NUMBER) IS
    BEGIN
        UPDATE OB_DW.F_ORDER_METRIC
           SET STATUS = 'C',
               UPDATED_AT = SYSDATE
         WHERE ORDER_ID = p_order_id;
    END;

    FUNCTION COUNT_BY_STATUS(p_status VARCHAR2) RETURN NUMBER IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_cnt
          FROM OB_DW.F_ORDER_METRIC
         WHERE STATUS = p_status;
        RETURN v_cnt;
    END;
END PKG_ORDER_MGMT;