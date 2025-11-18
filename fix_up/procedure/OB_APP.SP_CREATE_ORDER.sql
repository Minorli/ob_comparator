-- 修补缺失的 PROCEDURE OB_APP.SP_CREATE_ORDER (源: ORA_SALES.SP_CREATE_ORDER)
-- 本文件由校验工具自动生成，请在 OceanBase 执行前仔细审核。

CREATE OR REPLACE EDITIONABLE PROCEDURE "OB_APP"."SP_CREATE_ORDER" (
    p_customer_code IN VARCHAR2,
    p_ship_method   IN VARCHAR2,
    p_amount        IN NUMBER,
    p_discount      IN NUMBER
) AS
    v_cust_id NUMBER;
    v_ship_id NUMBER;
BEGIN
    SELECT CUSTOMER_ID INTO v_cust_id FROM OB_ODS.CUST_DIM
     WHERE CUSTOMER_CODE = p_customer_code;

    SELECT SHIP_METHOD_ID INTO v_ship_id FROM OB_STAGE.SHIP_METHOD
     WHERE SHIP_METHOD_CODE = p_ship_method;

    INSERT INTO OB_DW.F_ORDER_METRIC (
        ORDER_ID, ORDER_CODE, CUSTOMER_ID, SHIP_METHOD_ID,
        ORDER_TOTAL, DISCOUNT_RATE, STATUS, ORDER_NOTE
    )
    VALUES (
        OB_DW.SEQ_F_ORDER.NEXTVAL,
        'ORD-' || TO_CHAR(OB_DW.SEQ_F_ORDER.CURRVAL),
        v_cust_id,
        v_ship_id,
        p_amount,
        p_discount,
        'N',
        'Created via SP_CREATE_ORDER'
    );
END;