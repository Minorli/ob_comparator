-- Labyrinth Case - OceanBase 目标端初始化脚本（故意不完整）
-- 作用：清理并重建 OB_* 目标 schema，保留缺口和噪声以验证对比结果。

-- 1) 清理旧用户
BEGIN
  FOR u IN (
    SELECT username FROM all_users
     WHERE username IN ('OB_BASE','OB_FIN','OB_SALES','OB_APP','OB_ANALYTICS','OB_UTIL')
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'DROP USER '||u.username||' CASCADE';
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/

-- 2) 创建用户
CREATE USER OB_BASE IDENTIFIED BY ob_base;
CREATE USER OB_FIN  IDENTIFIED BY ob_fin;
CREATE USER OB_SALES IDENTIFIED BY ob_sales;
CREATE USER OB_APP  IDENTIFIED BY ob_app;
CREATE USER OB_ANALYTICS IDENTIFIED BY ob_analytics;
CREATE USER OB_UTIL IDENTIFIED BY ob_util;
GRANT DBA, CONNECT, RESOURCE TO OB_BASE, OB_FIN, OB_SALES, OB_APP, OB_ANALYTICS, OB_UTIL;

--------------------------------------------------------------------------------
-- OB_BASE：刻意漏掉序列/触发器/NOTE列，缩短 NAME 长度
--------------------------------------------------------------------------------
ALTER SESSION SET CURRENT_SCHEMA = OB_BASE;

CREATE TABLE CORE_ACCOUNT (
  ACCOUNT_ID   NUMBER PRIMARY KEY,
  NAME         VARCHAR2(20) NOT NULL, -- 长度不足
  STATUS       VARCHAR2(20) NOT NULL,
  REGION_CODE  VARCHAR2(8),
  CREATED_AT   DATE DEFAULT SYSDATE
);

-- 无 SEQ_ACCOUNT / TRG_ACCOUNT_BI / NOTE 列 / 类型 T_AUDIT_TAG / 包

GRANT SELECT ON CORE_ACCOUNT TO OB_ANALYTICS;

--------------------------------------------------------------------------------
-- OB_FIN：缺少序列/触发器，去掉外键；加入一个多余序列作为噪声
--------------------------------------------------------------------------------
ALTER SESSION SET CURRENT_SCHEMA = OB_FIN;

CREATE TABLE FIN_LEDGER (
  LEDGER_ID   NUMBER PRIMARY KEY,
  ACCOUNT_ID  NUMBER NOT NULL,
  AMOUNT      NUMBER(12,2) NOT NULL,
  CURRENCY    VARCHAR2(3),
  REMARK      VARCHAR2(50)
);

CREATE SEQUENCE EXTRA_SEQ_FIN_NOISE START WITH 1; -- 目标端多余对象

-- 缺少 SEQ_LEDGER / TRG_LEDGER_BI / FN_GET_BALANCE / PKG_FIN_RECON

--------------------------------------------------------------------------------
-- OB_SALES：只建主表，缺少子表/序列/触发器，并添加额外表
--------------------------------------------------------------------------------
ALTER SESSION SET CURRENT_SCHEMA = OB_SALES;

CREATE TABLE APP_ORDER (
  ORDER_ID    NUMBER PRIMARY KEY,
  ACCOUNT_ID  NUMBER NOT NULL,
  ORDER_CODE  VARCHAR2(24), -- 长度不足
  CHANNEL     VARCHAR2(10), -- 长度不足
  CREATED_AT  DATE
);

CREATE TABLE EXTRA_SHADOW_ORDER (
  ID NUMBER PRIMARY KEY,
  NOTE VARCHAR2(40)
);

-- 缺少 APP_ORDER_LINE / SEQ_ORDER / SEQ_ORDER_LINE / TRG_ORDER_BI / 相关过程

--------------------------------------------------------------------------------
-- OB_APP：仅创建包规范，缺少包体与多数过程，加一个无关的函数
--------------------------------------------------------------------------------
ALTER SESSION SET CURRENT_SCHEMA = OB_APP;

CREATE OR REPLACE PACKAGE PKG_APP_API AS
  FUNCTION ORDER_WITH_BALANCE(P_ORDER_ID NUMBER) RETURN NUMBER;
END PKG_APP_API;
/

CREATE OR REPLACE FUNCTION FN_NOISE RETURN NUMBER IS BEGIN RETURN 42; END;
/

-- 缺少 PKG_APP_API BODY / PR_CREATE_ORDER / FN_ORDER_TOTAL / PKG_CORE_UTIL / PKG_FIN_RECON

--------------------------------------------------------------------------------
-- OB_ANALYTICS：仅创建视图占位，缺少 MV 与其他视图
--------------------------------------------------------------------------------
ALTER SESSION SET CURRENT_SCHEMA = OB_ANALYTICS;

CREATE OR REPLACE VIEW V_ACCOUNT_ACTIVE AS
SELECT ACCOUNT_ID, NAME, STATUS FROM OB_BASE.CORE_ACCOUNT WHERE STATUS = 'ACTIVE';

-- 缺少 MV_STATUS_COUNT / V_ORDER_SUM

--------------------------------------------------------------------------------
-- OB_UTIL：记录表存在但缺触发器；同义词指向错误对象
--------------------------------------------------------------------------------
ALTER SESSION SET CURRENT_SCHEMA = OB_UTIL;

CREATE TABLE UTIL_LOG (
  LOG_ID     NUMBER PRIMARY KEY,
  TAG        VARCHAR2(20),
  DETAILS    VARCHAR2(100),
  CREATED_AT DATE
);

-- 缺少 SEQ_LOG / TRG_LOG_BI / PR_LOG_ACTIVITY / FN_LOG_AND_BALANCE

CREATE OR REPLACE SYNONYM SYN_FN_BALANCE FOR OB_FIN.FN_GET_BALANCE; -- 指向不存在的函数
CREATE OR REPLACE SYNONYM SYN_ORDER_VIEW FOR OB_SALES.APP_ORDER;    -- 指向表而非视图

COMMIT;
