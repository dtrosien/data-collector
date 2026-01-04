-- noinspection SqlNoDataSourceInspectionForFile


CREATE TABLE POLYGON_DIVIDENDS (
    ticker                       VARCHAR(10)    NOT NULL,
    record_date                  DATE,
    pay_date                     DATE,
    declaration_date             DATE,
    ex_dividend_date             DATE           NOT NULL,
    frequency                    INTEGER,
    cash_amount                  NUMERIC(11, 6) NOT NULL,
    currency                     VARCHAR(5)     NOT NULL,
    distribution_type            VARCHAR(20),
    historical_adjustment_factor NUMERIC(11, 6),
    split_adjusted_cash_amount   NUMERIC(11, 6),
    is_staged                    BOOLEAN        NOT NULL DEFAULT FALSE,
    CONSTRAINT POLYGON_DIVIDENDS_pkey PRIMARY KEY (ticker, ex_dividend_date)
);
