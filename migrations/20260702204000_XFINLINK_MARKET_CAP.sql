-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

-- Create new table for gathering Xfinlink price and market cap data
CREATE TABLE public.xfinlink_market_cap (
    symbol varchar(20) NOT NULL,
    business_date date NOT NULL,
    entity_name varchar(200) NULL,
    gics_sector varchar(200) NULL,
    open float8 NULL,
    high float8 NULL,
    low float8 NULL,
    close float8 NULL,
    adj_close float8 NULL,
    volume float8 NULL,
    return_daily float8 NULL,
    shares_outstanding float8 NULL,
    exchange_code varchar(20) NULL,
    split_ratio float8 NULL,
    dividend float8 NULL,
    market_cap float8 NULL,
    is_staged bool DEFAULT false NOT NULL,
    date_loaded date DEFAULT CURRENT_DATE NOT NULL,
    CONSTRAINT xfinlink_market_cap_pkey PRIMARY KEY (symbol, business_date)
);
CREATE INDEX staged_xfinlink_market_cap ON public.xfinlink_market_cap USING btree (is_staged DESC);
