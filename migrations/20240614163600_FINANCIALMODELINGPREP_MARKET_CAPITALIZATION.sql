-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

-- Create new table for gathering market capitalization data
CREATE TABLE public.financialmodelingprep_market_cap (
	symbol varchar(10) NOT NULL,
	business_date date NOT NULL,
	market_cap numeric(23, 8) NOT NULL,
	is_staged bool DEFAULT false NOT NULL,
	date_loaded date DEFAULT CURRENT_DATE NOT NULL,
	CONSTRAINT financialmodelingprep_market_cap_pkey PRIMARY KEY (symbol, business_date)
);
CREATE INDEX staged_financialmodelingprep_market_cap ON public.financialmodelingprep_market_cap USING btree (is_staged DESC);
