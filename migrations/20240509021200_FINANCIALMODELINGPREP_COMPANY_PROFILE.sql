-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

CREATE TABLE financialmodelingprep_company_profile (
	symbol varchar(10) NOT NULL,
	price numeric(11, 4) NULL,
	beta numeric(11, 4) NULL,
	vol_avg int8 NULL,
	mkt_cap numeric(18, 4) NULL,
	last_div float8 NULL,
	"range" varchar(100) NULL,
	changes float8 NULL,
	company_name varchar(100) NOT NULL,
	currency varchar(5) NULL,
	cik varchar(10) NULL,
	isin varchar(20) NULL,
	cusip varchar(9) NULL,
	exchange varchar(100) NULL,
	exchange_short_name varchar(100) NULL,
	industry varchar(100) NULL,
	website varchar(100) NULL,
	description varchar(10000) NULL,
	ceo varchar(100) NULL,
	sector varchar(100) NULL,
	country varchar(5) NULL,
	full_time_employees int4 NULL,
	phone varchar(100) NULL,
	address varchar(100) NULL,
	city varchar(100) NULL,
	state varchar(100) NULL,
	zip varchar(10) NULL,
	dcf_diff numeric(20, 10) NULL,
	dcf numeric(20, 10) NULL,
	image varchar(255) NULL,
	ipo_date date NULL,
	default_image varchar(100) NULL,
	is_etf bool NULL,
	is_actively_trading bool NULL,
	is_adr bool NULL,
	is_fund bool NULL,
	is_staged bool DEFAULT false NOT NULL,
	date_loaded date DEFAULT CURRENT_DATE NOT NULL,
	CONSTRAINT financialmodelingprep_company_profile_pkey PRIMARY KEY (symbol, date_loaded)
);

create index staged_financialmodelingprep_company_profile on financialmodelingprep_company_profile using btree (is_staged desc);

cluster verbose financialmodelingprep_company_profile using staged_financialmodelingprep_company_profile;
analyse verbose financialmodelingprep_company_profile;
