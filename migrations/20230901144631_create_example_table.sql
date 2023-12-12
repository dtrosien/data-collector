-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
-- Create Example Table
CREATE TABLE example
(
    id            uuid        NOT NULL,
    PRIMARY KEY (id),
    email         TEXT        NOT NULL UNIQUE,
    name          TEXT        NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE NYSE_EVENTS (
	action_date date NOT NULL,
	action_status varchar(100) NOT NULL,
	action_type varchar(100) NOT NULL,
	issue_symbol varchar(100) NOT NULL,
	issuer_name varchar(200) NOT NULL,
	updated_at varchar(100) NOT NULL,
	market_event varchar(36) NOT NULL,
	is_staged boolean NOT NULL DEFAULT false,
	PRIMARY KEY (action_date, issue_symbol, issuer_name, market_event)
);
create index staged_events on NYSE_EVENTS using btree (is_staged desc);

cluster verbose NYSE_EVENTS using staged_events;
analyse verbose NYSE_EVENTS;


CREATE TABLE NYSE_INSTRUMENTS (
	instrument_name VARCHAR(1000) NOT NULL,
	instrument_type VARCHAR(100) NOT NULL,
	symbol_ticker VARCHAR(100) NOT NULL,
	symbol_exchange_ticker VARCHAR(100),
	normalized_ticker VARCHAR(100),
	symbol_esignal_ticker VARCHAR(100),
	mic_code VARCHAR(4) NOT NULL,
	dateLoaded DATE DEFAULT CURRENT_DATE,
	is_staged boolean NOT NULL DEFAULT false,
	PRIMARY KEY (instrument_name, instrument_type, symbol_ticker, mic_code)
);
create index  staged_instruments on NYSE_INSTRUMENTS using btree (is_staged desc);

cluster verbose NYSE_INSTRUMENTS using staged_instruments;
analyse verbose NYSE_INSTRUMENTS;


CREATE TABLE SEC_COMPANIES (
    cik INT NOT NULL,
    sic INT,
    name VARCHAR(200) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    exchange VARCHAR(10),
    state_of_incorporation VARCHAR(2),
    date_loaded DATE DEFAULT CURRENT_DATE NOT NULL,
    is_staged BOOLEAN DEFAULT FALSE NOT NULL,
    PRIMARY KEY (name, ticker)
);
create index staged_sec_companies on SEC_COMPANIES using btree (is_staged desc);

cluster verbose SEC_COMPANIES using staged_sec_companies;
analyse verbose SEC_COMPANIES;


CREATE TABLE micCodes (
    Market_Name VARCHAR(255) NOT NULL,
    Mic_Code CHAR(4) NOT NULL,
    PRIMARY KEY (Mic_Code)
);
INSERT INTO micCodes (Mic_Code, Market_Name) VALUES
('ARCX', 'NYSE ARCA'),
('BATS', 'CBOE BZX U.S. EQUITIES EXCHANGE'),
('IEXG', 'INVESTORS EXCHANGE - DAX FACILITY'),
('XASE', 'NYSE MKT LLC'),
('XCBO', 'CBOE GLOBAL MARKETS INC.'),
('XCME', 'CHICAGO MERCANTILE EXCHANGE'),
('XNAS', 'NASDAQ - ALL MARKETS'),
('XNCM', 'NASDAQ CAPITAL MARKET'),
('XNGS', 'NASDAQ/NGS (GLOBAL SELECT MARKET)'),
('XNMS', 'NASDAQ/NMS (GLOBAL MARKET)'),
('XNYS', 'NEW YORK STOCK EXCHANGE, INC.'),
('XSTX', 'STOXX LIMITED - INDICES'),
('XXXX', 'NO MARKET');

