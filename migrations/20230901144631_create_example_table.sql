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
    is_staged boolean DEFAULT false,
    PRIMARY KEY (action_date, issue_symbol, issuer_name, market_event, is_staged)
)
PARTITION BY LIST (is_staged);
CREATE TABLE NYSE_EVENTS_STAGED PARTITION OF NYSE_EVENTS
  FOR VALUES in (true);
CREATE TABLE NYSE_EVENTS_NOT_STAGED PARTITION OF NYSE_EVENTS
  FOR VALUES in (false);


CREATE TABLE NYSE_INSTRUMENTS (
	instrument_name VARCHAR(1000) NOT NULL,
	instrument_type VARCHAR(100) NOT NULL,
	symbol_ticker VARCHAR(100) NOT NULL,
	symbol_exchange_ticker VARCHAR(100),
	normalized_ticker VARCHAR(100),
	symbol_esignal_ticker VARCHAR(100),
	mic_code VARCHAR(4) NOT NULL,
	dateLoaded DATE DEFAULT CURRENT_DATE,
	is_staged BOOLEAN DEFAULT false,
	PRIMARY KEY (instrument_name, instrument_type, symbol_ticker, mic_code, is_staged)
)
PARTITION BY LIST (is_staged);
CREATE TABLE NYSE_INSTRUMENTS_STAGED PARTITION OF NYSE_INSTRUMENTS
  FOR VALUES in (true);
CREATE TABLE NYSE_INSTRUMENTS_NOT_STAGED PARTITION OF NYSE_INSTRUMENTS
  FOR VALUES in (false);


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

