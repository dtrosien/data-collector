-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
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