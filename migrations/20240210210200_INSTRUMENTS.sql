-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
CREATE TABLE instruments (
    Instrument VARCHAR(100) NOT NULL PRIMARY KEY,
    is_sp500_eligible BOOLEAN
);

--Insert instruments data
INSERT INTO instruments (Instrument, is_sp500_eligible)
VALUES 
    ('BOND', NULL),
    ('CLOSED_END_FUND', NULL),
    ('COMMON_STOCK', true),
    ('DEPOSITORY_RECEIPT', NULL),
    ('EQUITY', true),
    ('EXCHANGE_TRADED_FUND', false),
    ('EXCHANGE_TRADED_NOTE', false),
    ('INDEX', false),
    ('LIMITED_PARTNERSHIP', NULL),
    ('NOTE', false),
    ('OTC', false),
    ('PREFERRED_STOCK', false),
    ('REIT', true),
    ('RIGHT', false),
    ('TEST', false),
    ('TRUST', false),
    ('UNITS_OF_BENEFICIAL_INTEREST', NULL),
    ('UNIT', false),
    ('WARRANT', NULL);
