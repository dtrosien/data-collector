-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

CREATE TABLE source_symbol_warden (
    issue_symbol VARCHAR(10) PRIMARY KEY,
    financial_modeling_prep BOOLEAN DEFAULT NULL,
    polygon BOOLEAN DEFAULT NULL,
    SEC BOOLEAN DEFAULT NULL,
    NYSE BOOLEAN DEFAULT NULL
);