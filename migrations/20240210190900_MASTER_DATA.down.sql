-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE MASTER_DATA ADD column is_company boolean;
ALTER TABLE MASTER_DATA DROP COLUMN Instrument;
update nyse_instruments set is_staged = false;