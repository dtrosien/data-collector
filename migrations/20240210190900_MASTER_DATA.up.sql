-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE MASTER_DATA ADD COLUMN Instrument Varchar(100);
ALTER TABLE MASTER_DATA DROP COLUMN is_company;
update nyse_instruments set is_staged = false;