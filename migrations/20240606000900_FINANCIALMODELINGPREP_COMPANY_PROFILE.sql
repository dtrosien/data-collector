-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

-- Extend website page length, since there is actually a company with 136 characters
ALTER TABLE financialmodelingprep_company_profile ALTER COLUMN website TYPE varchar(1000) USING website::varchar(1000);
