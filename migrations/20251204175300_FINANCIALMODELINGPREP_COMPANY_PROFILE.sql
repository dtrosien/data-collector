-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE public.financialmodelingprep_company_profile ALTER COLUMN address TYPE varchar(500) USING address::varchar(500);
