-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

ALTER TABLE public.financialmodelingprep_company_profile ALTER COLUMN company_name TYPE varchar(1000) USING company_name::varchar(1000);

