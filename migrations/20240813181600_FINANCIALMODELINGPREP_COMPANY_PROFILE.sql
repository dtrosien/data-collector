-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

ALTER TABLE public.financialmodelingprep_company_profile ALTER COLUMN company_name DROP NOT NULL;

