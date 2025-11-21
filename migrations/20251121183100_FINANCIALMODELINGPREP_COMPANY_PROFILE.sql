-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE public.financialmodelingprep_company_profile ALTER COLUMN averagevolume TYPE float8 USING averagevolume::float8;

