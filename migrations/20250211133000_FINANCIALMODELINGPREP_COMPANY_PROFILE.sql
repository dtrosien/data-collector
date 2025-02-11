-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE public.FINANCIALMODELINGPREP_COMPANY_PROFILE ALTER COLUMN vol_avg TYPE float8 USING vol_avg::float8;
