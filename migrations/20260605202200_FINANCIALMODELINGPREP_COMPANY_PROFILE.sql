-- noinspection SqlNoDataSourceInspectionForFile

ALTER TABLE public.financialmodelingprep_company_profile ALTER COLUMN volume TYPE float8 USING volume::float8;
