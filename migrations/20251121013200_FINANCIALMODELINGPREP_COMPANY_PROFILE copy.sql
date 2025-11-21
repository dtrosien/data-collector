-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE public.financialmodelingprep_company_profile RENAME COLUMN mkt_cap TO "marketCap";
ALTER TABLE public.financialmodelingprep_company_profile RENAME COLUMN last_div TO "lastDividend";
ALTER TABLE public.financialmodelingprep_company_profile RENAME COLUMN changes TO "change";
ALTER TABLE public.financialmodelingprep_company_profile ADD changepercentage float8 NULL;
ALTER TABLE public.financialmodelingprep_company_profile ADD volume int8 NULL;
ALTER TABLE public.financialmodelingprep_company_profile ADD averageVolume int8 NULL;
ALTER TABLE public.financialmodelingprep_company_profile RENAME COLUMN exchange TO "exchangeFullName";
ALTER TABLE public.financialmodelingprep_company_profile RENAME COLUMN exchange_short_name TO exchange;
ALTER TABLE public.financialmodelingprep_company_profile DROP COLUMN dcf_diff;
ALTER TABLE public.financialmodelingprep_company_profile DROP COLUMN dcf;
