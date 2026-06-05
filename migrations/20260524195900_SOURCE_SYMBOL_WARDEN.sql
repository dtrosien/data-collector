-- noinspection SqlNoDataSourceInspectionForFile


ALTER TABLE public.source_symbol_warden ADD massive_dividends date NULL;
COMMENT ON COLUMN public.source_symbol_warden.massive_dividends IS 'Stores the last date, when a symbol was not existing in the massive data set';
