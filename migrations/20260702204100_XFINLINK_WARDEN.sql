-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

ALTER TABLE public.source_symbol_warden ADD COLUMN xfinlink date DEFAULT NULL;
COMMENT ON COLUMN public.source_symbol_warden.xfinlink IS 'Date when symbol was last found missing in Xfinlink; NULL = not checked; re-checked after 30 days';
