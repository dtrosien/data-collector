-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE public.polygon_open_close ALTER COLUMN volume DROP NOT NULL;
