-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
ALTER TABLE public.polygon_open_close ALTER COLUMN volume DROP NOT NULL;
ALTER TABLE public.polygon_open_close ALTER COLUMN "close" DROP NOT NULL;
ALTER TABLE public.polygon_open_close ALTER COLUMN business_date SET NOT NULL;
ALTER TABLE public.polygon_open_close ALTER COLUMN high DROP NOT NULL;
ALTER TABLE public.polygon_open_close ALTER COLUMN low DROP NOT NULL;
ALTER TABLE public.polygon_open_close ALTER COLUMN "open" DROP NOT NULL;

