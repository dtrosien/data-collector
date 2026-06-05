-- noinspection SqlNoDataSourceInspectionForFile

ALTER TABLE public.polygon_dividends ALTER COLUMN historical_adjustment_factor TYPE numeric(38, 12) USING historical_adjustment_factor::numeric(38, 12);
ALTER TABLE public.polygon_dividends ALTER COLUMN split_adjusted_cash_amount TYPE numeric(38, 12) USING split_adjusted_cash_amount::numeric(38, 12);
ALTER TABLE public.polygon_dividends ALTER COLUMN cash_amount TYPE numeric(38, 12) USING cash_amount::numeric(38, 12);
