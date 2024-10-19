-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

ALTER TABLE polygon_grouped_daily RENAME COLUMN stock_volume TO order_amount;
ALTER TABLE polygon_grouped_daily RENAME COLUMN traded_volume TO stock_traded;