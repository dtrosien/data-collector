-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

CREATE TABLE Polygon_grouped_daily (
    close NUMERIC(15, 4) NOT NULL,
    business_date DATE NOT NULL,
    high NUMERIC(15, 4) NOT NULL,
    low NUMERIC(15, 4) NOT NULL,
    open NUMERIC(15, 4) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    stock_volume INTEGER,
    traded_volume NUMERIC(15, 4) NOT NULL,
    volume_weighted_average_price NUMERIC(15, 4),
    is_staged BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (business_date, symbol)
);
create index staged_Polygon_grouped_daily on Polygon_grouped_daily using btree (is_staged desc);

cluster verbose Polygon_grouped_daily using staged_Polygon_grouped_daily;
analyse verbose Polygon_grouped_daily;
