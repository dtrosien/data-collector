-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

CREATE TABLE Polygon_open_close (
    after_Hours NUMERIC(15, 4),
    close NUMERIC(15, 4) NOT NULL,
    business_date DATE NOT NULL,
    high NUMERIC(15, 4) NOT NULL,
    low NUMERIC(15, 4) NOT NULL,
    open NUMERIC(15, 4) NOT NULL,
    pre_Market NUMERIC(15, 4),
    symbol VARCHAR(10) NOT NULL,
    volume INTEGER NOT NULL,
    is_staged BOOLEAN DEFAULT false,
    dateLoaded DATE DEFAULT CURRENT_DATE,
    PRIMARY KEY (business_date, symbol)
);
create index staged_Polygon_open_close on Polygon_open_close using btree (is_staged desc);

cluster verbose Polygon_open_close using staged_Polygon_open_close;
analyse verbose Polygon_open_close;