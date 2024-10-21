CREATE TABLE market_data (
    symbol                VARCHAR(10)    NOT NULL,
    business_date         DATE           NOT NULL,
    year_month            INTEGER        not null,
    stock_price           FLOAT          NOT NULL,
    open                  FLOAT,
    close                 FLOAT,
    stock_traded          FLOAT,
    order_amount          FLOAT,
    after_hours           FLOAT,
    pre_market            FLOAT,
    market_capitalization FLOAT,
    PRIMARY KEY (symbol, business_date, year_month)
) PARTITION BY LIST (year_month);
CREATE TABLE market_data_default PARTITION OF market_data default;

CREATE OR REPLACE FUNCTION market_data_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.business_date IS NOT NULL AND (CAST ((EXTRACT(YEAR FROM new.business_date) * 100) + EXTRACT(MONTH FROM new.business_date) AS INTEGER)  <> new.year_month) THEN
		update market_data set year_month = CAST ((EXTRACT(YEAR FROM business_date) * 100) + EXTRACT(MONTH FROM business_date) AS INTEGER) 
         where (CAST ((EXTRACT(YEAR FROM business_date) * 100) + EXTRACT(MONTH FROM business_date) AS INTEGER)  <> year_month)
            AND symbol = NEW.symbol;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

create or replace TRIGGER market_data_trigger_update
after UPDATE of business_date, year_month ON market_data
FOR EACH ROW
EXECUTE FUNCTION market_data_trigger_function();