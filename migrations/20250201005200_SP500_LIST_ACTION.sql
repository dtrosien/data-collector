
CREATE type sp500_list_action AS ENUM ('ADDED', 'REMOVED');
CREATE TABLE sp500_changes (
    symbol VARCHAR(10) NOT NULL,
    business_date DATE NOT NULL,
    action sp500_list_action NOT NULL,
    PRIMARY KEY (symbol, business_date)
);

CREATE OR REPLACE FUNCTION sp500(cutoff_date DATE)
RETURNS TABLE(symbol varchar )
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
   select sub.symbol from (
     select sc.symbol, count(sc.symbol) as s_count 
     from sp500_changes sc 
     where business_date < cutoff_date
     group by sc.symbol) sub
   where  sub.s_count % 2 = 1 ;
END;
$$;