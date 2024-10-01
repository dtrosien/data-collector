-- Add several partitions
CREATE TABLE market_data_202401 PARTITION OF market_data
    FOR VALUES  in (202401) ;
CREATE TABLE market_data_202402 PARTITION OF market_data
    FOR VALUES  in (202402) ;
CREATE TABLE market_data_200002 PARTITION OF market_data
    FOR VALUES in (200002) ;