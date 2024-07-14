-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here

-- SET all current dates to first stock exchange existence in New York
UPDATE master_data 
SET start_nyse = '1792-05-17'
WHERE start_nyse IS NOT NULL;
UPDATE master_data 
SET start_nyse_arca  = '1792-05-17'
WHERE start_nyse_arca IS NOT NULL;
UPDATE master_data 
SET start_nyse_american  = '1792-05-17'
WHERE start_nyse_american IS NOT NULL;
UPDATE master_data 
SET start_nasdaq  = '1792-05-17'
WHERE start_nasdaq IS NOT NULL;
UPDATE master_data 
SET start_nasdaq_global_select_market  = '1792-05-17'
WHERE start_nasdaq_global_select_market IS NOT NULL;
UPDATE master_data 
SET start_nasdaq_select_market  = '1792-05-17'
WHERE start_nasdaq_select_market IS NOT NULL;
UPDATE master_data 
SET start_nasdaq_select_market  = '1792-05-17'
WHERE start_nasdaq_select_market IS NOT NULL;
UPDATE master_data 
SET start_nasdaq_capital_market  = '1792-05-17'
WHERE start_nasdaq_capital_market IS NOT NULL;


-- Set all to unstaged, so the dates in master_data table will be updated
update financialmodelingprep_company_profile
set is_staged = false;