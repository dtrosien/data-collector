-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
create view master_data_eligible as
    select * from master_data md 
    where 
            instrument    in (select instrument             from instruments i where is_sp500_eligible = true)
        and md."location" in (select distinct(country_code) from countries c where is_sp500_eligible = true); 