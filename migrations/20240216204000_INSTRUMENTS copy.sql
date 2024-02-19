-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
update instruments 
set is_sp500_eligible = false 
where instrument = 'LIMITED_PARTNERSHIP';
