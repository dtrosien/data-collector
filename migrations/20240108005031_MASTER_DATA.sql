-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
CREATE TABLE MASTER_DATA (
	issuer_name VARCHAR(180) not NULL,
	issue_symbol VARCHAR(10) not NULL,
	Location VARCHAR(3),
	Start_of_listing_NYSE DATE,
	Start_of_listing_NYSE_Arca DATE,
	Start_of_listing_NYSE_American DATE,
	Start_of_listing_NASDAQ DATE,
	Start_of_listing_NASDAQ_Global_Select_Market DATE,
	Start_of_listing_NASDAQ_Select_Market DATE,
	Start_of_listing_NASDAQ_Capital_Market DATE,
	is_company BOOLEAN,
	Category VARCHAR(100),
	Renamed_to_issuer_name VARCHAR(180),
	Renamed_to_issue_symbol VARCHAR(10),
	Renamed_at_date DATE,
	Current_name VARCHAR(180),
	Suspended BOOLEAN,
	Suspension_date DATE,
	primary key (issuer_name, issue_symbol)
);
comment on column MASTER_DATA."location"  is 'ISO 3166-1 alpha-3';