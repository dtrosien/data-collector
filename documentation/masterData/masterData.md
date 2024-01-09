# Master data


## Schema

 | Column                                       | Type            |          |
 | -------------------------------------------- | --------------- | -------- |
 | issuer_name                                  | VARCHAR(180)    | NOT NULL (PK) |
 | issue_symbol                                 | VARCHAR(10)     | NOT NULL (PK) |
 | Location                                     | VARCHAR(100)    |          |
 | Start of listing NYSE                        | Date yyyy-MM-dd |          |
 | Start of listing NYSE Arca                   | Date yyyy-MM-dd |          |
 | Start of listing NYSE American               | Date yyyy-MM-dd |          |
 | Start of listing NASDAQ                      | Date yyyy-MM-dd |          |
 | Start of listing NASDAQ Global Select Market | Date yyyy-MM-dd |          |
 | Start of listing NASDAQ Select Market        | Date yyyy-MM-dd |          |
 | Start of listing NASDAQ Capital Market       | Date yyyy-MM-dd |          |
 | Category (Company/ETF/...)                   | VARCHAR(100)    |          |
 | Renamed to issuer_name                       | VARCHAR(180)    |          |
 | Renamed to issue_symbol                      | VARCHAR(10)     |          |
 | Renamed at date                              | Date yyyy-MM-dd |          |
 | Current name                                 | VARCHAR(180)    |          |
 | Suspended                                    | Boolean         |          |
 | Suspension date                              | Date yyyy-MM-dd |          |


 ~~~sql
CREATE TABLE master_data (
	issuer_name VARCHAR(180) not NULL,
	issue_symbol VARCHAR(10) not NULL,
	Location VARCHAR(100),
	Start_of_listing_NYSE DATE,
	Start_of_listing_NYSE_Arca DATE,
	Start_of_listing_NYSE_American DATE,
	Start_of_listing_NASDAQ DATE,
	Start_of_listing_NASDAQ_Global_Select_Market DATE,
	Start_of_listing_NASDAQ_Select_Market DATE,
	Start_of_listing_NASDAQ_Capital_Market DATE,
	Category VARCHAR(100),
	Renamed_to_issuer_name VARCHAR(180),
	Renamed_to_issue_symbol VARCHAR(10),
	Renamed_at_date DATE,
	Current_name VARCHAR(180),
	Suspended BOOLEAN,
	Suspension_date DATE,
	primary key (issuer_name, issue_symbol)
);
~~~