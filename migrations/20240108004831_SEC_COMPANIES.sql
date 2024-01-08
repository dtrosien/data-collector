-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
CREATE TABLE SEC_COMPANIES (
    cik INT NOT NULL,
    sic INT,
    name VARCHAR(200) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    exchange VARCHAR(10),
    state_of_incorporation VARCHAR(2),
    date_loaded DATE DEFAULT CURRENT_DATE NOT NULL,
    is_staged BOOLEAN DEFAULT FALSE NOT NULL,
    PRIMARY KEY (name, ticker)
);
create index staged_sec_companies on SEC_COMPANIES using btree (is_staged desc);

cluster verbose SEC_COMPANIES using staged_sec_companies;
analyse verbose SEC_COMPANIES;