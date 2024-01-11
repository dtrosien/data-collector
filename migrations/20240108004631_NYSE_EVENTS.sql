-- noinspection SqlNoDataSourceInspectionForFile

-- Add migration script here
CREATE TABLE NYSE_EVENTS (
	action_date date NOT NULL,
	action_status varchar(100) NOT NULL,
	action_type varchar(100) NOT NULL,
	issue_symbol varchar(100) NOT NULL,
	issuer_name varchar(200) NOT NULL,
	updated_at varchar(100) NOT NULL,
	market_event varchar(36) NOT NULL,
	is_staged boolean NOT NULL DEFAULT false,
	PRIMARY KEY (action_date, issue_symbol, issuer_name, market_event)
);
create index staged_events on NYSE_EVENTS using btree (is_staged desc);

cluster verbose NYSE_EVENTS using staged_events;
analyse verbose NYSE_EVENTS;


