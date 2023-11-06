-- Add migration script here
-- Create Example Table
CREATE TABLE example
(
    id            uuid        NOT NULL,
    PRIMARY KEY (id),
    email         TEXT        NOT NULL UNIQUE,
    name          TEXT        NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE NYSE_EVENTS (
    action_date date NOT NULL,
    action_status varchar(100) NOT NULL,
    action_type varchar(100) NOT NULL,
    issue_symbol varchar(100) NOT NULL,
    issuer_name varchar(200) NOT NULL,
    updated_at varchar(100) NOT NULL,
    market_event varchar(36) NOT NULL,
    is_staged boolean DEFAULT false,
    PRIMARY KEY (action_date, issue_symbol, issuer_name, market_event, is_staged)
)
PARTITION BY LIST (is_staged);

CREATE TABLE NYSE_EVENTS_STAGED PARTITION OF NYSE_EVENTS
  FOR VALUES in (true);
CREATE TABLE NYSE_EVENTS_NOT_STAGED PARTITION OF NYSE_EVENTS
  FOR VALUES in (false);