-- noinspection SqlNoDataSourceInspectionForFile

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
