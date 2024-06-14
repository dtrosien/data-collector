# No empty strings in application or database

## Status

In progress

## Context

This project loads and stores data from public APIs. Therefore, we have to deal with standard conversion, cleaning and casting issues. One of this issue is handling the empty string `""` which happens to be returned from some APIs. Here we want to decide on how the empty string is stored in the database and dealt with in the application.


## Options

### Store as is

An empty string `""` will be stored as it is and will also be stored in the database as empty string.

### Convert to null/None

An empty string will be converted to `Option::None` and will be stored as `null` in the database.


## Decision

Convert to null/None.


## Consequences

Actions must be implemented such that incoming data will be converted and outgoing data are valid.</br>
Reads from the database are trustworthy, such that `Option::Some` actually contains meaningful data.</br>
Actions which do not fulfill this standard must be updated.
