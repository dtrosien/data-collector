# NYSE information

The New York stock exchange offers data via REST API at:</br>
https://listingmanager.nyse.com/api/corpax/?action_date__gte=2023-09-01&action_date__lte=2023-10-01&page=1&page_size=20
</br>
and answers with a .json.</br>
``{"count":43,"next":"http://listingmanager.nyse.com/api/corpax/?action_date__gte=2023-09-01&action_date__lte=2023-10-01&&page_size=2&page=2","previous":null,"results":[{"action_date":"2023-09-01","action_status":"Effective before the Open","action_type":"Suspend","issue_symbol":"NEX","issuer_name":"NexTier Oilfield Solutions Inc.","updated_at":"2023-09-01T09:23:44.135423-04:00","market_event":"aa613982-dad8-44be-ad8d-565e1dc5158d"},{"action_date":"2023-09-11","action_status":"Effective before the Open","action_type":"Change Product Name","issue_symbol":"AGGH","issuer_name":"Simplify Exchange Traded Funds","updated_at":"2023-08-31T13:30:20.821091-04:00","market_event":"47bbca62-7911-4820-b1e0-e6e2c6cdeaed"}]}`` </br>
having the fields:</br>

| Field       |          Result           | Data type       |
|-------------|:-------------------------:|-----------------|
|action_date|2023-09-01| date yyyy-MM-dd |
| action_status    | Effective before the Open | VARCHAR(100)    |
| action_type   |Suspend| VARCHAR(100)    |
| issue_symbol    |NEX| VARCHAR(100)    |
| issuer_name | NexTier Oilfield Solutions Inc. | VARCHAR(200)    |
| action_status| Effective before the Open| VARCHAR(100)    |
| updated_at|2023-09-01T09:23:44.135423-04:00| VARCHAR(100)    |
| market_event|aa613982-dad8-44be-ad8d-565e1dc5158d| VARCHAR(?)      |

The earliest date for which data is available is the 2015-12-07; checked on 2023-09-01.

## Schema
The create statement:</br>
~~~~sql
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
~~~~
Postgres has no automatic partition creation, so they need to be created manually.
~~~~sql
CREATE TABLE NYSE_EVENTS_STAGED PARTITION OF NYSE_EVENTS
  FOR VALUES in (true);
CREATE TABLE NYSE_EVENTS_NOT_STAGED PARTITION OF NYSE_EVENTS
  FOR VALUES in (false);
~~~~

## Summary

With this API it is possible to receive the offered stocks (symbol + company name) and also get the entry/exit dates. 