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

## Responses and staging rules
The responds from the API contain useful as well as non-useful information for us. The information can be clustered using the `action_status` response. 

| Event                                              | Comment                                                 | Staging behavior     |
| -------------------------------------------------- | ------------------------------------------------------- | -------------------- |
| Admit                                              | Enter a market                                          | Stage without action |
| Admit - "Ex-Distribution" Market                   | Enter a market                                          | Stage without action |
| Admit - "When-Distributed" Market                  | Enter a market                                          | Stage without action |
| Admit - "When Issued", Additional Shares, Market   | Enter a market                                          | Stage without action |
| Admit - "When-Issued" Market                       | Enter a market                                          | Stage without action |
| Change Company Name/Product Name                   |                                                         | TO-DO                |
| Change Company Name/Product Name/CUSIP             |                                                         | TO-DO                |
| Change Company Name/Product Name/Symbol            |                                                         | TO-DO                |
| Change Company Name/Product Name/Symbol/CUSIP      |                                                         | TO-DO                |
| Change CUSIP                                       | Rating change of stock                                  | Stage without action |
| Change DMM / Post                                  | Change the responsible market maker                     | Stage without action |
| Change in Terms                                    |                                                         | Stage without action |
| Change Name                                        |                                                         | TO-DO                |
| Change Name/CUSIP                                  |                                                         | TO-DO                |
| Change Name/Symbol                                 |                                                         | TO-DO                |
| Change Name/Symbol/CUSIP                           |                                                         | TO-DO                |
| Change Product Name                                |                                                         | Stage without action |
| Change Product Name/CUSIP                          |                                                         | Stage without action |
| Change Product Name/Symbol                         |                                                         | TO-DO                |
| Change Symbol                                      |                                                         | TO-DO                |
| Change Underlying Share Ratio                      | Change ratio of stock with voting rights                | Stage without action |
| Direct Listing                                     | Enter listing                                           | Stage without action |
| Full Call                                          | Issuer buys all it's own stock                          | Stage without action |
| Initial Public Offering                            |                                                         | Stage without action |
| New Listing                                        | New listing can occur, when stock exchanges are changed | Stage without action |
| New Structured Product                             |                                                         | Stage without action |
| Other Action Type                                  |                                                         | Stage without action |
| Reclassification                                   |                                                         | Stage without action |
| Regular-Way in lieu of "When-Issued"               |                                                         | Stage without action |
| Reverse Stock Split                                |                                                         | Stage without action |
| Suspend                                            |                                                         | Stage without action |
| Suspend & Delist - Voluntary                       |                                                         | TO-DO                |
| Suspend - "Ex-Distribution" Market                 |                                                         | Stage without action |
| Suspend - For - Cause                              |                                                         | Stage without action |
| Suspend - Voluntary                                |                                                         | Stage without action |
| Suspend - "When-Distributed" Market                |                                                         | Stage without action |
| Suspend - "When-Issued", Additional Shares, Market |                                                         | Stage without action |
| Suspend - "When-Issued" Market                     |                                                         | Stage without action |

## Staging
Stage first event as initial listing time
~~~sql
update master_data as md set start_of_listing_nyse = tmp.min_date from
  (select min(action_date) as min_date, issuer_name from nyse_events ne where action_type != 'Suspend & Delist - Voluntary' group by issuer_name) as tmp 
where md.issuer_name = tmp.issuer_name;
~~~
Stage delisting event
~~~sql
update master_data as md set suspended = tmp.is_suspended, suspension_date =  tmp.action_date from 
  (select issuer_name, issue_symbol, action_date ,true as is_suspended from nyse_events where action_type = 'Suspend & Delist - Voluntary') as tmp
where md.issuer_name = tmp.issuer_name and md.issue_symbol = tmp.issue_symbol;
~~~

## Summary

With this API it is possible to receive the offered stocks (symbol + company name) and also get the entry/exit dates. 

