# NYSE instruments

Query message:

~~~bash

curl -X POST https://www.nyse.com/api/quotes/filter -H 'Content-Type: application/json' -d '{"instrumentType":"COMMON_STOCK","pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":3,"filterToken":""}' > response.json
~~~
Response:
~~~json
[
    {
        "total": 5083,
        "url": "https://www.nyse.com/quote/XNYS:A",
        "exchangeId": "558",
        "instrumentType": "COMMON_STOCK",
        "symbolTicker": "A",
        "symbolExchangeTicker": "A",
        "normalizedTicker": "A",
        "symbolEsignalTicker": "A",
        "instrumentName": "AGILENT TECHNOLOGIES INC",
        "micCode": "XNYS"
    }
]
~~~

