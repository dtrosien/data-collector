# Polygon.io

This is an API provider, which provides access to 5 API calls per minute, 2 years of historical data as well as access to master data. The access is organized via REST calls and an authentication key. There is NYSE data but no NASDAQ data.

## End of day and volume

The end of day [documentation](https://polygon.io/docs/stocks/get_v1_open-close__stocksticker___date) requires the stock ticker symbol, exactly one date and if the result should be adjusted for splits and other actions.

The request is:
```http
https://api.polygon.io/v1/open-close/AAPL/2023-01-09?adjusted=true&apiKey=PutYourKeyHere
```

The respond looks like 
```json
{
  "afterHours": 322.1,
  "close": 325.12,
  "from": "2023-01-09",
  "high": 326.2,
  "low": 322.3,
  "open": 324.66,
  "preMarket": 324.5,
  "status": "OK",
  "symbol": "AAPL",
  "volume": 26122646
}
```
The schema from the documentation is as follows
| Name      | Data Type | Nullable | Description                                                |
|-----------|-----------|----------|------------------------------------------------------------|
| afterHours| number    | Yes      | The close price of the ticker symbol in after hours trading.|
| close     | number    | No       | The close price for the symbol in the given time period.    |
| from      | string    | No       | The requested date.                                        |
| high      | number    | No       | The highest price for the symbol in the given time period.  |
| low       | number    | No       | The lowest price for the symbol in the given time period.   |
| open      | number    | No       | The open price for the symbol in the given time period.     |
| otc       | boolean   | Yes      | Whether or not this aggregate is for an OTC ticker. This field will be left off if false.|
| preMarket | integer   | Yes      | The open price of the ticker symbol in pre-market trading.  |
| status    | string    | No       | The status of this request's response.                      |
| symbol    | string    | No       | The exchange symbol that this item is traded under.         |
| volume    | number    | No       | The trading volume of the symbol in the given time period.  |


## Master data

Ticker information can be [requested](https://polygon.io/docs/stocks/get_v3_reference_tickers) specifically for a given ticker or in general per stock exchange. <i>Common stock</i> is supported, while <i>REIT</i> and <i>Equity</i> are not supported.