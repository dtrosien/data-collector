# Companies

Current companies:</br>
- https://www.slickcharts.com/sp500
- https://datahub.io/core/s-and-p-500-companies

# Weights in index

Current weights: https://www.slickcharts.com/sp500

# Stock data access

- https://polygon.io/pricing - 5 calls per minute (need to check what this means) - https://polygon.io/docs/stocks/get_v3_trades__stockticker offers stock trade volume

# Master data

- https://www.nyse.com/corporate-actions - get .json with: https://listingmanager.nyse.com/api/corpax/?action_date__gte=2023-09-01&action_date__lte=2023-10-01&page=1&page_size=20. It is possible to get the trading name (symbol) and the company name from here. (Earliest record: 2015-12-07)

## List of stock data providers

- http://www.columbia.edu/~tmd2142/best-6-stock-market-apis-for-2020.html overview list
- https://polygon.io - 5 calls per minute - 2 years historical data
- https://www.nyse.com/corporate-actions - get .json with: https://listingmanager.nyse.com/api/corpax/?action_date__gte=2023-09-01&action_date__lte=2023-10-01&page=1&page_size=20 