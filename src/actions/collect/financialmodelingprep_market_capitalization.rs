use crate::api_keys::api_key::{ApiKey, ApiKeyPlatform, Status};
use crate::api_keys::key_manager::KeyManager;
use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap};
use async_trait::async_trait;
use chrono::{Days, Duration, NaiveDate, Utc};
use futures_util::TryFutureExt;
use reqwest::Client;
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sqlx::PgPool;
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex};

use tracing::{debug, info, warn};

const URL: &str = "https://financialmodelingprep.com/api/v3/historical-market-capitalization/";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Financialmodelingprep;
const WAIT_FOR_KEY: bool = false;

struct IssueSymbols {
    issue_symbol: String,
}

#[derive(Debug)]
struct FinancialmodelingprepMarketCapitalizationRequest<'a> {
    base: String,
    api_key: &'a mut Box<dyn ApiKey>,
}

impl FinancialmodelingprepMarketCapitalizationRequest<'_> {
    fn expose_secret(&mut self) -> String {
        self.base.clone() + self.api_key.get_secret().expose_secret()
    }
}

impl Display for FinancialmodelingprepMarketCapitalizationRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.base)?;
        Secret::new(self.api_key.expose_secret_for_data_structure().clone()).fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct FinancialmodelingprepMarketCapitalizationCollector {
    pool: PgPool,
    client: Client,
    // api_key: Option<Secret<String>>,
    key_manager: Arc<Mutex<KeyManager>>,
}

impl FinancialmodelingprepMarketCapitalizationCollector {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(
        pool: PgPool,
        client: Client,
        // api_key: Option<Secret<String>>,
        key_manager: Arc<Mutex<KeyManager>>,
    ) -> Self {
        FinancialmodelingprepMarketCapitalizationCollector {
            pool,
            client,
            // api_key,
            key_manager,
        }
    }
}

impl Display for FinancialmodelingprepMarketCapitalizationCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FinancialmodelingprepMarketCapitalizationColletor struct."
        )
    }
}

#[async_trait]
impl Runnable for FinancialmodelingprepMarketCapitalizationCollector {
    #[tracing::instrument(
        name = "Run FinancialmodelingprepMarketCapitalizationColletor",
        skip(self)
    )]
    async fn run(&self) -> Result<Option<StatsMap>, crate::dag_schedule::task::TaskError> {
        // if let Some(key) = &self.api_key {
        load_and_store_missing_data(
            self.pool.clone(),
            self.client.clone(),
            self.key_manager.clone(),
        )
        .map_err(UnexpectedError)
        .await?;
        // } else {
        //     error!("No Api key provided for FinancialmodelingprepMarketCapitalizationColletor");
        //     return Err(UnexpectedError(Error::msg(
        //         "FinancialmodelingprepMarketCapitalizationColletor key not provided",
        //     )));
        // }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Responses {
    NotFound(Vec<EmptyStruct>),
    Data(Vec<MarketCapElement>),
    KeyExhausted(ErrorStruct),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmptyStruct;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorStruct {
    #[serde(rename = "Error Message")]
    error_message: String,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketCapElement {
    symbol: String,
    date: NaiveDate,
    market_cap: f64,
}

pub struct MarketCapTransposed {
    symbol: Vec<String>,
    date: Vec<NaiveDate>,
    market_cap: Vec<f64>,
}

impl MarketCapTransposed {
    fn new(input: Vec<MarketCapElement>) -> MarketCapTransposed {
        let mut result = MarketCapTransposed {
            symbol: vec![],
            date: vec![],
            market_cap: vec![],
        };

        for data in input {
            result.symbol.push(data.symbol);
            result.date.push(data.date);
            result.market_cap.push(data.market_cap);
        }

        result
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_and_store_missing_data(
    connection_pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Result<(), anyhow::Error> {
    load_and_store_missing_data_given_url(connection_pool, client, key_manager, URL).await
}

#[tracing::instrument(level = "debug", skip_all)]
async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
    url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load Financialmodelingprep Market Capitalization Collector.");
    let mut potential_issue_sybmol: Option<String> =
        get_next_uncollected_issue_symbol(&connection_pool).await?;

    info!("Next symbol: {:?}", potential_issue_sybmol);
    let mut general_api_key = get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY).await;
    let mut _successful_request_counter: u16 = 0;
    while let (Some(issue_sybmol), true) =
        (potential_issue_sybmol.as_ref(), general_api_key.is_some())
    {
        let mut api_key = general_api_key.unwrap();
        info!("Searching start date for symbol {}", &issue_sybmol);
        let mut start_request_date: NaiveDate;
        {
            start_request_date = search_start_date(&connection_pool, issue_sybmol).await?;
        }
        info!("Requesting symbol {}", &issue_sybmol);
        while start_request_date < Utc::now().date_naive() && api_key.get_status() == Status::Ready
        {
            let mut request = create_polygon_market_capitalization_request(
                url,
                issue_sybmol,
                &start_request_date,
                &mut api_key,
            );
            debug!(
                "Financialmodelingprep market capitalization request: {}",
                request
            );
            let response = client
                .get(&request.expose_secret())
                .send()
                .await?
                .text()
                .await?;
            debug!("Response: {}", response);
            //TODO: Handle error
            let parsed = crate::utils::action_helpers::parse_response::<Responses>(&response)?;
            match parsed {
                Responses::Data(data) => {
                    store_data(data, &connection_pool).await?;
                    _successful_request_counter += 1;
                }
                Responses::KeyExhausted(_) => {
                    println!("Key exhaustion detected");
                    api_key.set_status(Status::Exhausted);
                }
                Responses::NotFound(_) => {
                    info!("Stock symbol '{}' not found.", issue_sybmol);
                    add_missing_issue_symbol(issue_sybmol, &connection_pool).await?;
                }
            }

            start_request_date = start_request_date
                .checked_add_days(Days::new(1313))
                .expect("Should not leave date range.");
        }
        potential_issue_sybmol = get_next_uncollected_issue_symbol(&connection_pool).await?;
        if api_key.get_status() == Status::Ready {
            general_api_key = Some(api_key);
        } else {
            general_api_key =
                exchange_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, api_key).await;
            println!("general_api_key {:?}", general_api_key);
        }
    }
    if let Some(api_key) = general_api_key {
        let mut d = key_manager.lock().expect("msg");
        d.add_key_by_platform(api_key);
    }
    Ok(())
}

async fn exchange_apikey_or_wait(
    key_manager: Arc<Mutex<KeyManager>>,
    wait: bool,
    api_key: Box<dyn ApiKey>,
) -> Option<Box<dyn ApiKey>> {
    {
        let mut d = key_manager.lock().expect("msg");
        d.add_key_by_platform(api_key);
    }
    get_new_apikey_or_wait(key_manager, wait).await
}

async fn get_new_apikey_or_wait(
    key_manager: Arc<Mutex<KeyManager>>,
    wait: bool,
) -> Option<Box<dyn ApiKey>> {
    let mut g = {
        let mut d = key_manager.lock().expect("msg");
        d.get_key_and_timeout(PLATFORM)
    };
    while let Ok(f) = g {
        match f {
            (Some(_), Some(_)) => return None, // Cannot occur
            // Queue is empty
            (None, None) => {
                if wait {
                    tokio::time::sleep(Duration::minutes(1).to_std().unwrap()).await;
                } else {
                    return None;
                }
            }
            (None, Some(refresh_time)) => {
                if wait {
                    let time_difference = refresh_time - Utc::now();
                    tokio::time::sleep(time_difference.to_std().unwrap()).await;
                // TODO: If they are very close to each other this could fail
                } else {
                    return None;
                }
            }
            (Some(key), None) => return Some(key),
        }
        g = {
            let mut d = key_manager.lock().expect("msg");
            d.get_key_and_timeout(PLATFORM)
        };
    }
    None // Key never added to queue
}

async fn search_start_date(
    connection_pool: &PgPool,
    issue_sybmol: &String,
) -> Result<NaiveDate, anyhow::Error> {
    let result = sqlx::query!(
        "select max(business_date) 
        from financialmodelingprep_market_cap 
        where symbol = $1::text 
        group by symbol",
        issue_sybmol
    )
    .fetch_optional(connection_pool)
    .await?;
    info!("Found date in database: {:?}", result);
    if let Some(query_result) = result {
        return Ok(query_result.max.expect("Checked earlier"));
    }

    let result = sqlx::query!(
        "select concat(start_nyse, start_nyse_arca, start_nyse_american, start_nasdaq, start_nasdaq_global_select_market, start_nasdaq_select_market, start_nasdaq_capital_market, start_cboe) as ipo_date from master_data_eligible md where issue_symbol = $1::text order by issue_symbol",
        issue_sybmol
    )
    .fetch_one(connection_pool)
    .await?;
    let ipo_date = NaiveDate::parse_from_str(
        &result.ipo_date.expect("Existence checked before"),
        "%Y-%m-%d",
    )
    .expect("Parsing issue_symbol. Issue symbol is listed in non or multiple stock markets, but should be in exactly one.");
    //TODO: Add search of first data, if ipo date is initialization date
    Ok(ipo_date)
}

async fn get_next_uncollected_issue_symbol(
    connection_pool: &PgPool,
) -> Result<Option<String>, anyhow::Error> {
    let missing_issue_symbols = sqlx::query_as!(
        IssueSymbols,
        "SELECT issue_symbol FROM source_symbol_warden ssw  where financial_modeling_prep = false"
    )
    .fetch_all(connection_pool)
    .await?;
    let missing_issue_symbols = missing_issue_symbols
        .into_iter()
        .map(|issue_symbol| issue_symbol.issue_symbol)
        .collect::<Vec<_>>();

    let query_result = sqlx::query!(
        "select issue_symbol from master_data_eligible mde
         where
        (start_nyse != '1792-05-17' or start_nyse is null) and
        (start_nyse_arca != '1792-05-17' or start_nyse_arca is null) and
        (start_nyse_american != '1792-05-17' or start_nyse_american is null) and
        (start_nasdaq != '1792-05-17' or start_nasdaq is null) and
        (start_nasdaq_global_select_market != '1792-05-17' or start_nasdaq_global_select_market is null) and
        (start_nasdaq_select_market != '1792-05-17' or start_nasdaq_select_market is null) and
        (start_nasdaq_capital_market != '1792-05-17' or start_nasdaq_capital_market is null) and
        (start_cboe != '1792-05-17' or start_cboe is null) and
        (issue_symbol not in (select unnest($1::text[]))) and
        (issue_symbol not in
          (select distinct(symbol)
           from financialmodelingprep_market_cap))
        order by issue_symbol limit 1",
        &missing_issue_symbols
    )
    .fetch_one(connection_pool)
    .await;
    if let Ok(r) = query_result {
        return Ok(r.issue_symbol);
    }

    // // Check if there is a symbol missing in the set of lower priority
    let query_result = sqlx::query!(
        "select issue_symbol from master_data_eligible mde
         where
        not (start_nyse != '1792-05-17' or start_nyse is null) and
        (start_nyse_arca != '1792-05-17' or start_nyse_arca is null) and
        (start_nyse_american != '1792-05-17' or start_nyse_american is null) and
        (start_nasdaq != '1792-05-17' or start_nasdaq is null) and
        (start_nasdaq_global_select_market != '1792-05-17' or start_nasdaq_global_select_market is null) and
        (start_nasdaq_select_market != '1792-05-17' or start_nasdaq_select_market is null) and
        (start_nasdaq_capital_market != '1792-05-17' or start_nasdaq_capital_market is null) and
        (start_cboe != '1792-05-17' or start_cboe is null) and issue_symbol not in (select unnest($1::text[]))
         order by issue_symbol limit 1",
        &missing_issue_symbols
    )
    .fetch_one(connection_pool)
    .await;

    match query_result {
        Ok(result) => Ok(result.issue_symbol),
        Err(_) => get_next_outdated_issue_symbol(connection_pool).await,
    }
}

// TODO: Misinterprets unlisted issue_symbols as active and creates and endless loop further upstream.
async fn get_next_outdated_issue_symbol(
    connection_pool: &PgPool,
) -> Result<Option<String>, anyhow::Error> {
    let result = sqlx::query!("select r.symbol, r.maxDate from
(select symbol ,max(business_date) as maxDate from financialmodelingprep_market_cap group by symbol) as r
order by r.maxDate asc limit 1").fetch_one(connection_pool).await?;

    if let Some(date) = result.maxdate {
        //If date is today or yesterday, then the dataset is up to date.
        if date == Utc::now().date_naive() || date == Utc::now().date_naive() - Duration::days(1) {
            return Ok(Option::None);
        } else {
            return Ok(Some(result.symbol));
        }
    }
    Ok(Option::None)
}

async fn store_data(
    data: Vec<MarketCapElement>,
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    let transposed_data = MarketCapTransposed::new(data);

    sqlx::query!(
        r#"INSERT INTO financialmodelingprep_market_cap (symbol, business_date, market_cap)
        Select * from UNNEST ($1::text[], $2::date[], $3::float[]) on conflict do nothing"#,
        &transposed_data.symbol[..],
        &transposed_data.date[..],
        &transposed_data.market_cap[..]
    )
    .execute(connection_pool)
    .await?;

    Ok(())
}

async fn add_missing_issue_symbol(
    issue_symbol: &str,
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r#"INSERT INTO source_symbol_warden (issue_symbol, financial_modeling_prep)
      VALUES($1, false)
      ON CONFLICT(issue_symbol)
      DO UPDATE SET
        financial_modeling_prep = false"#,
        issue_symbol
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

///  Example output https://financialmodelingprep.com/api/v3/historical-market-capitalization/AAPL?limit=1313&from=1980-01-01&to=1990-01-01&apikey=TOKEN
#[tracing::instrument(level = "debug", skip_all)]
fn create_polygon_market_capitalization_request<'a>(
    base_url: &'a str,
    issue_symbol: &str,
    start_date: &NaiveDate,
    api_key: &'a mut Box<dyn ApiKey>,
) -> FinancialmodelingprepMarketCapitalizationRequest<'a> {
    let end_date = start_date
        .checked_add_days(Days::new(1312))
        .expect("Should not leave date range.");

    let base_request_url = base_url.to_string()
        + issue_symbol.to_string().as_str()
        + "?limit=1313&from="
        + &start_date.to_string()
        + "&to="
        + &end_date.to_string()
        + "&apikey=";
    FinancialmodelingprepMarketCapitalizationRequest {
        base: base_request_url,
        api_key,
    }
}

#[cfg(test)]
mod test {
    // use super::ErrorStruct;

    // use chrono::NaiveDate;

    use chrono::NaiveDate;

    use super::{MarketCapElement, Responses};

    #[test]
    fn parse_data_response() {
        let input_json = r#"[
        {
            "symbol": "A",
            "date": "2003-06-20",
            "marketCap": 6099000000
        }]
        "#;
        let parsed = crate::utils::action_helpers::parse_response::<Responses>(input_json).unwrap();

        let instrument = MarketCapElement {
            symbol: "A".to_string(),
            date: NaiveDate::parse_from_str("2003-06-20", "%Y-%m-%d").expect("Parsing constant."),
            market_cap: 6099000000.0,
        };
        match parsed {
            Responses::Data(data) => {
                assert_eq!(data[0], instrument);
            }
            _ => panic!("Data no parsable"),
        }
    }

    // #[test]
    // fn parse_data_struct_with_empty_ipo() {
    //     let input_json = r#"[
    //         {
    //           "symbol": "A",
    //           "price": 137.74,
    //           "beta": 1.122,
    //           "volAvg": 1591377,
    //           "mktCap": 40365395700,
    //           "lastDiv": 0.94,
    //           "range": "96.8-151.58",
    //           "changes": 1.37,
    //           "companyName": "Agilent Technologies, Inc.",
    //           "currency": "USD",
    //           "cik": "0001090872",
    //           "isin": "US00846U1016",
    //           "cusip": "00846U101",
    //           "exchange": "New York Stock Exchange",
    //           "exchangeShortName": "NYSE",
    //           "industry": "Medical - Diagnostics & Research",
    //           "website": "https://www.agilent.com",
    //           "description": "Agilent Technologies",
    //           "ceo": "Mr. Michael R. McMullen",
    //           "sector": "Healthcare",
    //           "country": "US",
    //           "fullTimeEmployees": "17700",
    //           "phone": "800 227 9770",
    //           "address": "5301 Stevens Creek Boulevard",
    //           "city": "Santa Clara",
    //           "state": "CA",
    //           "zip": "95051",
    //           "dcfDiff": 53.46901,
    //           "dcf": 84.27099210145948,
    //           "image": "https://financialmodelingprep.com/image-stock/A.png",
    //           "ipoDate": "",
    //           "defaultImage": false,
    //           "isEtf": false,
    //           "isActivelyTrading": true,
    //           "isAdr": false,
    //           "isFund": false
    //         }
    //       ]"#;
    //     let parsed =
    //         crate::utils::action_helpers::parse_response::<Vec<CompanyProfileElement>>(input_json)
    //             .unwrap();

    //     let instrument = CompanyProfileElement {
    //         symbol: "A".to_string(),
    //         price: Some(137.74),
    //         beta: Some(1.122),
    //         vol_avg: Some(1591377),
    //         mkt_cap: Some(40365395700),
    //         last_div: Some(0.94),
    //         range: Some("96.8-151.58".to_string()),
    //         changes: Some(1.37),
    //         company_name: "Agilent Technologies, Inc.".to_string(),
    //         currency: Some("USD".to_string()),
    //         cik: Some("0001090872".to_string()),
    //         isin: Some("US00846U1016".to_string()),
    //         cusip: Some("00846U101".to_string()),
    //         exchange: Some("New York Stock Exchange".to_string()),
    //         exchange_short_name: Some("NYSE".to_string()),
    //         industry: Some("Medical - Diagnostics & Research".to_string()),
    //         website: Some("https://www.agilent.com".to_string()),
    //         description: Some("Agilent Technologies".to_string()),
    //         ceo: Some("Mr. Michael R. McMullen".to_string()),
    //         sector: Some("Healthcare".to_string()),
    //         country: Some("US".to_string()),
    //         full_time_employees: Some(17700),
    //         phone: Some("800 227 9770".to_string()),
    //         address: Some("5301 Stevens Creek Boulevard".to_string()),
    //         city: Some("Santa Clara".to_string()),
    //         state: Some("CA".to_string()),
    //         zip: Some("95051".to_string()),
    //         dcf_diff: Some(53.46901),
    //         dcf: Some(84.27099210145948),
    //         image: Some("https://financialmodelingprep.com/image-stock/A.png".to_string()),
    //         ipo_date: None,
    //         default_image: Some(false),
    //         is_etf: Some(false),
    //         is_actively_trading: Some(true),
    //         is_adr: Some(false),
    //         is_fund: Some(false),
    //     };
    //     assert_eq!(parsed[0], instrument);
    // }

    // #[test]
    // fn parse_empty_response() {
    //     let input_json = r#"[]"#;
    //     let parsed_response =
    //         crate::utils::action_helpers::parse_response::<Responses>(input_json).unwrap();

    //     match parsed_response {
    //         Responses::NotFound(a) => assert_eq!(a.len(), 0),
    //         _ => panic!("Wrong parse!"),
    //     }
    // }

    // #[test]
    // fn parse_empty_struct() {
    //     let input_json = r#"[]"#;
    //     let parsed_response =
    //         crate::utils::action_helpers::parse_response::<Vec<EmptyStruct>>(input_json).unwrap();

    //     assert_eq!(parsed_response.len(), 0);
    // }

    // #[test]
    // fn parse_error_response() {
    //     let input_json = r#"{"Error Message": "Limit Reach."}"#;
    //     let parsed_response =
    //         crate::utils::action_helpers::parse_response::<Responses>(input_json).unwrap();

    //     match parsed_response {
    //         Responses::KeyExhausted(a) => assert_eq!(a.error_message, "Limit Reach."),
    //         _ => panic!("Wrong parse!"),
    //     };
    // }

    // #[test]
    // fn parse_error_struct() {
    //     let input_json = r#"{"Error Message": "Limit Reach."}"#;
    //     let parsed_response =
    //         crate::utils::action_helpers::parse_response::<ErrorStruct>(input_json).unwrap();

    //     assert_eq!(parsed_response.error_message, "Limit Reach.");
    // }
}
