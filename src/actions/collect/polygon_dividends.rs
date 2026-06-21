use crate::database::polygon_dividends_service::PolygonDividendsServiceTrait;
use crate::database::warden_service::WardenServiceTrait;
use crate::{
    api_keys::{
        api_key::{ApiKey, ApiKeyPlatform},
        key_manager::KeyManager,
    },
    database::{
        polygon_dividends_service::{PolygonDividendsEntry, PolygonDividendsService},
        warden_service::{WardenService, WardenType},
    },
};
use async_trait::async_trait;
use chrono::NaiveDate;
use futures_util::TryFutureExt;
use secrecy::{ExposeSecret, Secret};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};
use tracing::info;

use std::fmt::Display;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;

use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://api.massive.com/stocks/v1/dividends?";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Polygon;
const WAIT_FOR_KEY: bool = true;

#[derive(Debug)]
struct PolygonDividendsRequest<'a> {
    base: String,
    api_key: &'a mut Box<dyn ApiKey>,
}

impl PolygonDividendsRequest<'_> {
    fn expose_secret(&mut self) -> String {
        self.base.clone() + self.api_key.get_secret().expose_secret()
    }
}

impl Display for PolygonDividendsRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.base)?;
        Secret::new(self.api_key.expose_secret_for_data_structure().clone()).fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct PolygonDividendsCollector {
    pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
}

impl PolygonDividendsCollector {
    #[tracing::instrument(name = "Run Polygon dividends collector", skip_all)]
    pub fn new(pool: PgPool, client: Client, key_manager: Arc<Mutex<KeyManager>>) -> Self {
        PolygonDividendsCollector {
            pool,
            client,
            key_manager,
        }
    }
}

impl Display for PolygonDividendsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PolygonDividendsCollector struct.")
    }
}

#[async_trait]
impl Runnable for PolygonDividendsCollector {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        load_and_store_missing_data(
            self.pool.clone(),
            self.client.clone(),
            self.key_manager.clone(),
        )
        .map_err(TaskError::UnexpectedError)
        .await?;
        Ok(None)
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
async fn load_and_store_missing_data_with_services(
    polygon_dividends_service: &(dyn PolygonDividendsServiceTrait + Send + Sync),
    warden_service: &(dyn WardenServiceTrait + Send + Sync),
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
    url: &str,
) -> Result<(), anyhow::Error> {
    let skippable_symbols = warden_service
        .get_missing_symbols(crate::database::warden_service::WardenType::MassiveDividends)
        .await?;
    let mut issue_symbol_candidate = polygon_dividends_service
        .get_next_issue_symbol_candidate("".to_string(), &skippable_symbols)
        .await;
    let mut general_api_key =
        KeyManager::get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, PLATFORM).await;
    while let (Some(issue_symbol), true) = (issue_symbol_candidate, general_api_key.is_some()) {
        let mut api_key = general_api_key.unwrap();
        let mut request = create_polygon_dividends_request(url, &issue_symbol, &mut api_key);
        info!("Polygon dividends request: {}", request);
        let response = client
            .get(request.expose_secret())
            .send()
            .await?
            .text()
            .await?;
        let response_dividends =
            crate::utils::action_helpers::parse_response::<Dividends>(&response)?.results;
        // info!("response {:?}", response_dividends);
        if let Some(dividends) = response_dividends {
            if dividends.is_empty() {
                warden_service
                    .add_or_update(&issue_symbol, WardenType::MassiveDividends)
                    .await?;
            } else {
                let dividends_response_entries: Vec<_> = dividends
                    .into_iter()
                    .filter_map(map_dividend_entry)
                    .collect();
                // TODO: Highest declaration day is older than 2 years -> Put in warden

                polygon_dividends_service
                    .save_all(dividends_response_entries)
                    .await?;
            }
        }

        // TODO: Continue here. Parse result from request and store in database

        // Refresh iteration objects
        issue_symbol_candidate = polygon_dividends_service
            .get_next_issue_symbol_candidate(issue_symbol, &skippable_symbols)
            .await;
        general_api_key = KeyManager::exchange_apikey_or_wait_if_non_ready(
            key_manager.clone(),
            WAIT_FOR_KEY,
            api_key,
            PLATFORM,
        )
        .await;
    }

    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
    url: &str,
) -> Result<(), anyhow::Error> {
    let polygon_dividends_service = PolygonDividendsService::new(connection_pool.clone());
    let warden_service = WardenService::new(connection_pool.clone());
    load_and_store_missing_data_with_services(
        &polygon_dividends_service,
        &warden_service,
        client,
        key_manager,
        url,
    )
    .await
}

fn convert_string_to_naive_date(data: Option<String>) -> Option<NaiveDate> {
    match data {
        Some(x) => NaiveDate::parse_from_str(&x, "%Y-%m-%d").ok(),
        None => None,
    }
}

#[tracing::instrument(level = "debug", skip_all)]
fn create_polygon_dividends_request<'a>(
    base_url: &'a str,
    ticker_symbol: &'a str,
    api_key: &'a mut Box<dyn ApiKey>,
) -> PolygonDividendsRequest<'a> {
    let base_request_url = base_url.to_string() + "ticker=" + ticker_symbol + "&apiKey=";
    PolygonDividendsRequest {
        base: base_request_url,
        api_key,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dividends {
    status: Option<String>,
    request_id: Option<String>,
    results: Option<Vec<DividendsResponseEntry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DividendsResponseEntry {
    pub id: Option<String>,
    pub ticker: String,
    pub record_date: Option<String>,
    pub pay_date: Option<String>,
    pub declaration_date: Option<String>,
    pub ex_dividend_date: Option<String>,
    pub frequency: Option<i64>,
    pub cash_amount: Option<f64>,
    pub currency: Option<String>,
    pub distribution_type: Option<String>,
    pub historical_adjustment_factor: Option<f64>,
    pub split_adjusted_cash_amount: Option<f64>,
}

/// Map a DividendsResponseEntry into a PolygonDividendsEntry when all required fields are present.
/// Returns None when required fields are missing or parsing fails.
fn map_dividend_entry(div: DividendsResponseEntry) -> Option<PolygonDividendsEntry> {
    // Required fields: ex_dividend_date, cash_amount, currency
    let ex_div = div.ex_dividend_date?;
    let cash = div.cash_amount?;
    let currency = div.currency?;

    let ex_div_parsed = NaiveDate::parse_from_str(&ex_div, "%Y-%m-%d").ok()?;

    Some(PolygonDividendsEntry {
        ticker: div.ticker,
        record_date: convert_string_to_naive_date(div.record_date),
        pay_date: convert_string_to_naive_date(div.pay_date),
        declaration_date: convert_string_to_naive_date(div.declaration_date),
        ex_dividend_date: ex_div_parsed,
        frequency: div.frequency,
        cash_amount: cash,
        currency,
        distribution_type: div.distribution_type,
        historical_adjustment_factor: div.historical_adjustment_factor,
        split_adjusted_cash_amount: div.split_adjusted_cash_amount,
        is_staged: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_keys::api_key::{ApiKey, PolygonKey};
    use crate::database::polygon_dividends_service::MockPolygonDividendsServiceTrait;
    use crate::database::warden_service::MockWardenServiceTrait;
    use httpmock::{Method::GET, MockServer};
    use serde_json::json;
    use std::cell::RefCell;

    #[test]
    fn test_create_polygon_dividends_request_formats_url_and_masks_key() {
        let mut key: Box<dyn ApiKey> = Box::new(PolygonKey::new("secret123".to_string()));
        let mut req = create_polygon_dividends_request("https://api.test/?", "TICK", &mut key);
        assert!(req
            .base
            .starts_with("https://api.test/?ticker=TICK&apiKey="));
        // expose_secret returns base + secret
        let exposed = req.expose_secret();
        assert!(exposed.ends_with("secret123"));
    }

    #[test]
    fn test_convert_string_to_naive_date_various() {
        assert_eq!(
            convert_string_to_naive_date(Some("2020-01-02".to_string()))
                .unwrap()
                .to_string(),
            "2020-01-02"
        );
        assert_eq!(convert_string_to_naive_date(None), None);
        assert_eq!(
            convert_string_to_naive_date(Some("invalid".to_string())),
            None
        );
    }

    #[test]
    fn test_dividends_deserialize_empty_results() {
        let data = json!({
            "status": null,
            "request_id": null,
            "results": null
        });
        let s = serde_json::to_string(&data).unwrap();
        let parsed: Dividends = serde_json::from_str(&s).unwrap();
        assert!(parsed.results.is_none());
    }

    #[test]
    fn test_map_dividend_entry_missing_fields_returns_none() {
        let entry = DividendsResponseEntry {
            id: None,
            ticker: "TICK".to_string(),
            record_date: None,
            pay_date: None,
            declaration_date: None,
            ex_dividend_date: None,
            frequency: None,
            cash_amount: None,
            currency: None,
            distribution_type: None,
            historical_adjustment_factor: None,
            split_adjusted_cash_amount: None,
        };
        assert!(map_dividend_entry(entry).is_none());
    }

    #[test]
    fn test_map_dividend_entry_valid_maps_to_polygon_entry() {
        let entry = DividendsResponseEntry {
            id: Some("1".to_string()),
            ticker: "TICK".to_string(),
            record_date: Some("2020-01-02".to_string()),
            pay_date: Some("2020-01-03".to_string()),
            declaration_date: Some("2020-01-01".to_string()),
            ex_dividend_date: Some("2020-01-04".to_string()),
            frequency: Some(4),
            cash_amount: Some(1.23),
            currency: Some("USD".to_string()),
            distribution_type: Some("type".to_string()),
            historical_adjustment_factor: Some(0.5),
            split_adjusted_cash_amount: Some(0.0),
        };
        let mapped = map_dividend_entry(entry).expect("should map");
        assert_eq!(mapped.ticker, "TICK");
        assert_eq!(mapped.ex_dividend_date.to_string(), "2020-01-04");
        assert_eq!(mapped.cash_amount, 1.23);
        assert_eq!(mapped.currency, "USD");
        assert_eq!(mapped.frequency, Some(4));
    }

    #[test]
    fn test_dividend_entry_with_invalid_date_returns_none() {
        let entry = DividendsResponseEntry {
            id: Some("1".to_string()),
            ticker: "TICK".to_string(),
            record_date: Some("not-a-date".to_string()),
            pay_date: None,
            declaration_date: None,
            ex_dividend_date: Some("invalid-date".to_string()),
            frequency: None,
            cash_amount: Some(2.0),
            currency: Some("EUR".to_string()),
            distribution_type: None,
            historical_adjustment_factor: None,
            split_adjusted_cash_amount: None,
        };
        assert!(map_dividend_entry(entry).is_none());
    }

    #[tokio::test]
    async fn test_loop_with_mock_services_calls_warden_on_empty_response() {
        // start mock server
        let server = MockServer::start_async().await;
        // response with empty results
        let body = serde_json::json!({
            "status": "ok",
            "request_id": "r",
            "results": []
        })
        .to_string();
        let _m = server
            .mock_async(|when, then| {
                when.method(GET);
                then.status(200).body(body.clone());
            })
            .await;

        // Set up mocks with expectations
        let mut polygon_mock = MockPolygonDividendsServiceTrait::new();
        let call_count = RefCell::new(0);
        polygon_mock
            .expect_get_next_issue_symbol_candidate()
            .returning(move |_, _| {
                let mut count = call_count.borrow_mut();
                *count += 1;
                let result = if *count == 1 {
                    Some("SYM".to_string())
                } else {
                    None
                };
                Box::pin(async move { result })
            });

        let mut warden_mock = MockWardenServiceTrait::new();
        warden_mock
            .expect_get_missing_symbols()
            .times(1)
            .return_once(|_| Box::pin(async { Ok(vec![]) }));
        warden_mock
            .expect_add_or_update()
            .times(1)
            .withf(|symbol, _| symbol == "SYM")
            .return_once(|_, _| Box::pin(async { Ok(()) }));

        // key manager with a polygon key so KeyManager returns one
        let km = Arc::new(Mutex::new(crate::api_keys::key_manager::KeyManager::new()));
        {
            let mut k = km.lock().unwrap();
            k.add_key_by_platform(Box::new(PolygonKey::new("secret123".to_string())));
        }

        let client = reqwest::Client::new();
        let url = &format!("{}{}", server.url("/"), "stocks/v1/dividends?");

        // run
        let res = load_and_store_missing_data_with_services(
            &polygon_mock,
            &warden_mock,
            client,
            km.clone(),
            url,
        )
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_loop_with_mock_services_calls_save_all_on_valid_response() {
        let server = MockServer::start_async().await;
        // response with one valid dividend
        let body = serde_json::json!({
            "status": "ok",
            "request_id": "r",
            "results": [
                {
                    "id": "1",
                    "ticker": "SYM",
                    "record_date": "2020-01-01",
                    "pay_date": "2020-01-02",
                    "declaration_date": "2020-01-01",
                    "ex_dividend_date": "2020-01-03",
                    "frequency": 1,
                    "cash_amount": 1.5,
                    "currency": "USD"
                }
            ]
        })
        .to_string();

        let _m = server
            .mock_async(|when, then| {
                when.method(GET);
                then.status(200).body(body.clone());
            })
            .await;

        let mut polygon_mock = MockPolygonDividendsServiceTrait::new();
        let call_count = RefCell::new(0);
        // First call returns SYM, second call returns None to exit loop
        polygon_mock
            .expect_get_next_issue_symbol_candidate()
            .returning(move |_, _| {
                let mut count = call_count.borrow_mut();
                *count += 1;
                let result = if *count == 1 {
                    Some("SYM".to_string())
                } else {
                    None
                };
                Box::pin(async move { result })
            });
        polygon_mock
            .expect_save_all()
            .times(1)
            .withf(|data| data.len() == 1 && data[0].ticker == "SYM" && data[0].cash_amount == 1.5)
            .return_once(|_| Box::pin(async { Ok(()) }));

        let mut warden_mock = MockWardenServiceTrait::new();
        warden_mock
            .expect_get_missing_symbols()
            .times(1)
            .return_once(|_| Box::pin(async { Ok(vec![]) }));

        let km = Arc::new(Mutex::new(crate::api_keys::key_manager::KeyManager::new()));
        {
            let mut k = km.lock().unwrap();
            k.add_key_by_platform(Box::new(PolygonKey::new("secret123".to_string())));
        }

        let client = reqwest::Client::new();
        let url = &format!("{}{}", server.url("/"), "stocks/v1/dividends?");

        let res = load_and_store_missing_data_with_services(
            &polygon_mock,
            &warden_mock,
            client,
            km.clone(),
            url,
        )
        .await;
        assert!(res.is_ok());
    }
}
