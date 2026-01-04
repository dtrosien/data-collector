use crate::{
    api_keys::{
        api_key::{self, ApiKey, ApiKeyPlatform},
        key_manager::KeyManager,
    },
    database::polygon_dividends_service::{self, PolygonDividendsService},
    utils::action_helpers::parse_response,
};
use async_trait::async_trait;
use chrono::NaiveDate;
use futures_util::TryFutureExt;
use secrecy::{ExposeSecret, Secret};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};
use tracing::debug;

use std::fmt::Display;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;

use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://api.massive.com/stocks/v1/dividends?";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Polygon;
const WAIT_FOR_KEY: bool = true;
const IDLE_SYMBOL_TIMEOUT: i64 = 30; // Timeout in days

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
async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
    url: &str,
) -> Result<(), anyhow::Error> {
    let polygon_dividends_service = PolygonDividendsService::new(connection_pool.clone());
    let mut issue_symbol_candidate = polygon_dividends_service
        .get_next_issue_symbol_candidate("".to_string())
        .await;
    let mut general_api_key =
        KeyManager::get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, PLATFORM).await;
    while let (Some(issue_symbol), true) = (issue_symbol_candidate, general_api_key.is_some()) {
        let mut api_key = general_api_key.unwrap();
        let mut request = create_polygon_dividends_request(url, &issue_symbol, &mut api_key);
        debug!("Polygon dividends request: {}", request);
        let response = client
            .get(request.expose_secret())
            .send()
            .await?
            .text()
            .await?;
        let dividends =
            crate::utils::action_helpers::parse_response::<Dividends>(&response)?.results;

        // TODO: Continue here. Parse result from request and store in database

        // Refresh iteration objects
        issue_symbol_candidate = polygon_dividends_service
            .get_next_issue_symbol_candidate(issue_symbol)
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
fn create_polygon_dividends_request<'a>(
    base_url: &'a str,
    ticker_symbol: &'a str,
    api_key: &'a mut Box<dyn ApiKey>,
) -> PolygonDividendsRequest<'a> {
    let base_request_url = base_url.to_string() + ticker_symbol + "&apiKey=";
    PolygonDividendsRequest {
        base: base_request_url,
        api_key,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dividends {
    status: Option<String>,
    request_id: Option<String>,
    results: Option<Vec<DividendsEntry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DividendsEntry {
    id: Option<String>,
    ticker: String,
    record_date: Option<String>,
    pay_date: Option<String>,
    declaration_date: Option<String>,
    ex_dividend_date: Option<String>,
    frequency: Option<i64>,
    cash_amount: Option<f64>,
    currency: Option<String>,
    distribution_type: Option<String>,
    historical_adjustment_factor: Option<f64>,
    split_adjusted_cash_amount: Option<f64>,
}
