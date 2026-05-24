use crate::{
    api_keys::{
        api_key::{ApiKey, ApiKeyPlatform},
        key_manager::KeyManager,
    },
    database::polygon_dividends_service::{PolygonDividendsEntry, PolygonDividendsService},
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
            let dividends_response_entries: Vec<_> = dividends
                .into_iter()
                .filter_map(|div| {
                    if div.ex_dividend_date.is_none()
                        || div.cash_amount.is_none()
                        || div.currency.is_none()
                    {
                        return None;
                    }

                    Some(PolygonDividendsEntry {
                        ticker: div.ticker,
                        record_date: convert_string_to_naive_date(div.record_date),
                        pay_date: convert_string_to_naive_date(div.pay_date),
                        declaration_date: convert_string_to_naive_date(div.declaration_date),
                        ex_dividend_date: NaiveDate::parse_from_str(
                            &div.ex_dividend_date.expect("Checked before."),
                            "%Y-%m-%d",
                        )
                        .expect("Check happened above"),
                        frequency: div.frequency,
                        cash_amount: div.cash_amount.expect("Checked before"),
                        currency: div.currency.expect("Checked before"),
                        distribution_type: div.distribution_type,
                        historical_adjustment_factor: div.historical_adjustment_factor,
                        split_adjusted_cash_amount: div.split_adjusted_cash_amount,
                        is_staged: false,
                    })
                })
                .collect();
            polygon_dividends_service
                .save_all(dividends_response_entries)
                .await?;
        }

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
