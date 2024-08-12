use async_trait::async_trait;
use chrono::{Days, Months, NaiveDate, Utc};
use futures_util::TryFutureExt;
use secrecy::{ExposeSecret, Secret};
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex};

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;
use tracing::{debug, info};

use crate::api_keys::api_key::{ApiKey, ApiKeyPlatform, Status};
use crate::api_keys::key_manager::KeyManager;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Polygon;
const WAIT_FOR_KEY: bool = true;

#[derive(Debug)]
struct PolygonGroupedDailyRequest<'a> {
    base: String,
    api_key: &'a mut Box<dyn ApiKey>,
}

impl PolygonGroupedDailyRequest<'_> {
    fn expose_secret(&mut self) -> String {
        self.base.clone() + self.api_key.get_secret().expose_secret()
    }
}

impl Display for PolygonGroupedDailyRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.base)?;
        Secret::new(self.api_key.expose_secret_for_data_structure().clone()).fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct PolygonGroupedDailyCollector {
    pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
}

impl PolygonGroupedDailyCollector {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(pool: PgPool, client: Client, key_manager: Arc<Mutex<KeyManager>>) -> Self {
        PolygonGroupedDailyCollector {
            pool,
            client,
            key_manager,
        }
    }
}

impl Display for PolygonGroupedDailyCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PolygonGroupedDailyCollector struct.")
    }
}

#[async_trait]
impl Runnable for PolygonGroupedDailyCollector {
    #[tracing::instrument(name = "Run PolygonGroupedDailyCollector", skip_all)]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        // if let Some(key) = &self.api_key {
        load_and_store_missing_data(
            self.pool.clone(),
            self.client.clone(),
            self.key_manager.clone(),
        )
        .map_err(TaskError::UnexpectedError)
        .await?;
        // } else {
        //     return Err(TaskError::UnexpectedError(Error::msg(
        //         "Api key not provided for PolygonGroupedDailyCollector",
        //     )));
        // }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolygonGroupedDaily {
    adjusted: Option<bool>,
    query_count: Option<i64>,
    results: Option<Vec<DailyValue>>,
    results_count: Option<i64>,
    status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DailyValue {
    #[serde(rename = "T")]
    symbol: String,
    #[serde(rename = "c")]
    close: f64,
    #[serde(rename = "h")]
    high: f64,
    #[serde(rename = "l")]
    low: f64,
    #[serde(rename = "n")]
    stock_volume: Option<i64>,
    #[serde(rename = "o")]
    open: f64,
    #[serde(rename = "t")]
    unix_timestamp: i64,
    #[serde(rename = "v")]
    traded_volume: f64,
    #[serde(rename = "vw")]
    volume_weighted_average_price: Option<f64>,
}

#[derive(Default, Deserialize, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TransposedPolygonOpenClose {
    pub close: Vec<f64>,
    pub business_date: Vec<NaiveDate>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub open: Vec<f64>,
    pub symbol: Vec<String>,
    pub stock_volume: Vec<Option<i64>>,
    pub traded_volume: Vec<f64>,
    pub volume_weighted_average_price: Vec<Option<f64>>,
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
    info!("Starting to load Polygon grouped daily.");

    let result = sqlx::query!(
        "select max(business_date) as business_date
        from polygon_grouped_daily"
    )
    .fetch_one(&connection_pool)
    .await?
    .business_date;

    let mut general_api_key =
        KeyManager::get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, PLATFORM).await;
    let mut current_check_date = get_start_date(result);

    while current_check_date.lt(&Utc::now().date_naive()) && general_api_key.is_some() {
        let mut api_key = general_api_key.unwrap();
        let mut request =
            create_polygon_grouped_daily_request(url, &current_check_date, &mut api_key);
        debug!("Polygon grouped daily request: {}", request);
        let response = client
            .get(&request.expose_secret())
            .send()
            .await?
            .text()
            .await?;

        let open_close =
            crate::utils::action_helpers::parse_response::<PolygonGroupedDaily>(&response)?;

        if let Some(results) = open_close.results {
            let open_close = transpose_polygon_grouped_daily(results, current_check_date);

            sqlx::query!(r#"INSERT INTO public.polygon_grouped_daily ("close", business_date, high, low, "open", symbol, stock_volume, traded_volume, volume_weighted_average_price)
                Select * from UNNEST ($1::float[], $2::date[], $3::float[], $4::float[], $5::float[], $6::text[], $7::float[], $8::float[], $9::float[]) on conflict do nothing"#,
                &open_close.close[..],
                &open_close.business_date[..],
                &open_close.high[..],
                &open_close.low[..],
                &open_close.open[..],
                &open_close.symbol[..],
                &open_close.stock_volume[..] as _,
                &open_close.traded_volume[..],
                &open_close.volume_weighted_average_price[..] as _,)
            .execute(&connection_pool).await?;
        }
        if open_close.status != *"ERROR" {
            current_check_date = current_check_date
                .checked_add_days(Days::new(1))
                .expect("Adding one day must always work, given the operating date context.");
            // sleep(time::Duration::from_secs(13)).await;
        } else {
            info!(
                "Failed with request {} and got response {}",
                request, response
            );
            api_key.set_status(Status::Exhausted);
        }

        general_api_key = KeyManager::exchange_apikey_or_wait_if_non_ready(
            key_manager.clone(),
            WAIT_FOR_KEY,
            api_key,
            PLATFORM,
        )
        .await;
    }
    if let Some(api_key) = general_api_key {
        let mut d = key_manager.lock().expect("msg");
        d.add_key_by_platform(api_key);
    }
    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
fn get_start_date(result: Option<NaiveDate>) -> NaiveDate {
    if let Some(date) = result {
        return date
            .checked_add_days(Days::new(1))
            .expect("Adding one day must always work, given the operating date context.");
    }
    earliest_date()
}

#[tracing::instrument(level = "debug", skip_all)]
fn transpose_polygon_grouped_daily(
    instruments: Vec<DailyValue>,
    business_date: NaiveDate,
) -> TransposedPolygonOpenClose {
    let mut result = TransposedPolygonOpenClose {
        close: vec![],
        business_date: vec![],
        high: vec![],
        low: vec![],
        open: vec![],
        symbol: vec![],
        traded_volume: vec![],
        volume_weighted_average_price: vec![],
        stock_volume: vec![],
    };

    for data in instruments {
        result.close.push(data.close);
        result.business_date.push(business_date);
        result.high.push(data.high);
        result.low.push(data.low);
        result.open.push(data.open);
        result.symbol.push(data.symbol);
        result.traded_volume.push(data.traded_volume);
        result
            .volume_weighted_average_price
            .push(data.volume_weighted_average_price);
        result.stock_volume.push(data.stock_volume)
    }
    result
}

#[tracing::instrument(level = "debug", skip_all)]
fn earliest_date() -> NaiveDate {
    Utc::now()
        .date_naive()
        .checked_sub_months(Months::new(24))
        .expect("Minus 2 years should never fail")
        .checked_add_days(Days::new(1))
        .expect("Adding 1 day should always work")
}

///  Example output https://api.polygon.io/v1/open-close/AAPL/2023-01-09?adjusted=true&apiKey=PutYourKeyHere
#[tracing::instrument(level = "debug", skip_all)]
fn create_polygon_grouped_daily_request<'a>(
    base_url: &'a str,
    date: &'a NaiveDate,
    api_key: &'a mut Box<dyn ApiKey>,
) -> PolygonGroupedDailyRequest<'a> {
    let base_request_url =
        base_url.to_string() + date.to_string().as_str() + "?adjusted=true" + "&apiKey=";
    PolygonGroupedDailyRequest {
        base: base_request_url,
        api_key,
    }
}

#[cfg(test)]
mod test {}
