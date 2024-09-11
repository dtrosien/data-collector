use crate::{
    api_keys::{
        api_key::{ApiKey, ApiKeyPlatform, Status},
        key_manager::KeyManager,
    },
    utils::action_helpers::parse_response,
};
use async_trait::async_trait;
use chrono::{Days, Months, NaiveDate, TimeDelta, Utc};
use futures_util::TryFutureExt;
use secrecy::{ExposeSecret, Secret};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};
use tracing::{debug, info};

use std::fmt::Display;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;

use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://api.polygon.io/v1/open-close/";
const ERROR_MSG_VALUE_EXISTS: &str = "Value exists or error must have been caught before";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Polygon;
const WAIT_FOR_KEY: bool = true;
const IDLE_SYMBOL_TIMEOUT: i64 = 30; // Timeout in days

#[derive(Debug)]
struct PolygonOpenCloseRequest<'a> {
    base: String,
    api_key: &'a mut Box<dyn ApiKey>,
}

impl PolygonOpenCloseRequest<'_> {
    fn expose_secret(&mut self) -> String {
        self.base.clone() + self.api_key.get_secret().expose_secret()
    }
}

impl Display for PolygonOpenCloseRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.base)?;
        Secret::new(self.api_key.expose_secret_for_data_structure().clone()).fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct PolygonOpenCloseCollector {
    pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
}

impl PolygonOpenCloseCollector {
    #[tracing::instrument(name = "Run Polygon open close collector", skip_all)]
    pub fn new(pool: PgPool, client: Client, key_manager: Arc<Mutex<KeyManager>>) -> Self {
        PolygonOpenCloseCollector {
            pool,
            client,
            key_manager,
        }
    }
}

impl Display for PolygonOpenCloseCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PolygonOpenCloseCollector struct.")
    }
}

#[async_trait]
impl Runnable for PolygonOpenCloseCollector {
    #[tracing::instrument(level = "debug", skip_all)]
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
        //         "Api key not provided for PolygonOpenCloseCollector",
        //     )));
        // }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolygonOpenClose {
    #[serde(alias = "from")]
    business_date: Option<NaiveDate>,
    after_hours: Option<f64>,
    close: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    open: Option<f64>,
    status: String,
    pre_market: Option<f64>,
    symbol: Option<String>,
    volume: Option<f64>,
    message: Option<String>,
}

#[derive(Default, Deserialize, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TransposedPolygonOpenClose {
    pub after_hours: Vec<Option<f64>>,
    pub close: Vec<f64>,
    pub business_date: Vec<NaiveDate>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub open: Vec<f64>,
    pub pre_market: Vec<Option<f64>>,
    pub symbol: Vec<String>,
    pub volume: Vec<f64>,
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
    info!("Starting to load Polygon open close.");

    let mut issue_symbol_candidate: Option<String> =
        get_next_issue_symbol_candidate(&connection_pool, None).await;
    let mut general_api_key =
        KeyManager::get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, PLATFORM).await;
    while let (Some(issue_symbol), true) = (issue_symbol_candidate, general_api_key.is_some()) {
        let mut current_check_date = earliest_date(&issue_symbol, &connection_pool).await;

        while let Some(mut api_key) =
            general_api_key.take_if(|_| current_check_date.lt(&Utc::now().date_naive()))
        {
            let mut request = create_polygon_open_close_request(
                url,
                &issue_symbol,
                current_check_date,
                &mut api_key,
            );
            debug!("Polygon open close request: {}", request);
            let response = client
                .get(request.expose_secret())
                .send()
                .await?
                .text()
                .await?;
            let open_close = vec![parse_response::<PolygonOpenClose>(&response)?];
            if open_close[0].status.eq("OK") {
                let open_close_data = transpose_polygon_open_close(&open_close);
                sqlx::query!(r#"INSERT INTO polygon_open_close
                (after_hours, "close", business_date, high, low, "open", pre_market, symbol, volume)
                Select * from UNNEST ($1::float[], $2::float[], $3::date[], $4::float[], $5::float[], $6::float[], $7::float[], $8::text[], $9::float[]) on conflict do nothing"#,
                &open_close_data.after_hours[..] as _,
                &open_close_data.close[..],
                &open_close_data.business_date[..],
                &open_close_data.high[..],
                &open_close_data.low[..],
                &open_close_data.open[..],
                &open_close_data.pre_market[..] as _,
                &open_close_data.symbol[..],
                &open_close_data.volume[..],)
                .execute(&connection_pool)
                .await?;
            }
            if open_close[0].status.ne("ERROR") {
                current_check_date = current_check_date
                    .checked_add_days(Days::new(1))
                    .expect("Adding one day must always work, given the operating date context.");
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
        // Mark symbols without new date as not available
        if Utc::now().date_naive() - earliest_date(&issue_symbol, &connection_pool).await
            > TimeDelta::days(IDLE_SYMBOL_TIMEOUT)
        {
            add_missing_issue_symbol(&issue_symbol, &connection_pool).await?;
        }

        issue_symbol_candidate =
            get_next_issue_symbol_candidate(&connection_pool, Some(issue_symbol)).await;
    }
    if let Some(api_key) = general_api_key {
        let mut d = key_manager.lock().expect("msg");
        d.add_key_by_platform(api_key);
    }
    info!("Finished loading Polygon open close.");
    Ok(())
}

async fn add_missing_issue_symbol(
    issue_symbol: &str,
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r#"INSERT INTO source_symbol_warden (issue_symbol, polygon)
      VALUES($1, false)
      ON CONFLICT(issue_symbol)
      DO UPDATE SET
        polygon = false"#,
        issue_symbol
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

// Check first if a symbol can be updated and then if totally undocumented symbols are available
async fn get_next_issue_symbol_candidate(
    connection_pool: &sqlx::Pool<sqlx::Postgres>,
    lower_symbol_bound: Option<String>,
) -> Option<String> {
    let lower_symbol_bound = match lower_symbol_bound {
        Some(s) => s,
        None => "".to_string(),
    };
    let a = sqlx::query!(
        "SELECT symbol
        FROM polygon_open_close poc 
        GROUP BY symbol
        HAVING MAX(business_date) < (CURRENT_DATE - INTERVAL '7 days') 
          AND symbol > $1::text
          AND symbol not in (select issue_symbol from source_symbol_warden ssw where polygon = false)
        order by symbol limit 1",
        lower_symbol_bound
    )
    .fetch_one(connection_pool)
    .await;
    if let Ok(issue_symbol_candidate) = a {
        return Some(issue_symbol_candidate.symbol);
    }

    let issue_symbol_candidate = sqlx::query!(
        "select issue_symbol 
        from master_data_eligible mde 
        where issue_symbol not in 
          (select distinct(symbol) 
           from polygon_open_close poc)
           and issue_symbol > $1::text 
           and issue_symbol not in (select issue_symbol from source_symbol_warden ssw where polygon = false)
        order by issue_symbol
        limit 1",
        lower_symbol_bound
    )
    .fetch_one(connection_pool)
    .await;
    if let Ok(issue_symbol) = issue_symbol_candidate {
        return issue_symbol.issue_symbol;
    }
    None
}

#[tracing::instrument(level = "debug", skip_all)]
fn transpose_polygon_open_close(instruments: &Vec<PolygonOpenClose>) -> TransposedPolygonOpenClose {
    let mut result = TransposedPolygonOpenClose {
        after_hours: vec![],
        close: vec![],
        business_date: vec![],
        high: vec![],
        low: vec![],
        open: vec![],
        pre_market: vec![],
        symbol: vec![],
        volume: vec![],
    };
    for data in instruments {
        result.after_hours.push(data.after_hours);
        result.close.push(data.close.expect(ERROR_MSG_VALUE_EXISTS));
        result
            .business_date
            .push(data.business_date.expect(ERROR_MSG_VALUE_EXISTS));
        result.high.push(data.high.expect(ERROR_MSG_VALUE_EXISTS));
        result.low.push(data.low.expect(ERROR_MSG_VALUE_EXISTS));
        result.open.push(data.open.expect(ERROR_MSG_VALUE_EXISTS));
        result.pre_market.push(data.pre_market);
        result.symbol.push(
            data.symbol
                .as_ref()
                .expect(ERROR_MSG_VALUE_EXISTS)
                .to_string(),
        );
        result
            .volume
            .push(data.volume.expect(ERROR_MSG_VALUE_EXISTS));
    }
    result
}

#[tracing::instrument(level = "debug", skip_all)]
async fn earliest_date(
    issue_symbol: &String,
    connection_pool: &sqlx::Pool<sqlx::Postgres>,
) -> NaiveDate {
    let a = sqlx::query!(
        "select MAX(business_date) as max_date from polygon_open_close pgd where symbol = $1::text",
        issue_symbol
    )
    .fetch_one(connection_pool)
    .await;
    if let Ok(date) = a {
        return date.max_date.expect("Check already happended before.");
    }
    Utc::now()
        .date_naive()
        .checked_sub_months(Months::new(24))
        .expect("Minus 2 years should never fail")
        .checked_add_days(Days::new(1))
        .expect("Adding 1 day should always work")
}

#[tracing::instrument(level = "debug", skip_all)]
fn create_polygon_open_close_request<'a>(
    base_url: &'a str,
    ticker_symbol: &'a str,
    date: NaiveDate,
    api_key: &'a mut Box<dyn ApiKey>,
) -> PolygonOpenCloseRequest<'a> {
    let base_request_url = base_url.to_string()
        + ticker_symbol
        + "/"
        + date.to_string().as_str()
        + "?adjusted=true"
        + "&apiKey=";
    PolygonOpenCloseRequest {
        base: base_request_url,
        api_key,
    }
}

#[cfg(test)]
mod test {

    use crate::actions::collect::polygon_open_close::PolygonOpenClose;
    use chrono::NaiveDate;

    #[test]
    fn parse_polygon_open_close_response_with_one_result() {
        let input_json = r#"{
            "afterHours": 183.95,
            "close": 184.37,
            "from": "2024-02-22",
            "status": "OK",
            "symbol": "AAPL",
            "open": 183.48,
            "high": 184.955,
            "low": 182.46,
            "volume": 5.2284192e+07,
            "preMarket": 183.8
        }"#;
        let parsed =
            crate::utils::action_helpers::parse_response::<PolygonOpenClose>(input_json).unwrap();
        let instrument = PolygonOpenClose {
            after_hours: Some(183.95),
            close: Some(184.37),
            business_date: Some(
                NaiveDate::parse_from_str("2024-02-22", "%Y-%m-%d").expect("Parsing constant."),
            ),
            high: Some(184.955),
            low: Some(182.46),
            open: Some(183.48),
            status: "OK".to_string(),
            pre_market: Some(183.8),
            symbol: Some("AAPL".to_string()),
            volume: Some(52284192.0),
            message: None,
        };
        assert_eq!(parsed, instrument);
    }
}
