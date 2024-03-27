use async_trait::async_trait;
use chrono::{Days, Months, NaiveDate, Utc};
use futures_util::TryFutureExt;
use std::fmt::Display;
use std::time;
use tokio::time::sleep;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;
use tracing::{debug, info};

use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://api.polygon.io/v1/open-close/";
const ERROR_MSG_VALUE_EXISTS: &str = "Value exists or error must have been caught before";

#[derive(Clone, Debug)]
pub struct PolygonOpenCloseCollector {
    pool: PgPool,
    client: Client,
    api_key: String,
}

impl PolygonOpenCloseCollector {
    pub fn new(pool: PgPool, client: Client, api_key: String) -> Self {
        PolygonOpenCloseCollector {
            pool,
            client,
            api_key,
        }
    }
}

impl Display for PolygonOpenCloseCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PloygonOpenCloseCollector struct.")
    }
}

#[async_trait]
impl Runnable for PolygonOpenCloseCollector {
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        load_and_store_missing_data(self.pool.clone(), self.client.clone(), &self.api_key)
            .map_err(TaskError::UnexpectedError)
            .await?;
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

pub async fn load_and_store_missing_data(
    connection_pool: PgPool,
    client: Client,
    api_key: &str,
) -> Result<(), anyhow::Error> {
    load_and_store_missing_data_given_url(connection_pool, client, api_key, URL).await
}

async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    api_key: &str,
    url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load Polygon open close.");

    let mut issue_symbol_candidate: Option<String> = sqlx::query!(
        "select issue_symbol
        from master_data_eligible mde
        where issue_symbol not in 
          (select distinct(symbol) 
           from polygon_open_close poc)
        order by issue_symbol"
    )
    .fetch_one(&connection_pool)
    .await?
    .issue_symbol;
    while let Some(issue_symbol) = issue_symbol_candidate {
        let mut current_check_date = earliest_date();

        while current_check_date.lt(&Utc::now().date_naive()) {
            let request =
                create_polygon_open_close_request(url, &issue_symbol, current_check_date, api_key);
            debug!("Polygon open close request: {}", request);
            let response = client.get(request).send().await?.text().await?;
            let open_close = vec![crate::utils::action_helpers::parse_response::<
                PolygonOpenClose,
            >(&response)?];
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
            }
            sleep(time::Duration::from_secs(13)).await;
        }
        issue_symbol_candidate = sqlx::query!(
            "select issue_symbol 
            from master_data_eligible mde 
            where issue_symbol not in 
              (select distinct(symbol) 
               from polygon_open_close poc) 
            order by issue_symbol"
        )
        .fetch_one(&connection_pool)
        .await?
        .issue_symbol;
    }
    Ok(())
}

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
    println!("Data: {:?}", instruments);
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

fn earliest_date() -> NaiveDate {
    Utc::now()
        .date_naive()
        .checked_sub_months(Months::new(24))
        .expect("Minus 2 years should never fail")
        .checked_add_days(Days::new(1))
        .expect("Adding 1 day should always work")
}

// impl Collector for PolygonOpenCloseCollector {
//     fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
//         vec![
//             sp500_fields::Fields::OpenClose,
//             sp500_fields::Fields::MonthTradingVolume,
//         ]
//     }

//     fn get_source(&self) -> collector_sources::CollectorSource {
//         collector_sources::CollectorSource::PolygonOpenClose
//     }
// }

fn create_polygon_open_close_request(
    base_url: &str,
    ticker_symbol: &str,
    date: NaiveDate,
    api_key: &str,
) -> String {
    let request_url = base_url.to_string()
        + ticker_symbol
        + "/"
        + date.to_string().as_str()
        + "?adjusted=true"
        + "&apiKey="
        + api_key;
    request_url
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
