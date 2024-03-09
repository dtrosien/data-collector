use chrono::{Days, Months, NaiveDate, Utc};
use std::fmt::Display;
use std::{thread, time};

use crate::{collectors::utils, tasks::runnable::Runnable};

use async_trait::async_trait;
use futures_util::TryFutureExt;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;
use tracing::info;

use crate::collectors::collector::Collector;
use crate::collectors::{collector_sources, sp500_fields};
use crate::tasks::task::TaskError;

const URL: &str = "https://api.polygon.io/v1/open-close";

#[derive(Clone)]
pub struct PolygonGroupedDailyCollector {
    pool: PgPool,
    client: Client,
    api_key: String,
}

impl PolygonGroupedDailyCollector {
    pub fn new(pool: PgPool, client: Client, api_key: String) -> Self {
        PolygonGroupedDailyCollector {
            pool,
            client,
            api_key,
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
    async fn run(&self) -> Result<(), TaskError> {
        load_and_store_missing_data(self.pool.clone(), self.client.clone(), &self.api_key)
            .map_err(TaskError::UnexpectedError)
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolygonGroupedDaily {
    adjusted: bool,
    query_count: i64,
    results: Option<Vec<DailyValue>>,
    results_count: i64,
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
    _url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load Polygon grouped daily.");

    let result =
        sqlx::query!("select max(business_date) as business_date  from polygon_grouped_daily")
            .fetch_one(&connection_pool)
            .await?
            .business_date;

    let mut current_check_date = get_start_date(result);

    while current_check_date.lt(&Utc::now().date_naive()) {
        let request = create_polygon_open_close_request(current_check_date, api_key);
        println!("my request: {}", request);
        let response = client
            .get(request)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let open_close = utils::parse_response::<PolygonGroupedDaily>(&response).unwrap();

        if open_close.results.is_some() {
            let open_close =
                transpose_polygon_grouped_daily(open_close.results.unwrap(), current_check_date);

            // INSERT INTO public.polygon_grouped_daily ("close", business_date, high, low, "open", symbol, stock_volume, traded_volume, volume_weighted_average_price) VALUES(0, '', 0, 0, 0, '', 0, 0, 0);
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
                &open_close.volume_weighted_average_price[..] as _,
        ).execute(&connection_pool).await.unwrap();
        }
        current_check_date = current_check_date.checked_add_days(Days::new(1)).unwrap();
        let thirteen_secs = time::Duration::from_secs(13);
        thread::sleep(thirteen_secs);
    }

    Ok(())
}

fn get_start_date(result: Option<NaiveDate>) -> NaiveDate {
    if result.is_some() {
        return result.unwrap().checked_add_days(Days::new(1)).unwrap();
    }
    earliest_date()
}

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

fn earliest_date() -> NaiveDate {
    Utc::now()
        .date_naive()
        .checked_sub_months(Months::new(24))
        .expect("Minus 2 years should never fail")
        .checked_add_days(Days::new(1))
        .expect("Adding 1 day should always work")
}

// fn filter_for_valid_datasets(input: Vec<NyseInstrument>) -> Vec<NyseInstrument> {
//     input
//         .into_iter()
//         .filter(|instrument| instrument.instrument_name.is_some())
//         .collect()
// }

impl Collector for PolygonGroupedDailyCollector {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        vec![
            sp500_fields::Fields::OpenClose,
            sp500_fields::Fields::MonthTradingVolume,
        ]
    }

    fn get_source(&self) -> collector_sources::CollectorSource {
        collector_sources::CollectorSource::PolygonGroupedDaily
    }
}

// async fn get_amount_instruments_available(
//     client: &Client,
//     url: &str,
// ) -> Result<u32, anyhow::Error> {
//     let response = client
//         .post(url)
//         .header("content-type", "application/json")
//         .body(create_nyse_instruments_request(1, 1))
//         .send()
//         .await?
//         .text()
//         .await?;
//     let response = utils::parse_response::<Vec<NysePeekResponse>>(&response)?;

//     match response.first() {
//         Some(some) => Ok(some.total),
//         None => Err(anyhow!(
//             "Error while receiving amount of NYSE instruments. Option was None."
//         )),
//     }
// }

// https://api.polygon.io/v1/open-close/AAPL/2023-01-09?adjusted=true&apiKey=PutYourKeyHere
fn create_polygon_open_close_request(date: NaiveDate, api_key: &str) -> String {
    let request_url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/".to_string()
        + date.to_string().as_str()
        + "?adjusted=true"
        + "&apiKey="
        + api_key;
    request_url
}

#[cfg(test)]
mod test {
    //     use chrono::Utc;
    //     use httpmock::{Method::POST, MockServer};

    //     use crate::collectors::{source_apis::nyse_instruments::NysePeekResponse, utils};
    //     use crate::utils::test_helpers::get_test_client;
    //     use sqlx::{Pool, Postgres};
    //     use tracing_test::traced_test;

    //     use super::*;

    use chrono::NaiveDate;

    use crate::collectors::{
        source_apis::{
            polygon_grouped_daily::PolygonGroupedDaily, polygon_open_close::PolygonOpenClose,
        },
        utils,
    };

    // #[test]
    // fn parse_nyse_instruments_response_with_one_result() {
    //     // let input_json = r#"{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}"#;
    //     let input_json = r#"{
    //         "afterHours": 183.95,
    //         "close": 184.37,
    //         "from": "2024-02-22",
    //         "status": "OK",
    //         "symbol": "AAPL",
    //         "open": 183.48,
    //         "high": 184.955,
    //         "low": 182.46,
    //         "volume": 5.2284192e+07,
    //         "preMarket": 183.8
    //     }"#;
    //     let parsed = utils::parse_response::<PolygonOpenClose>(input_json).unwrap();
    //     let instrument = PolygonGroupedDaily {
    //         after_hours: Some(183.95),
    //         close: Some(184.37),
    //         business_date: Some(
    //             NaiveDate::parse_from_str("2024-02-22", "%Y-%m-%d").expect("Parsing constant."),
    //         ),
    //         high: Some(184.955),
    //         low: Some(182.46),
    //         open: Some(183.48),
    //         status: "OK".to_string(),
    //         pre_market: Some(183.8),
    //         symbol: Some("AAPL".to_string()),
    //         volume: Some(52284192.0),
    //         message: None,
    //     };
    //     assert_eq!(parsed, instrument);
    // }

    //     #[test]
    //     fn parse_nyse_instruments_response_with_one_result() {
    //         let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
    //         let parsed = utils::parse_response::<Vec<NyseInstrument>>(input_json).unwrap();
    //         let instrument = NyseInstrument {
    //             instrument_type: "COMMON_STOCK".to_string(),
    //             symbol_ticker: "A".to_string(),
    //             symbol_exchange_ticker: "A".to_string(),
    //             normalized_ticker: "A".to_string(),
    //             symbol_esignal_ticker: "A".to_string(),
    //             instrument_name: Some("AGILENT TECHNOLOGIES INC".to_string()),
    //             mic_code: "XNYS".to_string(),
    //         };
    //         assert_eq!(parsed[0], instrument);
    //     }

    //     #[test]
    //     fn parse_nyse_instruments_peek_response_with_one_result() {
    //         let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
    //         let parsed = utils::parse_response::<Vec<NysePeekResponse>>(input_json).unwrap();
    //         let instrument = NysePeekResponse { total: 13202 };
    //         assert_eq!(parsed[0], instrument);
    //     }

    //     #[test]
    //     fn create_request_statement() {
    //         let expected = r#"{"filterToken":"","maxResultsPerPage":2,"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC"}"#;
    //         let build = create_nyse_instruments_request(1, 1);
    //         assert_eq!(expected, build);
    //     }

    //     #[traced_test]
    //     #[sqlx::test]
    //     async fn query_http_and_write_to_db(pool: Pool<Postgres>) -> Result<(), anyhow::Error> {
    //         // Start a lightweight mock server.
    //         let server = MockServer::start();
    //         let url = server.base_url();
    //         let request_json = r#"{"filterToken":"","maxResultsPerPage":2,"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC"}"#;
    //         let response_json = r#"[{"total":1,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"B","normalizedTicker":"C","symbolEsignalTicker":"D","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;

    //         server.mock(|when, then| {
    //             when.method(POST)
    //                 .header("content-type", "application/json")
    //                 .body(request_json);
    //             then.status(200)
    //                 .header("content-type", "application/json")
    //                 .body(response_json);
    //         });

    //         let client = get_test_client();

    //         load_and_store_missing_data_given_url(pool.clone(), client, &url).await?;

    //         let saved = sqlx::query!("SELECT instrument_name, instrument_type, symbol_ticker, symbol_exchange_ticker, normalized_ticker, symbol_esignal_ticker, mic_code, dateloaded, is_staged FROM public.nyse_instruments;").fetch_one(&pool).await?;
    //         assert_eq!(saved.instrument_name, "AGILENT TECHNOLOGIES INC");
    //         assert_eq!(saved.instrument_type, "COMMON_STOCK");
    //         assert_eq!(saved.symbol_ticker, "A");
    //         assert_eq!(saved.symbol_exchange_ticker.unwrap(), "B");
    //         assert_eq!(saved.normalized_ticker.unwrap(), "C");
    //         assert_eq!(saved.symbol_esignal_ticker.unwrap(), "D");
    //         assert_eq!(saved.mic_code, "XNYS");
    //         assert_eq!(saved.dateloaded.unwrap(), Utc::now().date_naive());
    //         assert_eq!(saved.is_staged, false);
    //         Ok(())
    //     }
}
