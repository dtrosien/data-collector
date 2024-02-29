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
    async fn run(&self) -> Result<(), TaskError> {
        load_and_store_missing_data(self.pool.clone(), self.client.clone(), &self.api_key)
            .map_err(TaskError::UnexpectedError)
            .await
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
    pub after_hours: Vec<f64>,
    pub close: Vec<f64>,
    pub business_date: Vec<NaiveDate>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub open: Vec<f64>,
    pub pre_market: Vec<f64>,
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
    _url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load Polygon open close.");

    let mut result  = sqlx::query!("select issue_symbol from master_data_eligible mde where issue_symbol not in (select distinct(symbol) from polygon_open_close poc ) order by issue_symbol").fetch_one(&connection_pool).await?.issue_symbol;
    while result.is_some() {
        let mut current_check_date = earliest_date();
        let test = result.clone().unwrap();
        while current_check_date.lt(&Utc::now().date_naive()) {
            let request = create_polygon_open_close_request(&test, current_check_date, api_key);
            println!("my request: {}", request);
            let response = client
                .get(request)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            let open_close = vec![utils::parse_response::<PolygonOpenClose>(&response).unwrap()];
            if open_close[0].status.eq("OK") {
                let open_close = transpose_polygon_open_close(open_close);
                // INSERT INTO polygon_open_close (after_hours, "close", business_date, high, low, "open", pre_market, symbol, volume) VALUES(0, 0, '', 0, 0, 0, 0, '', 0);
                sqlx::query!(r#"INSERT INTO polygon_open_close
                (after_hours, "close", business_date, high, low, "open", pre_market, symbol, volume)
                Select * from UNNEST ($1::float[], $2::float[], $3::date[], $4::float[], $5::float[], $6::float[], $7::float[], $8::text[], $9::float[]) on conflict do nothing"#,
                &open_close.after_hours[..],
                &open_close.close[..],
                &open_close.business_date[..],
                &open_close.high[..],
                &open_close.low[..],
                &open_close.open[..],
                &open_close.pre_market[..],
                &open_close.symbol[..],
                &open_close.volume[..],
        ).execute(&connection_pool).await?;
            }
            current_check_date = current_check_date.checked_add_days(Days::new(1)).unwrap();
            let thirteen_secs = time::Duration::from_secs(13);
            thread::sleep(thirteen_secs);
        }
        result  = sqlx::query!("select issue_symbol from master_data_eligible mde where issue_symbol not in (select distinct(symbol) from polygon_open_close poc ) order by issue_symbol").fetch_one(&connection_pool).await?.issue_symbol;
    }
    Ok(())
}

fn transpose_polygon_open_close(instruments: Vec<PolygonOpenClose>) -> TransposedPolygonOpenClose {
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

    //my request: https://api.polygon.io/v1/open-close/AACI/2022-06-06?adjusted=true&apiKey=iMX3MwHNhuj_RAHRG1zkor2o4WyZqp2U
    // thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value', src/collectors/source_apis/polygon_open_close.rs:152:50
    for data in instruments {
        result.after_hours.push(data.after_hours.unwrap());
        result.close.push(data.close.unwrap());
        result.business_date.push(data.business_date.unwrap());
        result.high.push(data.high.unwrap());
        result.low.push(data.low.unwrap());
        result.open.push(data.open.unwrap());
        result.pre_market.push(data.pre_market.unwrap());
        result.symbol.push(data.symbol.unwrap());
        result.volume.push(data.volume.unwrap());
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

impl Collector for PolygonOpenCloseCollector {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        vec![
            sp500_fields::Fields::OpenClose,
            sp500_fields::Fields::MonthTradingVolume,
        ]
    }

    fn get_source(&self) -> collector_sources::CollectorSource {
        collector_sources::CollectorSource::PolygonOpenClose
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
fn create_polygon_open_close_request(
    ticker_symbol: &str,
    date: NaiveDate,
    api_key: &str,
) -> String {
    // https://api.polygon.io/v1
    // open-close
    let request_url = "https://api.polygon.io/v1/open-close/".to_string()
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
    //     use chrono::Utc;
    //     use httpmock::{Method::POST, MockServer};

    //     use crate::collectors::{source_apis::nyse_instruments::NysePeekResponse, utils};
    //     use crate::utils::test_helpers::get_test_client;
    //     use sqlx::{Pool, Postgres};
    //     use tracing_test::traced_test;

    //     use super::*;

    use chrono::NaiveDate;

    use crate::collectors::{source_apis::polygon_open_close::PolygonOpenClose, utils};

    #[test]
    fn parse_nyse_instruments_response_with_one_result() {
        // let input_json = r#"{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}"#;
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
        let parsed = utils::parse_response::<PolygonOpenClose>(input_json).unwrap();
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
