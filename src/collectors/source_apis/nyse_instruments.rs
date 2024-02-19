use anyhow::anyhow;
use std::fmt::Display;

use crate::collectors::utils;

use async_trait::async_trait;
use futures_util::TryFutureExt;

use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use tracing::info;

use crate::collectors::collector::Collector;
use crate::collectors::{collector_sources, sp500_fields};
use crate::dag_scheduler::task::TaskError::UnexpectedError;
use crate::dag_scheduler::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://www.nyse.com/api/quotes/filter";

#[derive(Clone)]
pub struct NyseInstrumentCollector {
    pool: PgPool,
    client: Client,
}

impl NyseInstrumentCollector {
    pub fn new(pool: PgPool, client: Client) -> Self {
        NyseInstrumentCollector { pool, client }
    }
}

impl Display for NyseInstrumentCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NyseInstrumentCollector struct.")
    }
}

#[async_trait]
impl Runnable for NyseInstrumentCollector {
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        load_and_store_missing_data(self.pool.clone(), self.client.clone())
            .map_err(UnexpectedError)
            .await?;
        Ok(None)
    }
}

#[derive(Default, Deserialize, Debug, PartialEq)]
struct NysePeekResponse {
    pub total: u32,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
struct NyseInstruments {
    pub instruments: Vec<NyseInstrument>,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct NyseInstrument {
    pub instrument_type: String,
    pub symbol_ticker: String,
    pub symbol_exchange_ticker: String,
    pub normalized_ticker: String,
    pub symbol_esignal_ticker: String,
    pub instrument_name: Option<String>,
    pub mic_code: String,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TransposedNyseInstrument {
    pub instrument_type: Vec<String>,
    pub symbol_ticker: Vec<String>,
    pub symbol_exchange_ticker: Vec<String>,
    pub normalized_ticker: Vec<String>,
    pub symbol_esignal_ticker: Vec<String>,
    pub instrument_name: Vec<String>,
    pub mic_code: Vec<String>,
}

pub async fn load_and_store_missing_data(
    connection_pool: PgPool,
    client: Client,
) -> Result<(), anyhow::Error> {
    load_and_store_missing_data_given_url(connection_pool, client, URL).await
}

async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load NYSE instruments");
    let page_size = get_amount_instruments_available(&client, url).await?;

    let list_of_pages: Vec<u32> = (1..=1).collect();
    for page in list_of_pages {
        let request = create_nyse_instruments_request(page, page_size);
        let response = client
            .post(url)
            .header("content-type", "application/json")
            .body(request)
            .send()
            .await?
            .text()
            .await?;
        let instruments = utils::parse_response::<Vec<NyseInstrument>>(&response)?;
        let instruments = transpose_nyse_instruments(instruments);
        sqlx::query!("INSERT INTO nyse_instruments 
        (instrument_name, instrument_type, symbol_ticker, symbol_exchange_ticker, normalized_ticker, symbol_esignal_ticker, mic_code)
        Select * from UNNEST ($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) on conflict do nothing",
            &instruments.instrument_name[..],
            &instruments.instrument_type[..],
            &instruments.symbol_ticker[..],
            &instruments.symbol_exchange_ticker[..],
            &instruments.normalized_ticker[..],
            &instruments.symbol_esignal_ticker[..],
            &instruments.mic_code[..],
        ).execute(&connection_pool).await?;
    }
    Ok(())
}

fn transpose_nyse_instruments(instruments: Vec<NyseInstrument>) -> TransposedNyseInstrument {
    let mut result = TransposedNyseInstrument {
        instrument_type: vec![],
        symbol_ticker: vec![],
        symbol_exchange_ticker: vec![],
        normalized_ticker: vec![],
        symbol_esignal_ticker: vec![],
        instrument_name: vec![],
        mic_code: vec![],
    };
    let instruments = filter_for_valid_datasets(instruments);
    for data in instruments {
        result.instrument_type.push(data.instrument_type);
        result.symbol_ticker.push(data.symbol_ticker);
        result
            .symbol_exchange_ticker
            .push(data.symbol_exchange_ticker);
        result.normalized_ticker.push(data.normalized_ticker);
        result
            .symbol_esignal_ticker
            .push(data.symbol_esignal_ticker);
        result.instrument_name.push(data.instrument_name.unwrap());
        result.mic_code.push(data.mic_code);
    }
    result
}

fn filter_for_valid_datasets(input: Vec<NyseInstrument>) -> Vec<NyseInstrument> {
    input
        .into_iter()
        .filter(|instrument| instrument.instrument_name.is_some())
        .collect()
}

impl Collector for NyseInstrumentCollector {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        vec![sp500_fields::Fields::Nyse]
    }

    fn get_source(&self) -> collector_sources::CollectorSource {
        collector_sources::CollectorSource::NyseInstruments
    }
}

async fn get_amount_instruments_available(
    client: &Client,
    url: &str,
) -> Result<u32, anyhow::Error> {
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(create_nyse_instruments_request(1, 1))
        .send()
        .await?
        .text()
        .await?;
    let response = utils::parse_response::<Vec<NysePeekResponse>>(&response)?;

    match response.first() {
        Some(some) => Ok(some.total),
        None => Err(anyhow!(
            "Error while receiving amount of NYSE instruments. Option was None."
        )),
    }
}

fn create_nyse_instruments_request(page: u32, pagesize: u32) -> String {
    let request_json = json!({
        "pageNumber":page,
        "sortColumn":"NORMALIZED_TICKER",
        "sortOrder":"ASC",
        "maxResultsPerPage":pagesize + 1, // For reasons the NYSE API cuts off one result
        "filterToken":""
    });
    request_json.to_string()
}

#[cfg(test)]
mod test {
    use chrono::Utc;
    use httpmock::{Method::POST, MockServer};

    use crate::collectors::{source_apis::nyse_instruments::NysePeekResponse, utils};
    use crate::utils::test_helpers::get_test_client;
    use sqlx::{Pool, Postgres};
    use tracing_test::traced_test;

    use super::*;

    #[test]
    fn parse_nyse_instruments_response_with_one_result() {
        let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
        let parsed = utils::parse_response::<Vec<NyseInstrument>>(input_json).unwrap();
        let instrument = NyseInstrument {
            instrument_type: "COMMON_STOCK".to_string(),
            symbol_ticker: "A".to_string(),
            symbol_exchange_ticker: "A".to_string(),
            normalized_ticker: "A".to_string(),
            symbol_esignal_ticker: "A".to_string(),
            instrument_name: Some("AGILENT TECHNOLOGIES INC".to_string()),
            mic_code: "XNYS".to_string(),
        };
        assert_eq!(parsed[0], instrument);
    }

    #[test]
    fn parse_nyse_instruments_peek_response_with_one_result() {
        let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
        let parsed = utils::parse_response::<Vec<NysePeekResponse>>(input_json).unwrap();
        let instrument = NysePeekResponse { total: 13202 };
        assert_eq!(parsed[0], instrument);
    }

    #[test]
    fn create_request_statement() {
        let expected = r#"{"filterToken":"","maxResultsPerPage":2,"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC"}"#;
        let build = create_nyse_instruments_request(1, 1);
        assert_eq!(expected, build);
    }

    #[traced_test]
    #[sqlx::test]
    async fn query_http_and_write_to_db(pool: Pool<Postgres>) -> Result<(), anyhow::Error> {
        // Start a lightweight mock server.
        let server = MockServer::start();
        let url = server.base_url();
        let request_json = r#"{"filterToken":"","maxResultsPerPage":2,"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC"}"#;
        let response_json = r#"[{"total":1,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"B","normalizedTicker":"C","symbolEsignalTicker":"D","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;

        server.mock(|when, then| {
            when.method(POST)
                .header("content-type", "application/json")
                .body(request_json);
            then.status(200)
                .header("content-type", "application/json")
                .body(response_json);
        });

        let client = get_test_client();

        load_and_store_missing_data_given_url(pool.clone(), client, &url).await?;

        let saved = sqlx::query!("SELECT instrument_name, instrument_type, symbol_ticker, symbol_exchange_ticker, normalized_ticker, symbol_esignal_ticker, mic_code, dateloaded, is_staged FROM public.nyse_instruments;").fetch_one(&pool).await?;
        assert_eq!(saved.instrument_name, "AGILENT TECHNOLOGIES INC");
        assert_eq!(saved.instrument_type, "COMMON_STOCK");
        assert_eq!(saved.symbol_ticker, "A");
        assert_eq!(saved.symbol_exchange_ticker.unwrap(), "B");
        assert_eq!(saved.normalized_ticker.unwrap(), "C");
        assert_eq!(saved.symbol_esignal_ticker.unwrap(), "D");
        assert_eq!(saved.mic_code, "XNYS");
        assert_eq!(saved.dateloaded.unwrap(), Utc::now().date_naive());
        assert_eq!(saved.is_staged, false);
        Ok(())
    }
}
