use anyhow::anyhow;
use std::fmt::Display;
use std::thread::sleep;
use std::time::Duration;

use crate::utils::action_helpers;

use async_trait::async_trait;
use futures_util::TryFutureExt;

use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, info};

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://www.nyse.com/api/quotes/filter";

#[derive(Clone, Debug)]
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
    #[tracing::instrument(name = "Run NyseInstrumentCollector", skip(self))]
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
#[serde(rename_all = "camelCase")]
struct NyseInstrument {
    pub instrument_type: Option<String>,
    pub symbol_ticker: Option<String>,
    pub symbol_exchange_ticker: Option<String>,
    pub normalized_ticker: Option<String>,
    pub symbol_esignal_ticker: Option<String>,
    pub instrument_name: Option<String>,
    pub mic_code: Option<String>,
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

#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_and_store_missing_data(
    connection_pool: PgPool,
    client: Client,
) -> Result<(), anyhow::Error> {
    load_and_store_missing_data_given_url(connection_pool, client, URL).await
}

#[tracing::instrument(level = "debug", skip_all)]
async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load NYSE instruments");
    let page_size = get_amount_instruments_available(&client, url).await?;
    let request = create_nyse_incomplete_instruments_request(1, page_size);
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(request)
        .send()
        .await?
        .text()
        .await?;
    let incomplete_instruments = action_helpers::parse_response::<Vec<NyseInstrument>>(&response)?;

    let mut instruments: Vec<NyseInstrument> = vec![];
    for instrument in incomplete_instruments {
        let body = create_nyse_instruments_request_body(instrument.normalized_ticker.unwrap());
        let response = client
            .post(url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await;
        match response {
            Err(_) => break,
            Ok(ok) => {
                let response = ok.text().await?;
                info!("response: {}", response);
                instruments.push(
                    action_helpers::parse_response::<Vec<NyseInstrument>>(&response)?
                        .swap_remove(0),
                );
                sleep(Duration::from_millis(2000));
            }
        }
    }
    instruments.retain(|inst| inst.normalized_ticker.is_some());

    let instruments = transpose_nyse_instruments(instruments);
    debug!("instruments: {:?}", instruments);
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
    // 19298 in db
    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
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
    for mut data in instruments {
        result.instrument_type.push(
            data.instrument_type
                .get_or_insert("".to_string())
                .to_string(),
        );
        result
            .symbol_ticker
            .push(data.symbol_ticker.get_or_insert("".to_string()).to_string());
        result.symbol_exchange_ticker.push(
            data.symbol_exchange_ticker
                .get_or_insert("".to_string())
                .to_string(),
        );
        result.normalized_ticker.push(
            data.normalized_ticker
                .get_or_insert("".to_string())
                .to_string(),
        );
        result.symbol_esignal_ticker.push(
            data.symbol_esignal_ticker
                .get_or_insert("".to_string())
                .to_string(),
        );
        result.instrument_name.push(data.instrument_name.unwrap());
        result
            .mic_code
            .push(data.mic_code.get_or_insert("".to_string()).to_string());
    }
    result
}

#[tracing::instrument(level = "debug", skip_all)]
fn filter_for_valid_datasets(input: Vec<NyseInstrument>) -> Vec<NyseInstrument> {
    input
        .into_iter()
        .filter(|instrument| instrument.instrument_name.is_some())
        .collect()
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_amount_instruments_available(
    client: &Client,
    url: &str,
) -> Result<u32, anyhow::Error> {
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(create_nyse_incomplete_instruments_request(1, 1))
        .send()
        .await?
        .text()
        .await?;
    // info!("amount instruments: {}", response);
    let response = action_helpers::parse_response::<Vec<NysePeekResponse>>(&response)?;

    match response.first() {
        Some(some) => Ok(some.total),
        None => Err(anyhow!(
            "Error while receiving amount of NYSE instruments. Option was None."
        )),
    }
}

fn create_nyse_instruments_request_body(normalized_ticker: String) -> String {
    let request_json = json!({
        "micCode":"XNYS",
        "normalizedTicker":normalized_ticker}
    );
    request_json.to_string()
}

fn create_nyse_incomplete_instruments_request(page: u32, pagesize: u32) -> String {
    let request_json = json!({
        "pageNumber":page,
        "sortColumn":"NORMALIZED_TICKER",
        "sortOrder":"ASC",
        "maxResultsPerPage":pagesize + 1, // For reasons the NYSE API cuts off one result
        "filterToken":"",
        "instrumentType":"EQUITY"
    });
    request_json.to_string()
}

#[cfg(test)]
mod test {
    use chrono::Utc;
    use httpmock::{Method::POST, MockServer};

    use crate::actions::collect::nyse_instruments::NysePeekResponse;
    use crate::utils::test_helpers::get_test_client;
    use sqlx::{Pool, Postgres};

    use super::*;

    #[test]
    fn parse_nyse_instruments_response_with_one_result() {
        let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
        let parsed = action_helpers::parse_response::<Vec<NyseInstrument>>(input_json).unwrap();
        let instrument = NyseInstrument {
            instrument_type: Some("COMMON_STOCK".to_string()),
            symbol_ticker: Some("A".to_string()),
            symbol_exchange_ticker: Some("A".to_string()),
            normalized_ticker: Some("A".to_string()),
            symbol_esignal_ticker: Some("A".to_string()),
            instrument_name: Some("AGILENT TECHNOLOGIES INC".to_string()),
            mic_code: Some("XNYS".to_string()),
        };
        assert_eq!(parsed[0], instrument);
    }

    #[test]
    fn parse_nyse_instruments_peek_response_with_one_result() {
        let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
        let parsed = action_helpers::parse_response::<Vec<NysePeekResponse>>(input_json).unwrap();
        let instrument = NysePeekResponse { total: 13202 };
        assert_eq!(parsed[0], instrument);
    }

    #[test]
    fn create_request_statement() {
        let expected = r#"{"filterToken":"","instrumentType":"EQUITY","maxResultsPerPage":2,"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC"}"#;
        let build = create_nyse_incomplete_instruments_request(1, 1);
        assert_eq!(expected, build);
    }

    #[sqlx::test]
    async fn query_http_and_write_to_db(pool: Pool<Postgres>) -> Result<(), anyhow::Error> {
        // Start a lightweight mock server.
        let server = MockServer::start();
        let url = server.base_url();
        let initial_request_json = r#"{"filterToken":"","instrumentType":"EQUITY","maxResultsPerPage":2,"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC"}"#;
        let initial_response_json = r#"[{"total":1,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":null,"instrumentType":null,"symbolTicker": null,"symbolExchangeTicker":"B","normalizedTicker":"C","symbolEsignalTicker": null,"instrumentName":"AGILENT TECHNOLOGIES INC","micCode":null }]"#;
        let symbol_request_json = r#"{"micCode":"XNYS","normalizedTicker":"C"}"#;
        let symbol_response_json = r#"[{"total":1,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"B","normalizedTicker":"C","symbolEsignalTicker":"D","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;

        server.mock(|when, then| {
            when.method(POST)
                .header("content-type", "application/json")
                .body(initial_request_json);
            then.status(200)
                .header("content-type", "application/json")
                .body(initial_response_json);
        });
        server.mock(|when, then| {
            when.method(POST)
                .header("content-type", "application/json")
                .body(symbol_request_json);
            then.status(200)
                .header("content-type", "application/json")
                .body(symbol_response_json);
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
