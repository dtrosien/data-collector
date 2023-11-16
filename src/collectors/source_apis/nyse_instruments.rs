use std::fmt::Display;

use crate::{tasks::runnable::Runnable, utils::errors::Result};

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use reqwest::Client;
use serde::Deserialize;
use sqlx::PgPool;
use string_builder::Builder;
use tracing::info;

use crate::collectors::{collector_sources, sp500_fields, Collector};

const URL: &str = "https://www.nyse.com/api/quotes/filter";

#[derive(Default, Deserialize, Debug, PartialEq)]
struct NysePeekResponse {
    pub total: u32,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
struct NyseInstruments {
    pub instruments: Vec<NyseInstrument>,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
struct NyseInstrument {
    pub instrumentType: String,
    pub symbolTicker: String,
    pub symbolExchangeTicker: String,
    pub normalizedTicker: String,
    pub symbolEsignalTicker: String,
    pub instrumentName: Option<String>,
    pub micCode: String,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
struct TransposedNyseInstrument {
    pub instrumentType: Vec<String>,
    pub symbolTicker: Vec<String>,
    pub symbolExchangeTicker: Vec<String>,
    pub normalizedTicker: Vec<String>,
    pub symbolEsignalTicker: Vec<String>,
    pub instrumentName: Vec<String>,
    pub micCode: Vec<String>,
}

#[derive(Clone)]
pub struct NyseInstrumentCollector {
    pool: PgPool,
}

impl NyseInstrumentCollector {
    pub fn new(pool: PgPool) -> Self {
        NyseInstrumentCollector { pool }
    }
}

impl Display for NyseInstrumentCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NyseInstrumentCollector struct.")
    }
}

#[async_trait]
impl Runnable for NyseInstrumentCollector {
    async fn run(&self) -> Result<()> {
        load_and_store_missing_data(self.pool.clone()).await
    }
}

pub async fn load_and_store_missing_data(connection_pool: PgPool) -> Result<()> {
    info!("Starting to load NYSE instruments");
    let client = Client::new();
    let page_size = get_amount_instruments_available(&client, URL).await;
    // let pages_available: u32 = (peak_count as f32 / page_size as f32).ceil() as u32;
    let list_of_pages: Vec<u32> = (1..=1).collect();
    for page in list_of_pages {
        let request = create_nyse_instruments_request(page, page_size);
        let response = client
            .post(URL)
            .header("content-type", "application/json")
            .body(request)
            .send()
            .await?
            .text()
            .await?;
        let instruments: Vec<NyseInstrument> = parse_nyse_instruments_response(&response)?;
        let instruments = transpose_nyse_instruments(instruments);
        sqlx::query!("INSERT INTO public.nyse_instruments 
        (instrument_name, instrument_type, symbol_ticker, symbol_exchange_ticker, normalized_ticker, symbol_esignal_ticker, mic_code)
        Select * from UNNEST ($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) on conflict do nothing",
            &instruments.instrumentName[..],
            &instruments.instrumentType[..],
            &instruments.symbolTicker[..],
            &instruments.symbolExchangeTicker[..],
            &instruments.normalizedTicker[..],
            &instruments.symbolEsignalTicker[..],
            &instruments.micCode[..],
        ).execute(&connection_pool).await?;
    }
    Ok(())
}

fn transpose_nyse_instruments(instruments: Vec<NyseInstrument>) -> TransposedNyseInstrument {
    let mut result = TransposedNyseInstrument {
        instrumentType: vec![],
        symbolTicker: vec![],
        symbolExchangeTicker: vec![],
        normalizedTicker: vec![],
        symbolEsignalTicker: vec![],
        instrumentName: vec![],
        micCode: vec![],
    };
    let instruments = filter_for_valid_datasets(instruments);
    for data in instruments {
        result.instrumentType.push(data.instrumentType);
        result.symbolTicker.push(data.symbolTicker);
        result.symbolExchangeTicker.push(data.symbolExchangeTicker);
        result.normalizedTicker.push(data.normalizedTicker);
        result.symbolEsignalTicker.push(data.symbolEsignalTicker);
        result.instrumentName.push(data.instrumentName.unwrap());
        result.micCode.push(data.micCode);
    }
    result
}

fn filter_for_valid_datasets(input: Vec<NyseInstrument>) -> Vec<NyseInstrument> {
    input
        .into_iter()
        .filter(|instrument| instrument.instrumentName.is_some())
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

async fn get_amount_instruments_available(client: &Client, url: &str) -> u32 {
    let response = match match client.post(url).header("content-type", "application/json").body(r#"{"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":2,"filterToken":""}"#).send().await {
        Ok(ok) => ok,
        Err(_) => return 0,
    }
    .text()
    .await
    {
        Ok(ok) => ok,
        Err(_) => return 0,
    };
    let response = match parse_nyse_peek_response(&response) {
        Ok(ok) => ok,
        Err(_) => return 0,
    };
    response
        .first()
        .unwrap_or(&NysePeekResponse { total: 0 })
        .total
}

fn create_nyse_instruments_request(page: u32, pagesize: u32) -> String {
    let f = r#"{"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":2,"filterToken":""}"#;
    let mut bob_the_builder = Builder::default();
    bob_the_builder.append(r#"{"pageNumber":"#);
    bob_the_builder.append(page.to_string());
    bob_the_builder
        .append(r#","sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":"#);
    bob_the_builder.append((pagesize + 1).to_string()); // For reasons the NYSE API cuts off one result
    bob_the_builder.append(r#","filterToken":""}"#);
    bob_the_builder.string().unwrap()
}

fn parse_nyse_instruments_response(response: &str) -> Result<Vec<NyseInstrument>> {
    let response = match serde_json::from_str(response) {
        Ok(ok) => ok,
        Err(error) => {
            tracing::error!("Failed to parse response: {}", response);
            return Err(Box::new(error));
        }
    };
    Ok(response)
}

fn parse_nyse_peek_response(response: &str) -> Result<Vec<NysePeekResponse>> {
    if response.len() == 0 {
        return Ok(vec![]);
    }
    let response = match serde_json::from_str(response) {
        Ok(ok) => ok,
        Err(error) => {
            tracing::error!("Failed to parse response: {}", response);
            return Err(Box::new(error));
        }
    };
    Ok(response)
}

#[cfg(test)]
mod test {
    use crate::{
        collectors::source_apis::nyse_instruments::{parse_nyse_peek_response, NysePeekResponse},
        utils::errors::Result,
    };
    use reqwest::Client;

    use super::*;
    #[test]
    fn parse_nyse_instruments_response_with_one_result() {
        let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
        let parsed = parse_nyse_instruments_response(input_json).unwrap();
        let instrument = NyseInstrument {
            instrumentType: "COMMON_STOCK".to_string(),
            symbolTicker: "A".to_string(),
            symbolExchangeTicker: "A".to_string(),
            normalizedTicker: "A".to_string(),
            symbolEsignalTicker: "A".to_string(),
            instrumentName: Some("AGILENT TECHNOLOGIES INC".to_string()),
            micCode: "XNYS".to_string(),
        };
        assert_eq!(parsed[0], instrument);
    }
    #[test]
    fn parse_nyse_instruments_peek_response_with_one_result() {
        let input_json = r#"[{"total":13202,"url":"https://www.nyse.com/quote/XNYS:A","exchangeId":"558","instrumentType":"COMMON_STOCK","symbolTicker":"A","symbolExchangeTicker":"A","normalizedTicker":"A","symbolEsignalTicker":"A","instrumentName":"AGILENT TECHNOLOGIES INC","micCode":"XNYS"}]"#;
        let parsed = parse_nyse_peek_response(input_json).unwrap();
        let instrument = NysePeekResponse { total: 13202 };
        assert_eq!(parsed[0], instrument);
    }

    #[test]
    fn create_request_statement() {
        let expected = r#"{"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":2,"filterToken":""}"#;
        let build = create_nyse_instruments_request(1, 1);
        assert_eq!(expected, build);
    }

    // #[tokio::test]
    // async fn load_amount_instruments_test() -> Result<()> {
    //     let c = Client::new();
    //     let number = get_amount_instruments_available(&c, URL).await;
    //     assert_eq!(number, 13202);
    //     Ok(())
    // }
    // use super::load_and_store_missing_data;
    // #[tokio::test]
    // async fn load_data(pool: Pool<Postgres>) -> Result<()> {
    //     load_and_store_missing_data(pool);
    // }
}
