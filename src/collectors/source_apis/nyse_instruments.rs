use std::fmt::Display;

use crate::{collectors::utils, tasks::runnable::Runnable, utils::errors::Result};

use async_trait::async_trait;

use reqwest::Client;
use serde::Deserialize;
use sqlx::PgPool;
use string_builder::Builder;
use tracing::info;

use crate::collectors::{collector_sources, sp500_fields, Collector};

const URL: &str = "https://www.nyse.com/api/quotes/filter";

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

pub async fn load_and_store_missing_data(connection_pool: PgPool) -> Result<()> {
    info!("Starting to load NYSE instruments");
    let client = Client::new();
    let page_size = get_amount_instruments_available(&client, URL).await;
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
        let instruments = utils::parse_response::<Vec<NyseInstrument>>(&response)?;
        let instruments = transpose_nyse_instruments(instruments);
        sqlx::query!("INSERT INTO public.nyse_instruments 
        (instrument_name, instrument_type, symbol_ticker, symbol_exchange_ticker, normalized_ticker, symbol_esignal_ticker, mic_code)
        Select * from UNNEST ($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) on conflict do nothing",
            &instruments.instrument_type[..],
            &instruments.symbol_ticker[..],
            &instruments.symbol_exchange_ticker[..],
            &instruments.normalized_ticker[..],
            &instruments.symbol_esignal_ticker[..],
            &instruments.instrument_name[..],
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
    let response = match utils::parse_response::<Vec<NysePeekResponse>>(&response) {
        Ok(ok) => ok,
        Err(_) => return 0,
    };

    response
        .first()
        .unwrap_or(&NysePeekResponse { total: 0 })
        .total
}

fn create_nyse_instruments_request(page: u32, pagesize: u32) -> String {
    let _f = r#"{"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":2,"filterToken":""}"#;
    let mut bob_the_builder = Builder::default();
    bob_the_builder.append(r#"{"pageNumber":"#);
    bob_the_builder.append(page.to_string());
    bob_the_builder
        .append(r#","sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":"#);
    bob_the_builder.append((pagesize + 1).to_string()); // For reasons the NYSE API cuts off one result
    bob_the_builder.append(r#","filterToken":""}"#);
    bob_the_builder.string().unwrap()
}

#[cfg(test)]
mod test {
    use crate::collectors::{source_apis::nyse_instruments::NysePeekResponse, utils};

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
        let expected = r#"{"pageNumber":1,"sortColumn":"NORMALIZED_TICKER","sortOrder":"ASC","maxResultsPerPage":2,"filterToken":""}"#;
        let build = create_nyse_instruments_request(1, 1);
        assert_eq!(expected, build);
    }
}
