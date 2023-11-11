use std::{error::Error, fmt::Display};

use crate::{client, error::Result};
use futures_util::{future::BoxFuture, Future};
use reqwest::Client;
use serde::Deserialize;
use sqlx::PgPool;
use string_builder::Builder;
use tracing::info;

use crate::{
    collectors::{collector_sources, sp500_fields, Collector},
    runner::Runnable,
};

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
    pub instrumentName: String,
    pub micCode: String,
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

impl Runnable for NyseInstrumentCollector {
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>> {
        let f = load_and_store_missing_data(self.pool.clone());
        Box::pin(f)
    }
}

pub async fn load_and_store_missing_data(connection_pool: PgPool) -> Result<()> {
    info!("Starting to load NYSE instruments");
    let client = Client::new();
    let amout_instruments = get_amount_instruments_available(&client, URL);
    Ok(())
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
    use reqwest::Client;

    use crate::{
        error::Result,
        source_apis::nyse_instruments::{parse_nyse_peek_response, NysePeekResponse},
    };

    use super::{
        create_nyse_instruments_request, get_amount_instruments_available,
        parse_nyse_instruments_response, NyseInstrument, URL,
    };
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
            instrumentName: "AGILENT TECHNOLOGIES INC".to_string(),
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
}
