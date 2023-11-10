use std::fmt::Display;

use crate::error::Result;
use chrono::prelude::*;
use chrono::{Days, NaiveDate};
use futures_util::future::BoxFuture;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::collectors::{collector_sources, sp500_fields, Collector};
use crate::runner::Runnable;
use sqlx::{PgPool, Postgres};
use tracing::{debug, info, warn};

const NYSE_EVENT_URL: &str = "https://listingmanager.nyse.com/api/corpax/";

#[derive(Default, Deserialize, Serialize, Debug)]
struct NyseRequest {
    #[serde(rename = "action_date__gte")]
    action_date_gte: NaiveDate,
    #[serde(rename = "action_date__lte")]
    action_date_lte: NaiveDate,
    page: u32,
    page_size: u32,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
struct NysePeekResponse {
    pub count: Option<u32>,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
pub struct NyseResponse {
    pub count: u32,
    pub next: Option<String>,
    pub previous: Option<String>,
    pub results: Vec<NyseData>,
}

#[derive(Default, Deserialize, PartialEq)]
struct MaxDate {
    max_date: Option<NaiveDate>,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
pub struct NyseData {
    pub action_date: Option<String>,
    pub action_status: Option<String>,
    pub action_type: String,
    pub issue_symbol: Option<String>,
    pub issuer_name: Option<String>,
    pub updated_at: String,
    pub market_event: String,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
pub struct CleanedTransposedNyseData {
    pub action_date: Vec<NaiveDate>,
    pub action_status: Vec<String>,
    pub action_type: Vec<String>,
    pub issuer_symbol: Vec<String>,
    pub issuer_name: Vec<String>,
    pub updated_at: Vec<String>,
    pub market_event: Vec<String>,
}

#[derive(Clone)]
pub struct NyseEventCollector {
    pool: PgPool,
}

impl NyseEventCollector {
    pub fn new(pool: PgPool) -> Self {
        NyseEventCollector { pool }
    }
}

impl Display for NyseEventCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NyseEventCollector struct.")
    }
}

impl Runnable for NyseEventCollector {
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>> {
        let f = load_and_store_missing_data(self.pool.clone());
        Box::pin(f)
    }
}

impl Collector for NyseEventCollector {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        vec![sp500_fields::Fields::Nyse]
    }

    fn get_source(&self) -> collector_sources::CollectorSource {
        collector_sources::CollectorSource::NyseEvents
    }
}

impl NyseRequest {
    pub fn new(action_date_gte: NaiveDate, days: u64, page: u32, page_size: u32) -> Self {
        Self {
            action_date_gte,
            action_date_lte: action_date_gte
                .checked_add_days(Days::new(days - 1))
                .expect("Date should never leave the allowed range."),
            page,
            page_size,
        }
    }
}

pub async fn load_and_store_missing_data(connection_pool: PgPool) -> Result<()> {
    info!("Starting to load NYSE events");
    let now = Utc::now();
    let mut latest_date = latest_date_available(&connection_pool).await;
    let client = Client::new();
    while latest_date <= now.date_naive() {
        debug!("Loading NYSE event data for week: {}", latest_date);
        let week_data = load_missing_week(&client, &latest_date, NYSE_EVENT_URL).await?;
        let week_data = transpose_nyse_data_and_filter(week_data);

        sqlx::query!("INSERT INTO nyse_events
            (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event)
            Select * from UNNEST ($1::date[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) on conflict do nothing",
        &week_data.action_date[..],
        &week_data.action_status[..],
        &week_data.action_type[..],
        &week_data.issuer_symbol[..],
        &week_data.issuer_name[..],
        &week_data.updated_at[..],
        &week_data.market_event[..],
    ).execute(&connection_pool)
    .await?;

        latest_date = latest_date
            .checked_add_days(Days::new(7))
            .expect("Date should never leave the allowed range.");
    }
    Ok(())
}

///
/// Transforms the given vector into its components as vectors. The filter will remove all NyseData which contain a None.
/// The resulting tuple contains vectors in the order of:</br>
/// (0:action_date, 1:action_status, 2:action_type, 3:issue_symbol, 4:issuer_name, 5:updated_at, 6:market_event)
///
fn transpose_nyse_data_and_filter(input: Vec<NyseData>) -> CleanedTransposedNyseData {
    let mut result = CleanedTransposedNyseData {
        action_date: vec![],
        action_status: vec![],
        action_type: vec![],
        issuer_symbol: vec![],
        issuer_name: vec![],
        updated_at: vec![],
        market_event: vec![],
    };
    let input = filter_for_valid_datasets(input);
    for data in input {
        result.action_date.push(convert_string_to_naive_date(
            data.action_date
                .expect("None got filtered out in previous code."),
        ));
        result.action_status.push(
            data.action_status
                .expect("None got filtered out in previous code."),
        );
        result.action_type.push(data.action_type);
        result.issuer_symbol.push(
            data.issue_symbol
                .expect("None got filtered out in previous code."),
        );
        result.issuer_name.push(
            data.issuer_name
                .expect("None got filtered out in previous code."),
        );
        result.updated_at.push(data.updated_at);
        result.market_event.push(data.market_event);
    }
    result
}

fn convert_string_to_naive_date(data: String) -> NaiveDate {
    NaiveDate::parse_from_str(&data, "%Y-%m-%d")
        .expect("The NYSE API hopefully does only respond with valid dates.")
}

///A valid dataset consists out of entries with no NULL entries and the dates are either in the past or today.
fn filter_for_valid_datasets(input: Vec<NyseData>) -> Vec<NyseData> {
    let today = Utc::now().date_naive();
    let input: Vec<NyseData> = input
        .into_iter()
        .filter(|nyse_data| {
            nyse_data.action_date.is_some()
                && nyse_data.issue_symbol.is_some()
                && nyse_data.issuer_name.is_some()
                && nyse_data.action_status.is_some()
                && nyse_data
                    .action_date
                    .as_ref()
                    .expect("Existence is guaranteed by greedy && evaluation.")
                    <= &today.to_string()
        })
        .collect();
    input
}

pub async fn load_missing_week(
    client: &Client,
    date: &NaiveDate,
    url: &str,
) -> Result<Vec<NyseData>> {
    let max_page_size = 100; //API does not allow more entries.
    let mut output: Vec<NyseData> = vec![];

    let peak_count = peek_number_results(client, date, url).await?;

    let pages_available: u32 = (peak_count as f32 / max_page_size as f32).ceil() as u32;
    let list_of_pages: Vec<u32> = (1..=pages_available).collect();

    for page in list_of_pages {
        let response = request_nyse(client, url, date, page, max_page_size).await?;
        let mut response: NyseResponse = parse_nyse_response(&response)?;

        output.append(&mut response.results);
    }

    Ok(output)
}

async fn peek_number_results(client: &Client, date: &NaiveDate, url: &str) -> Result<u32> {
    let peak_response = request_nyse(client, url, date, 1, 1).await?;
    let peek_response = parse_nyse_peek_response(&peak_response)?;

    Ok(peek_response.count.unwrap_or(0))
}

async fn request_nyse(
    client: &Client,
    url: &str,
    date: &NaiveDate,
    page: u32,
    max_page_size: u32,
) -> Result<String> {
    let response = match client
        .get(url)
        .query(&NyseRequest::new(*date, 7, page, max_page_size))
        .send()
        .await
    {
        Ok(ok) => {
            debug!("Requested URL for NYSE events: {}", ok.url());
            ok
        }
        Err(error) => {
            tracing::error!("Error while loading data from NYSE ({}).", url);
            if let Some(x) = error.url() {
                tracing::error!("Error caused by query: {}", x);
            }
            return Err(Box::new(error));
        }
    }
    .text()
    .await?;
    Ok(response)
}

fn parse_nyse_peek_response(peak_response: &str) -> Result<NysePeekResponse> {
    let peak_response = match serde_json::from_str(peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            tracing::error!("Failed to parse response: {}", peak_response);
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

fn parse_nyse_response(peak_response: &str) -> Result<NyseResponse> {
    let peak_response = match serde_json::from_str(peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            tracing::error!("Failed to parse response: {}", peak_response);
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

/// Function will query DB and check for the newest date available and return that. If the date is not available, the earliest possible date for the NYSE API is returned. If the date is in the future, the current date will be returned; since this indicates an error in data mangement.
async fn latest_date_available(connection_pool: &sqlx::Pool<Postgres>) -> NaiveDate {
    let earliest_date =
        NaiveDate::parse_from_str("2015-12-07", "%Y-%m-%d").expect("Parsing constant.");
    let db_result = sqlx::query_as!(
        MaxDate,
        "select max(action_date) as max_date from nyse_events"
    )
    .fetch_one(connection_pool)
    .await;
    let db_result = match db_result {
        Ok(mut a) => a.max_date.get_or_insert(earliest_date).to_owned(),
        Err(_) => earliest_date,
    };
    //If db responds with future date, return today
    if db_result > Utc::now().date_naive() {
        warn!(
            "Database answered with future date, for latest available data: {}",
            db_result
        );
        return Utc::now().date_naive();
    }
    db_result
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};
    use httpmock::{Method::GET, MockServer};
    use reqwest::Client;
    use sqlx::Pool;

    use crate::source_apis::nyse::*;

    #[sqlx::test]
    fn empty_database_returns_initial_date(pool: Pool<Postgres>) -> Result<()> {
        let earliest_data_date =
            NaiveDate::parse_from_str("2015-12-07", "%Y-%m-%d").expect("Parsing constant.");
        assert!(earliest_data_date <= latest_date_available(&pool).await);
        Ok(())
    }

    #[sqlx::test]
    fn two_entry_database_returns_later_date(pool: Pool<Postgres>) -> Result<()> {
        sqlx::query!(
            r#"INSERT INTO nyse_events (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event, is_staged) VALUES('2023-10-30', 'Pending before the Open', 'Suspend', 'TRCA U', 'Twin Ridge Capital Acquisition Corp.', '2023-10-25T12:00:46.392605-04:00', 'b2d6f0ae-480c-4f77-b955-6bee917c7b30', false), ('2023-11-01', 'Pending before the Open', 'Suspend', 'TRCA U', 'Twin Ridge Capital Acquisition Corp.', '2023-10-25T12:00:46.392605-04:00', 'b2d6f0ae-480c-4f77-b955-6bee917c7b30', false);"#
        ).execute(&pool).await?;

        let latest_date =
            NaiveDate::parse_from_str("2023-11-01", "%Y-%m-%d").expect("Parsing constant.");
        assert!(latest_date <= latest_date_available(&pool).await);
        Ok(())
    }

    #[test]
    fn parse_nyse_peek_response_with_one_result() {
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":null,"action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":"SQNS","issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        let nyse_peek_response = parse_nyse_peek_response(input_json).unwrap();
        assert_eq!(1, nyse_peek_response.count.unwrap());
    }

    #[test]
    fn parse_nyse_response_with_one_result_and_missing_date() {
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":null,"action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":"SQNS","issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        let nyse_response = parse_nyse_response(input_json).unwrap();
        let data = NyseData {
            action_date: Option::None,
            action_status: Some("Pending before the Open".to_string()),
            action_type: "Suspend".to_string(),
            issue_symbol: Some("SQNS".to_string()),
            issuer_name: Some("Sequans Communications S.A.".to_string()),
            updated_at: "2023-10-20T09:24:47.134141-04:00".to_string(),
            market_event: "54a838d5-b1ae-427a-b7a3-629eb1a0de2c".to_string(),
        };
        let response = NyseResponse {
            count: 1,
            next: Option::None,
            previous: Option::None,
            results: vec![data],
        };
        assert_eq!(1, nyse_response.results.len());
        assert_eq!(response, nyse_response);
    }

    #[test]
    fn parse_nyse_response_with_one_result_and_missing_action_status() {
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":"2015-10-03","action_status":null,"action_type":"Suspend","issue_symbol":"SQNS","issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        let nyse_response = parse_nyse_response(input_json).unwrap();
        let data = NyseData {
            action_date: Some("2015-10-03".to_string()),
            action_status: None,
            action_type: "Suspend".to_string(),
            issue_symbol: Some("SQNS".to_string()),
            issuer_name: Some("Sequans Communications S.A.".to_string()),
            updated_at: "2023-10-20T09:24:47.134141-04:00".to_string(),
            market_event: "54a838d5-b1ae-427a-b7a3-629eb1a0de2c".to_string(),
        };
        let response = NyseResponse {
            count: 1,
            next: Option::None,
            previous: Option::None,
            results: vec![data],
        };
        assert_eq!(1, nyse_response.results.len());
        assert_eq!(response, nyse_response);
    }

    #[test]
    fn parse_nyse_response_with_one_result_and_missing_issuer_symbol() {
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":null,"action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":null,"issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        let nyse_response = parse_nyse_response(input_json).unwrap();
        let data = NyseData {
            action_date: Option::None,
            action_status: Some("Pending before the Open".to_string()),
            action_type: "Suspend".to_string(),
            issue_symbol: None,
            issuer_name: Some("Sequans Communications S.A.".to_string()),
            updated_at: "2023-10-20T09:24:47.134141-04:00".to_string(),
            market_event: "54a838d5-b1ae-427a-b7a3-629eb1a0de2c".to_string(),
        };
        let response = NyseResponse {
            count: 1,
            next: Option::None,
            previous: Option::None,
            results: vec![data],
        };
        assert_eq!(1, nyse_response.results.len());
        assert_eq!(response, nyse_response);
    }

    #[test]
    fn parse_nyse_response_with_one_result_and_missing_issuer_name() {
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":null,"action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":"SQNS","issuer_name":null,"updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        let nyse_response = parse_nyse_response(input_json).unwrap();
        let data = NyseData {
            action_date: Option::None,
            action_status: Some("Pending before the Open".to_string()),
            action_type: "Suspend".to_string(),
            issue_symbol: Some("SQNS".to_string()),
            issuer_name: None,
            updated_at: "2023-10-20T09:24:47.134141-04:00".to_string(),
            market_event: "54a838d5-b1ae-427a-b7a3-629eb1a0de2c".to_string(),
        };
        let response = NyseResponse {
            count: 1,
            next: Option::None,
            previous: Option::None,
            results: vec![data],
        };
        assert_eq!(1, nyse_response.results.len());
        assert_eq!(response, nyse_response);
    }

    #[test]
    fn parse_nyse_response_with_one_result_and_given_date() {
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":"2016-12-05","action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":"SQNS","issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        let nyse_response = parse_nyse_response(input_json).unwrap();
        let data = NyseData {
            action_date: Some("2016-12-05".to_string()),
            action_status: Some("Pending before the Open".to_string()),
            action_type: "Suspend".to_string(),
            issue_symbol: Some("SQNS".to_string()),
            issuer_name: Some("Sequans Communications S.A.".to_string()),
            updated_at: "2023-10-20T09:24:47.134141-04:00".to_string(),
            market_event: "54a838d5-b1ae-427a-b7a3-629eb1a0de2c".to_string(),
        };
        let response = NyseResponse {
            count: 1,
            next: Option::None,
            previous: Option::None,
            results: vec![data],
        };
        assert_eq!(1, nyse_response.results.len());
        assert_eq!(response, nyse_response);
    }

    #[tokio::test]
    async fn request_basic_nyse_response() {
        // Start a lightweight mock server.
        let server = MockServer::start();
        let url = server.base_url();
        let client = Client::new();

        let date = Utc
            .with_ymd_and_hms(2023, 10, 24, 0, 0, 0)
            .unwrap()
            .date_naive();
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":"2016-12-05","action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":"SQNS","issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        // Create a mock on the server.
        let hello_mock = server.mock(|when, then| {
            when.method(GET)
                .query_param("action_date__gte", "2023-10-24")
                .query_param("action_date__lte", "2023-10-30")
                .query_param("page", "1")
                .query_param("page_size", "100");
            then.status(200)
                .header("content-type", "text/html")
                .body(&input_json);
        });

        let result = request_nyse(&client, &url, &date, 1, 100).await.unwrap();

        hello_mock.assert();
        assert_eq!(result, input_json);
    }

    #[tokio::test]
    async fn request_basic_nyse_peek_response() {
        // Start a lightweight mock server.
        let server = MockServer::start();
        let url = server.base_url();
        let client = Client::new();

        let date = Utc
            .with_ymd_and_hms(2023, 10, 24, 0, 0, 0)
            .unwrap()
            .date_naive();
        let input_json = r#"{"count":1,"next":null,"previous":null,"results":[{"action_date":"2016-12-05","action_status":"Pending before the Open","action_type":"Suspend","issue_symbol":"SQNS","issuer_name":"Sequans Communications S.A.","updated_at":"2023-10-20T09:24:47.134141-04:00","market_event":"54a838d5-b1ae-427a-b7a3-629eb1a0de2c"}]}"#;
        // Create a mock on the server.
        let hello_mock = server.mock(|when, then| {
            when.method(GET)
                .query_param("action_date__gte", "2023-10-24")
                .query_param("action_date__lte", "2023-10-30")
                .query_param("page", "1")
                .query_param("page_size", "100");
            then.status(200)
                .header("content-type", "text/html")
                .body(&input_json);
        });

        let expected = NysePeekResponse { count: Some(1) };

        let result = request_nyse(&client, &url, &date, 1, 100).await.unwrap();
        let result = parse_nyse_peek_response(&result).unwrap();
        hello_mock.assert();
        assert_eq!(expected, result);
    }
}
