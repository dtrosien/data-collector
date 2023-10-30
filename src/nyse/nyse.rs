use std::error;

use chrono::prelude::*;
use chrono::{DateTime, Days, NaiveDate};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::Postgres;

#[derive(Default, Deserialize, Serialize, Debug)]
struct NyseRequest {
    action_date__gte: NaiveDate,
    action_date__lte: NaiveDate,
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

impl NyseRequest {
    pub fn new(action_date__gte: NaiveDate, days: u64, page: u32, page_size: u32) -> Self {
        Self {
            action_date__gte,
            action_date__lte: action_date__gte
                .checked_add_days(Days::new(days - 1))
                .expect("Date should never leave the allowed range."),
            page,
            page_size,
        }
    }
}

pub async fn load_and_store_missing_data(
    url: &str,
    connection_pool: &sqlx::Pool<Postgres>,
) -> Result<(), Box<dyn error::Error>> {
    let now = Utc::now();
    let mut latest_date = latest_date_available();
    let client = Client::new();
    while latest_date < now {
        let week_data = load_missing_week(&client, &latest_date, url).await?;
        let (
            action_date,
            action_status,
            action_type,
            issue_symbol,
            issuer_name,
            updated_at,
            market_event,
        ): (
            Vec<Option<String>>,
            Vec<String>,
            Vec<String>,
            Vec<String>,
            Vec<String>,
            Vec<String>,
            Vec<String>,
        ) = week_data
            .into_iter()
            .map(|nyse_data| {
                (
                    nyse_data.action_date,
                    nyse_data.action_status,
                    nyse_data.action_type,
                    nyse_data.issue_symbol,
                    nyse_data.issuer_name,
                    nyse_data.updated_at,
                    nyse_data.market_event,
                )
            })
            .fold(
                (
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
                |(
                    mut action_dates,
                    mut action_statuses,
                    mut action_types,
                    mut issue_symbols,
                    mut issure_names,
                    mut updated_ats,
                    mut market_events,
                ),
                 (
                    action_date,
                    action_status,
                    action_type,
                    issue_symbol,
                    issuer_name,
                    updated_at,
                    market_event,
                )| {
                    action_dates.push(action_date);
                    action_statuses.push(action_status.unwrap());
                    action_types.push(action_type);
                    issue_symbols.push(issue_symbol.unwrap());
                    issure_names.push(issuer_name.unwrap());
                    updated_ats.push(updated_at);
                    market_events.push(market_event);
                    (
                        action_dates,
                        action_statuses,
                        action_types,
                        issue_symbols,
                        issure_names,
                        updated_ats,
                        market_events,
                    )
                },
            );

        let action_date: Vec<NaiveDate> = action_date
            .into_iter()
            .map(|e| e.unwrap_or("2015-01-01".to_string()))
            .map(|f| NaiveDate::parse_from_str(&f, "%Y-%m-%d").unwrap())
            .collect();

        sqlx::query!("INSERT INTO nyse_events
            (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event)
            Select * from UNNEST ($1::date[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) on conflict do nothing",
        &action_date[..],
        &action_status[..],
        &action_type[..],
        &issue_symbol[..],
        &issuer_name[..],
        &updated_at[..],
        &market_event[..],
    ).execute(connection_pool)
    .await.unwrap();

        latest_date = latest_date
            .checked_add_days(Days::new(7))
            .expect("Date should never leave the allowed range.");
    }
    Ok(())
}

pub async fn load_missing_week(
    client: &Client,
    date: &DateTime<Utc>,
    url: &str,
) -> Result<Vec<NyseData>, Box<dyn error::Error>> {
    let max_page_size = 100; //API does not allow more entries.
    let mut output: Vec<NyseData> = vec![];

    let peak_count = peek_number_results(&client, date, url).await?;

    let pages_available: u32 = (peak_count as f32 / max_page_size as f32).ceil() as u32;
    let list_of_pages: Vec<u32> = (1..=pages_available).collect();

    for page in list_of_pages {
        let response = request_nyse(&client, url, date, page, max_page_size).await?;
        let mut response: NyseResponse = parse_nyse_response(&response)?;

        output.append(&mut response.results);
    }
    let output = output
        .into_iter()
        .filter(|e| {
            e.action_date.is_some()
                && e.issue_symbol.is_some()
                && e.issuer_name.is_some()
                && e.action_status.is_some()
        })
        .collect();
    Ok(output)
}

async fn peek_number_results(
    client: &Client,
    date: &DateTime<Utc>,
    url: &str,
) -> Result<u32, Box<dyn error::Error>> {
    let peak_response = request_nyse(client, url, date, 1, 1).await?;
    let peek_response = parse_nyse_peek_response(&peak_response)?;

    Ok(peek_response.count.unwrap_or(0))
}

async fn request_nyse(
    client: &Client,
    url: &str,
    date: &DateTime<Utc>,
    page: u32,
    max_page_size: u32,
) -> Result<String, Box<dyn error::Error>> {
    let response = match client
        .get(url)
        .query(&NyseRequest::new(date.date_naive(), 7, page, max_page_size))
        .send()
        .await
    {
        Ok(ok) => ok,
        Err(error) => {
            println!("Error while loading data from NYSE ({}).", url);
            if let Some(x) = error.url() {
                println!("Error caused by query: {}", x);
            }
            return Err(Box::new(error));
        }
    }
    .text()
    .await?;
    Ok(response)
}

fn parse_nyse_peek_response(
    peak_response: &str,
) -> Result<NysePeekResponse, Box<serde_json::Error>> {
    let peak_response = match serde_json::from_str(peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            println!("Failed to parse response: {}", peak_response);
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

fn parse_nyse_response(peak_response: &str) -> Result<NyseResponse, Box<serde_json::Error>> {
    let peak_response = match serde_json::from_str(peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            println!("Failed to parse response: {}", peak_response);
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

pub fn latest_date_available() -> DateTime<Utc> {
    //TODO: Get latest date from database
    let p = Utc.with_ymd_and_hms(2015, 12, 7, 0, 0, 0).unwrap();
    // let p = Utc.with_ymd_and_hms(2023, 10, 25, 0, 0, 0).unwrap();
    p
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};
    use httpmock::{Method::GET, MockServer};
    use reqwest::Client;

    use crate::nyse::nyse::*;

    #[test]
    fn start_within_data_date_range() {
        let earliest_data_date = Utc.with_ymd_and_hms(2015, 12, 7, 0, 0, 0).unwrap();
        assert!(earliest_data_date <= latest_date_available());
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

        let date = Utc.with_ymd_and_hms(2023, 10, 24, 0, 0, 0).unwrap();
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

        let date = Utc.with_ymd_and_hms(2023, 10, 24, 0, 0, 0).unwrap();
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
