use std::error;

use chrono::prelude::*;
use chrono::{DateTime, Days, NaiveDate};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Serialize, Debug)]
struct NyseRequest {
    action_date__gte: NaiveDate,
    action_date__lte: NaiveDate,
    page: u32,
    page_size: u32,
}

#[derive(Default, Deserialize, Debug)]
struct NysePeekResponse {
    pub count: Option<u32>,
}

#[derive(Default, Deserialize, Debug)]
pub struct NyseResponse {
    pub count: u32,
    pub next: Option<String>,
    pub previous: Option<String>,
    pub results: Vec<NyseData>,
}

#[derive(Default, Deserialize, Debug)]
pub struct NyseData {
    pub action_date: Option<String>,
    pub action_status: String,
    pub action_type: String,
    pub issue_symbol: String,
    pub issuer_name: String,
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

pub async fn load_and_store_missing_data() -> Result<Vec<NyseData>, Box<dyn error::Error>> {
    let now = Utc::now();
    let mut latest_date = latest_date_available();
    let mut data: Vec<NyseData> = Vec::new();
    let client = Client::new();
    while latest_date < now {
        data.append(&mut load_missing_week(&client, &latest_date).await?);
        latest_date = latest_date
            .checked_add_days(Days::new(7))
            .expect("Date should never leave the allowed range.");
    }
    Ok(data)
}

pub async fn load_missing_week(
    client: &Client,
    date: &DateTime<Utc>,
) -> Result<Vec<NyseData>, Box<dyn error::Error>> {
    let max_page_size = 100; //API does not allow more entries.
    let mut output: Vec<NyseData> = vec![];

    let peak_count = peek_number_results(&client, date).await?;

    let pages_available: u32 = (peak_count as f32 / max_page_size as f32).ceil() as u32;
    let list_of_pages: Vec<u32> = (1..=pages_available).collect();

    for page in list_of_pages {
        let response = request_nyse(&client, date, page, max_page_size).await?;
        let mut response: NyseResponse = parse_nyse_response(response)?;

        output.append(&mut response.results);
    }
    Ok(output)
}

async fn peek_number_results<'a>(
    client: &Client,
    date: &'a DateTime<Utc>,
) -> Result<u32, Box<dyn error::Error>> {
    let peak_response = request_nyse(client, date, 1, 1).await?;
    let peek_response = parse_nyse_peek_response(peak_response)?;

    Ok(peek_response.count.unwrap_or(0))
}

async fn request_nyse(
    client: &Client,
    date: &DateTime<Utc>,
    page: u32,
    max_page_size: u32,
) -> Result<String, Box<dyn error::Error>> {
    let response = match client
        .get("https://listingmanager.nyse.com/api/corpax/")
        .query(&NyseRequest::new(date.date_naive(), 7, page, max_page_size))
        .send()
        .await
    {
        Ok(ok) => ok,
        Err(error) => {
            println!(
                "Error while loading data from NYSE (https://listingmanager.nyse.com/api/corpax/)."
            );
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
    peak_response: String,
) -> Result<NysePeekResponse, Box<serde_json::Error>> {
    let peak_response = match serde_json::from_str(&peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            println!("Failed to parse response: {}", &peak_response);
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

fn parse_nyse_response(peak_response: String) -> Result<NyseResponse, Box<serde_json::Error>> {
    let peak_response = match serde_json::from_str(&peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            println!("Failed to parse response: {}", &peak_response);
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

fn latest_date_available() -> DateTime<Utc> {
    //TODO: Get latest date from database
    // let p = Utc.with_ymd_and_hms(2015, 12, 7, 0, 0, 0).unwrap();
    let p = Utc.with_ymd_and_hms(2014, 10, 9, 0, 0, 0).unwrap();
    p
}
