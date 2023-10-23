use chrono::prelude::*;
use chrono::{DateTime, Days, NaiveDate};
use reqwest::{Client, Error};
use serde::{Deserialize, Serialize};
use serde_json::error;

use std::collections::HashMap;

#[derive(Default, Deserialize, Debug)]
struct NysePeekResponse {
    pub count: Option<u32>,
    pub next: String,
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

#[derive(Default, Deserialize, Serialize, Debug)]
struct NyseRequest {
    action_date__gte: NaiveDate,
    action_date__lte: NaiveDate,
    page: u32,
    page_size: u32,
}

impl NyseRequest {
    pub fn new(action_date__gte: NaiveDate, days: u64, page: u32, page_size: u32) -> Self {
        Self {
            action_date__gte,
            action_date__lte: action_date__gte
                .checked_add_days(Days::new(days - 1))
                .expect("Date should never leave the allowed values."),
            page,
            page_size,
        }
    }
}

fn latest_date_available() -> DateTime<Utc> {
    //TODO: Get latest date from database
    // let p = Utc.with_ymd_and_hms(2015, 12, 7, 0, 0, 0).unwrap();
    let p = Utc.with_ymd_and_hms(2014, 10, 9, 0, 0, 0).unwrap();
    p
}

pub async fn load_and_store_missing_data() -> Result<Vec<NyseData>, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let mut latest_date = latest_date_available();
    let mut data: Vec<NyseData> = Vec::new();
    while latest_date < now {
        data.append(&mut load_missing_week(&latest_date).await?);
        latest_date = latest_date
            .checked_add_days(Days::new(7))
            .expect("Date should never leave the allowed values.");
    }
    Ok(data)
}

async fn load_missing_week(
    date: &DateTime<Utc>,
) -> Result<Vec<NyseData>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let max_page_size = 100;
    let mut output: Vec<NyseData> = vec![];

    let mut peak_response = match client
        .get("https://listingmanager.nyse.com/api/corpax/")
        .query(&NyseRequest::new(date.date_naive(), 7, 1, 1))
        .send()
        .await
    {
        Ok(ok) => ok,
        Err(error) => {
            println!(
                "Error while loading data from NYSE (https://listingmanager.nyse.com/api/corpax/)."
            );
            if let Some(x) = error.url() {
                println!("Issued query one page: {}", x);
            }
            return Err(Box::new(error));
        }
    }
    .text()
    .await?;
    let mut peak_response: NysePeekResponse = match serde_json::from_str(&peak_response) {
        Ok(ok) => ok,
        Err(error) => {
            println!("Failed to parse response: {}", &peak_response);
            return Err(Box::new(error));
        }
    };

    // let mut peak_response: NysePeekResponse = match match client
    //     .get("https://listingmanager.nyse.com/api/corpax/")
    //     .query(&NyseRequest::new(date.date_naive(), 7, 1, 1))
    //     .send()
    //     .await
    // {
    //     Ok(ok) => ok,
    //     Err(error) => {
    //         println!(
    //             "Error while loading data from NYSE (https://listingmanager.nyse.com/api/corpax/)."
    //         );
    //         if let Some(x) = error.url() {
    //             println!("Issued query one page: {}", x);
    //         }
    //         return Err(Box::new(error));
    //     }
    // }
    // .json()
    // .await
    // {
    //     Ok(ok) => ok,
    //     Err(error) => {
    //         println!(
    //             "Error while parsing json data from NYSE (https://listingmanager.nyse.com/api/corpax/).
    //             Content: {}", error.to_string()
    //         );

    //         return Err(Box::new(error));
    //     }
    // };
    let peak_count = peak_response.count.get_or_insert(0);

    let pages_available: u32 = (*peak_count as f32 / max_page_size as f32).ceil() as u32;
    let list_of_pages: Vec<u32> = (1..=pages_available).collect();
    for page in list_of_pages {
        let mut response = client
            .get("https://listingmanager.nyse.com/api/corpax/")
            // .query(&build_request(date, 7, max_page_size, page))
            .query(&NyseRequest::new(date.date_naive(), 7, page, max_page_size))
            .send()
            .await?
            .json::<NyseResponse>()
            .await?;
        if response.count > 0 {
            output.append(&mut response.results);
        }
    }
    Ok(output)
}
