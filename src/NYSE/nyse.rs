use chrono::prelude::*;
use chrono::DateTime;
use chrono::Days;
use chrono::Duration;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use reqwest::header::HOST;
use reqwest::Client;
use reqwest::Error;
use reqwest::RequestBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Default, Deserialize, Debug)]
pub struct NYSE_response {
    pub count: i32,
    pub next: Option<String>,
    pub previous: Option<String>,
    pub results: Vec<NYSE_data>,
}

#[derive(Default, Deserialize, Debug)]
pub struct NYSE_data {
    pub action_date: String,
    pub action_status: String,
    pub action_type: String,
    pub issue_symbol: String,
    pub issuer_name: String,
    pub updated_at: String,
    pub market_event: String,
}

fn latest_date_available() -> DateTime<Utc> {
    //TODO: Get latest date from database
    // let p = Utc.with_ymd_and_hms(2015, 12, 7, 0, 0, 0).unwrap();
    let p = Utc.with_ymd_and_hms(2023, 10, 9, 0, 0, 0).unwrap();

    p
}

pub async fn load_and_store_missing_data() -> Vec<NYSE_data> {
    let now = Utc::now();
    let mut latest_date = latest_date_available();
    let mut data: Vec<NYSE_data> = Vec::new();
    dbg!(&now);
    dbg!(&latest_date);
    while latest_date < now {
        println!("While was entered.");
        data.append(&mut load_missing_week(latest_date).await.unwrap());
        println!("Data size: {}", &data.len());
        latest_date = latest_date.checked_add_days(Days::new(7)).unwrap();
    }
    data
    // }
}

async fn load_missing_week(date: DateTime<Utc>) -> Result<Vec<NYSE_data>, Error> {
    let client = Client::new();
    let mut result = client
        .get("https://listingmanager.nyse.com/api/corpax/")
        .query(&build_request(date, 7))
        .send()
        .await?
        .json::<NYSE_response>()
        .await?;

    Ok(result.results)
}

fn build_request(start_date: DateTime<Utc>, days: u64) -> HashMap<String, String> {
    let end_date = start_date
        .date_naive()
        .checked_add_days(Days::new(days - 1))
        .unwrap();

    let mut header_map = HashMap::new();
    header_map.insert(
        String::from("action_date__gte"),
        start_date.date_naive().to_string(),
    );
    header_map.insert(String::from("action_date__lte"), end_date.to_string());
    header_map.insert(String::from("page"), String::from("1"));
    header_map.insert(String::from("page_size"), String::from("100"));
    header_map
}
