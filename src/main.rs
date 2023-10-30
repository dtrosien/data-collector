#![allow(dead_code)]
use chrono::{Days, NaiveDate, Utc};

use data_collector::configuration;
use data_collector::nyse::nyse::{self, latest_date_available, load_missing_week};
use reqwest::Client;

use sqlx::{PgPool, Postgres, QueryBuilder};
use std::error::{self, Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let tasks = collect_conf::load_file("configs/config.yaml").unwrap();
    let mut configuration = configuration::get_configuration().unwrap();
    configuration.database.database_name = "newsletter".to_string();

    let connection_pool = PgPool::connect_with(configuration.database.with_db())
        .await
        .expect("Failed to connect to Postgres.");

    let url = "https://listingmanager.nyse.com/api/corpax/";

    // load_and_store_missing_data(url, &connection_pool).await?;
    nyse::load_and_store_missing_data(url, &connection_pool).await?;

    println!("done");
    Ok(())
}

// pub async fn load_and_store_missing_data<'a, 'b>(
//     url: &str,
//     connection_pool: &sqlx::Pool<Postgres>,
// ) -> Result<(), Box<dyn error::Error>> {
//     let now = Utc::now();
//     let mut latest_date = latest_date_available();
//     let client = Client::new();
//     while latest_date < now {
//         let week_data = load_missing_week(&client, &latest_date, url).await?;

//         let (
//             action_date,
//             action_status,
//             action_type,
//             issue_symbol,
//             issuer_name,
//             updated_at,
//             market_event,
//         ): (
//             Vec<Option<String>>,
//             Vec<String>,
//             Vec<String>,
//             Vec<String>,
//             Vec<String>,
//             Vec<String>,
//             Vec<String>,
//         ) = week_data
//             .into_iter()
//             .map(|nyse_data| {
//                 (
//                     nyse_data.action_date,
//                     nyse_data.action_status,
//                     nyse_data.action_type,
//                     nyse_data.issue_symbol,
//                     nyse_data.issuer_name,
//                     nyse_data.updated_at,
//                     nyse_data.market_event,
//                 )
//             })
//             .fold(
//                 (
//                     Vec::new(),
//                     Vec::new(),
//                     Vec::new(),
//                     Vec::new(),
//                     Vec::new(),
//                     Vec::new(),
//                     Vec::new(),
//                 ),
//                 |(
//                     mut action_dates,
//                     mut action_statuses,
//                     mut action_types,
//                     mut issue_symbols,
//                     mut issure_names,
//                     mut updated_ats,
//                     mut market_events,
//                 ),
//                  (
//                     action_date,
//                     action_status,
//                     action_type,
//                     issue_symbol,
//                     issuer_name,
//                     updated_at,
//                     market_event,
//                 )| {
//                     action_dates.push(action_date);
//                     action_statuses.push(action_status.unwrap());
//                     action_types.push(action_type);
//                     issue_symbols.push(issue_symbol.unwrap());
//                     issure_names.push(issuer_name.unwrap());
//                     updated_ats.push(updated_at);
//                     market_events.push(market_event);
//                     (
//                         action_dates,
//                         action_statuses,
//                         action_types,
//                         issue_symbols,
//                         issure_names,
//                         updated_ats,
//                         market_events,
//                     )
//                 },
//             );

//         let action_date: Vec<NaiveDate> = action_date
//             .into_iter()
//             .map(|e| e.unwrap_or("2015-01-01".to_string()))
//             .map(|f| NaiveDate::parse_from_str(&f, "%Y-%m-%d").unwrap())
//             .collect();

//         sqlx::query!("INSERT INTO nyse_events
//                     (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event)
//                     Select * from UNNEST ($1::date[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) on conflict do nothing",
//                 &action_date[..],
//                 &action_status[..],
//                 &action_type[..],
//                 &issue_symbol[..],
//                 &issuer_name[..],
//                 &updated_at[..],
//                 &market_event[..],
//             ).execute(connection_pool)
//             .await.unwrap();

//         latest_date = latest_date
//             .checked_add_days(Days::new(7))
//             .expect("Date should never leave the allowed range.");
//     }
//     Ok(())
// }

fn say_hello() -> String {
    "Hello, world!".to_string()
}

#[cfg(test)]
mod tests {
    use crate::say_hello;

    #[test]
    fn say_hello_test() {
        assert_eq!(say_hello(), format!("Hello, world!"))
    }
}
