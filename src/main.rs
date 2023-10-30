#![allow(dead_code)]
use chrono::{Days, NaiveDate, Utc};
use data_collector::db;
use data_collector::nyse::nyse::{latest_date_available, load_missing_week, NyseData};
use data_collector::{collect_config::collect_conf, configuration};
use reqwest::Client;
use sqlx::postgres::{PgArguments, PgQueryResult};
use sqlx::query::Query;
use sqlx::types::Uuid;
use sqlx::{Connection, Executor, PgConnection, PgPool, Postgres, QueryBuilder};
use std::error::{self, Error};

struct testRow {
    id: i32,
    text: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "https://listingmanager.nyse.com/api/corpax/";
    // let tasks = collect_conf::load_file("configs/config.yaml").unwrap();

    let mut configuration = configuration::get_configuration().unwrap();
    configuration.database.database_name = "newsletter".to_string();

    let connection_pool = PgPool::connect_with(configuration.database.with_db())
        .await
        .expect("Failed to connect to Postgres.");

    load_and_store_missing_data(url, &connection_pool).await?;

    println!("done");
    Ok(())
}

pub async fn load_and_store_missing_data<'a, 'b>(
    url: &str,
    connection_pool: &sqlx::Pool<Postgres>,
) -> Result<(), Box<dyn error::Error>> {
    let now = Utc::now();
    let mut latest_date = latest_date_available();
    let client = Client::new();
    let mut query_builder: QueryBuilder<Postgres> =
    sqlx::QueryBuilder::new("insert into nyse_events(action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event) ");
    while latest_date < now {
        let mut week_data = load_missing_week(&client, &latest_date, url).await?;

        // let t1 = testRow {
        //     id: 1,
        //     text: Some("ab".to_string()),
        // };
        // let t2 = testRow {
        //     id: 2,
        //     text: Some("abc".to_string()),
        // };

        // let v = vec![t1, t2];

        // let (num, ttt): (Vec<i32>, Vec<Option<String>>) =
        //     v.into_iter().map(|exp| (exp.id, exp.text)).fold(
        //         (Vec::new(), Vec::new()),
        //         |(mut v_num, mut v_ttt), (num, ttt)| {
        //             v_num.push(num);
        //             v_ttt.push(ttt);
        //             (v_num, v_ttt)
        //         },
        //     );

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

        //         let f = sqlx::query!("INSERT INTO nyse_events
        // (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event, is_staged)
        // VALUES('2015-05-05', '', '', '', '', '', '', false);");

        sqlx::query!("INSERT INTO nyse_events
                    (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event)
                    Select * from UNNEST ($1::date[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])  on conflict (action_date, issue_symbol, issuer_name, market_event, is_staged) do NOTHING",
                &action_date[..],
                &action_status[..],
                &action_type[..],
                &issue_symbol[..],
                &issuer_name[..],
                &updated_at[..],
                &market_event[..],
            ).execute(connection_pool)
            .await.unwrap();

        // sqlx::query!(
        //     "INSERT INTO students(name, age) SELECT * FROM UNNEST($1::text[], $2::int8[])",
        //     &names[..],
        //     &ages[..]
        // )
        // .execute(&pool)
        // .await
        // .unwrap();

        //     let (action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event) =
        // week_data.iter_mut().map(|NyseData { action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event }|
        //      {
        //         (action_date.unwrap(), action_date.unwrap(), action_status.unwrap(), action_type, issue_symbol.unwrap(), issuer_name.unwrap(), updated_at, market_event)
        //     });

        //     println!("Week data found: {}", week_data.len());
        //     query_builder.push_values(week_data.into_iter(), |mut b, nyse_data| {
        //         let action_date = nyse_data.action_date.unwrap_or("2015-01-01".to_string());
        //         let action_date = NaiveDate::parse_from_str(&action_date, "%Y-%m-%d").unwrap();
        //         b
        //             .push_bind(action_date)
        //             .push_bind(nyse_data.action_status)
        //             .push_bind(nyse_data.action_type)
        //             .push_bind(nyse_data.issue_symbol)
        //             .push_bind(nyse_data.issuer_name)
        //             .push_bind(nyse_data.updated_at)
        //             .push_bind(nyse_data.market_event);
        //     });
        //     query_builder
        //         .separated("")
        //         .push_unseparated(" on conflict do NOTHING");
        //     let query = query_builder.build();

        //     query.execute(connection_pool).await?;
        //     query_builder.reset();
        //     latest_date = latest_date
        //         .checked_add_days(Days::new(7))
        //         .expect("Date should never leave the allowed range.");
    }
    Ok(())
}

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
