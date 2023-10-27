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
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "https://listingmanager.nyse.com/api/corpax/";
    let tasks = collect_conf::load_file("configs/config.yaml").unwrap();

    let mut configuration = configuration::get_configuration().unwrap();
    configuration.database.database_name = "newsletter".to_string();

    let connection_pool = PgPool::connect_with(configuration.database.with_db())
        .await
        .expect("Failed to connect to Postgres.");
    let p: Query<'_, Postgres, PgArguments> = sqlx::query("select * from test");

    let test_data = vec![
        testRow {
            id: 4,
            text: "H1".to_string(),
        },
        testRow {
            id: 5,
            text: "H2".to_string(),
        },
    ];
    let mut queryBuidler: QueryBuilder<Postgres> =
        sqlx::QueryBuilder::new("insert into test(id, text) ");
    let a: &mut QueryBuilder<'_, Postgres> =
        queryBuidler.push_values(test_data.into_iter(), |mut b, test| {
            b.push_bind(test.id).push_bind(test.text);
        });
    let query = queryBuidler.build();

    query.execute(&connection_pool).await.expect("msg2");
    load_and_store_missing_data(url, &connection_pool).await?;
    let p: Query<'_, Postgres, PgArguments> = sqlx::query("select * from test");

    // p.execute(executor)
    println!("done");
    Ok(())
}

pub async fn load_and_store_missing_data<'a, 'b>(
    url: &str,
    connection_pool: &sqlx::Pool<Postgres>,
) -> Result<(), Box<dyn error::Error>> {
    let now = Utc::now();
    let mut latest_date = latest_date_available();
    let mut data: Vec<NyseData> = Vec::new();
    let client = Client::new();
    let mut queryBuidler: QueryBuilder<Postgres> =
    sqlx::QueryBuilder::new("insert into nyse_events(action_date, action_status, action_type, issue_symbol, issuer_name, updated_at, market_event) ");
    while latest_date < now {
        let week_data = load_missing_week(&client, &latest_date, url).await?;
        println!("Week data found: {}", week_data.len());
        queryBuidler.push_values(week_data.into_iter(), |mut b, nyse_data| {
            let action_date = nyse_data.action_date.unwrap_or("2015-01-01".to_string());
            let action_date = NaiveDate::parse_from_str(&action_date, "%Y-%m-%d").unwrap();
            let queryBuidler: &mut sqlx::query_builder::Separated<'_, '_, Postgres, &str> = b
                .push_bind(action_date)
                .push_bind(nyse_data.action_status)
                .push_bind(nyse_data.action_type)
                .push_bind(nyse_data.issue_symbol)
                .push_bind(nyse_data.issuer_name)
                .push_bind(nyse_data.updated_at)
                .push_bind(nyse_data.market_event);
        });
        queryBuidler
            .separated("")
            .push_unseparated(" on conflict do NOTHING");
        println!("Query: {}", queryBuidler.sql());
        let query = queryBuidler.build();
        query.execute(connection_pool).await?;
        queryBuidler.reset();
        latest_date = latest_date
            .checked_add_days(Days::new(7))
            .expect("Date should never leave the allowed range.");
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
