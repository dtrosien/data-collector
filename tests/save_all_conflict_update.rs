use data_collector::configuration::{get_configuration, DatabaseSettings};
use data_collector::database::polygon_dividends_service::{PolygonDividendsEntry, PolygonDividendsService};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use sqlx::types::Uuid;
use chrono::NaiveDate;

async fn configure_database(config: &DatabaseSettings) -> PgPool {
    // Create database
    let mut connection = PgConnection::connect_with(&config.without_db())
        .await
        .expect("Failed to connect to Postgres");
    connection
        .execute(format!("CREATE DATABASE \"{}\";", config.database_name).as_str())
        .await
        .expect("Failed to create database.");

    // Migrate database
    let connection_pool = PgPool::connect_with(config.with_db())
        .await
        .expect("Failed to connect to Postgres.");
    sqlx::migrate!("./migrations")
        .run(&connection_pool)
        .await
        .expect("Failed to migrate the database");

    connection_pool
}

#[tokio::test]
async fn save_all_conflict_updates() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare configuration with isolated database
    let mut configuration = get_configuration().expect("Failed to read configuration.");
    configuration.database.database_name = Uuid::new_v4().to_string();
    let pool = configure_database(&configuration.database).await;

    let service = PolygonDividendsService::new(pool.clone());

    let ex_date = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();

    // First insert
    let entry1 = PolygonDividendsEntry {
        ticker: "TSTUPD".to_string(),
        record_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
        pay_date: Some(NaiveDate::from_ymd_opt(2020, 1, 3).unwrap()),
        declaration_date: None,
        ex_dividend_date: ex_date,
        frequency: None,
        cash_amount: 1.23,
        currency: "USD".to_string(),
        distribution_type: None,
        historical_adjustment_factor: None,
        split_adjusted_cash_amount: None,
        is_staged: false,
    };

    service.save_all(vec![entry1]).await?;

    let rec = sqlx::query!(
        "SELECT cash_amount, pay_date FROM polygon_dividends WHERE ticker = $1 AND ex_dividend_date = $2",
        "TSTUPD",
        ex_date
    )
    .fetch_one(&pool)
    .await?;

    // cash_amount is numeric -> BigDecimal in sqlx; convert to f64 for assertion
    let cash1: f64 = rec.cash_amount.to_string().parse()?;
    assert!((cash1 - 1.23).abs() < f64::EPSILON);

    // Now update same key with new cash_amount and pay_date
    let entry2 = PolygonDividendsEntry {
        ticker: "TSTUPD".to_string(),
        record_date: Some(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
        pay_date: Some(NaiveDate::from_ymd_opt(2020, 2, 3).unwrap()),
        declaration_date: None,
        ex_dividend_date: ex_date,
        frequency: None,
        cash_amount: 2.34,
        currency: "USD".to_string(),
        distribution_type: None,
        historical_adjustment_factor: None,
        split_adjusted_cash_amount: None,
        is_staged: true,
    };

    service.save_all(vec![entry2]).await?;

    let rec2 = sqlx::query!(
        "SELECT cash_amount, pay_date, is_staged FROM polygon_dividends WHERE ticker = $1 AND ex_dividend_date = $2",
        "TSTUPD",
        ex_date
    )
    .fetch_one(&pool)
    .await?;

    // cash_amount is numeric -> BigDecimal in sqlx; convert to f64 for assertion
    let cash2: f64 = rec2.cash_amount.to_string().parse()?;
    assert!((cash2 - 2.34).abs() < f64::EPSILON);
    assert_eq!(rec2.is_staged, true);

    Ok(())
}
