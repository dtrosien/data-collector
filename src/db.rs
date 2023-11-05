use crate::configuration::Settings;
// use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Pool, Postgres};

// use uuid::Uuid;

pub fn create_connection_pool(configuration: &Settings) -> Pool<Postgres> {
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_lazy_with(configuration.database.without_db())
}

pub async fn insert_into(_pool: &PgPool) -> Result<(), sqlx::Error> {
    todo!("implement");
    // let id = Uuid::new_v4();
    // let email = "test";
    // let name = "test";
    // let created_at = Utc::now();

    // sqlx::query!(
    //     r#"INSERT INTO example (id,email,name,created_at) VALUES ($1,$2,$3,$4)"#,
    //     id,
    //     email,
    //     name,
    //     created_at
    // )
    // .execute(_pool)
    // .await
    // .map_err(|e| {
    //     tracing::error!("Failed to execute query: {:?}", e);
    //     e
    // })?;
    // Ok(())
}
