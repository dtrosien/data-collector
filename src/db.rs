use crate::configuration::Settings;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

struct PgConfig {}

pub fn create_connection_pool(configuration: &Settings) -> Pool<Postgres> {
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_lazy_with(configuration.database.without_db())
}
