use reqwest::Client;

use crate::configuration::{DatabaseSettings, HttpClientSettings, Settings, TaskSetting};

use crate::scheduler::Scheduler;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub struct Application {
    pool: PgPool,
    task_settings: Vec<TaskSetting>,
    client: Client,
}

impl Application {
    pub async fn build(configuration: Settings) -> Self {
        let connection_pool = get_connection_pool(&configuration.database);
        connection_pool.set_connect_options(configuration.database.with_db());
        let client = build_http_client(configuration.application.http_client);
        Application {
            pool: connection_pool,
            task_settings: configuration.application.tasks,
            client,
        }
    }

    #[tracing::instrument(name = "Start running tasks", skip(self))]
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let scheduler = Scheduler::build(&self.task_settings, &self.pool, &self.client);
        let results = scheduler.build_execution_sequence().run_all().await;
        // todo handle and log errors etc
        results.into_iter().try_for_each(|r| r)?;
        Ok(())
    }
}

pub fn get_connection_pool(configuration: &DatabaseSettings) -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_lazy_with(configuration.with_db())
}

pub fn build_http_client(configuration: HttpClientSettings) -> Client {
    Client::builder()
        .timeout(configuration.timeout())
        .build()
        .expect("Error building http client")
}
