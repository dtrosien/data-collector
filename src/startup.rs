use reqwest::Client;

use crate::configuration::{DatabaseSettings, HttpClientSettings, Settings, TaskSetting};
use crate::scheduler::schedule_tasks;
use crate::tasks::task::execute_task;
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::task::JoinHandle;

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

    /// runs groups of tasks.
    /// while groups are processed in sequence, all tasks inside a group are running concurrently
    ///
    /// if one tasks failed, all other tasks will still processed, but overall the function will return an error
    #[tracing::instrument(name = "Start running tasks", skip(self))]
    pub async fn run(&self) -> Result<()> {
        let schedule = schedule_tasks(&self.task_settings, &self.pool, &self.client);

        let mut results = Vec::new();
        for task_group in schedule {
            let batch: Vec<JoinHandle<Result<()>>> =
                task_group.into_iter().map(execute_task).collect();
            let batch_result = join_handle_results(batch).await;

            results.push(batch_result)
        }

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
