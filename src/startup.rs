use crate::configuration::{DatabaseSettings, Settings, TaskSetting};
use crate::tasks::task::{build_task_prio_queue, execute_task};
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub struct Application {
    pool: PgPool,
    task_settings: Vec<TaskSetting>,
}

impl Application {
    pub async fn build(configuration: Settings) -> Self {
        let connection_pool = get_connection_pool(&configuration.database);
        connection_pool.set_connect_options(configuration.database.with_db());
        Application {
            pool: connection_pool,
            task_settings: configuration.application.tasks,
        }
    }

    /// runs all tasks concurrently and waits till all tasks finished,
    /// if one tasks failed, all other tasks will still processed, but overall the function will return an error
    #[tracing::instrument(name = "Start running tasks", skip(self))]
    pub async fn run(&self) -> Result<()> {
        let handles = build_task_prio_queue(&self.task_settings, &self.pool)
            .await
            .into_iter()
            .map(execute_task)
            .collect();

        join_handle_results(handles).await
    }
}

pub fn get_connection_pool(configuration: &DatabaseSettings) -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(2))
        .connect_lazy_with(configuration.with_db())
}
