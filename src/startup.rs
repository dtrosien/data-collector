use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;

use crate::configuration::{
    DatabaseSettings, HttpClientSettings, Settings, TaskDependency, TaskName, TaskSetting,
};

use crate::dag_scheduler::scheduler::{TaskDependenciesSpecs, TaskSpec, TaskSpecRef};
use crate::dag_scheduler::task::{ExecutionMode, RetryOptions};
use crate::tasks::task::Task;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

pub struct Application {
    pool: PgPool,
    task_dependencies: Vec<TaskDependency>,
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
            task_dependencies: configuration.application.task_dependencies,
            task_settings: configuration.application.tasks,
            client,
        }
    }

    // #[tracing::instrument(name = "Start running tasks", skip(self))]
    // pub async fn run(&self) -> Result<(), anyhow::Error> {
    //     let scheduler = Scheduler::build(&self.task_settings, &self.pool, &self.client);
    //     let results = scheduler.build_execution_sequence().run_all().await;
    //     // todo handle and log errors etc
    //     results.into_iter().try_for_each(|r| r)?;
    //     Ok(())
    // }

    #[tracing::instrument(name = "Start running tasks", skip(self))]
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let mut task_spec_refs = HashMap::new();

        // create task refs
        for ts in self.task_settings.iter() {
            let task_name: TaskName = ts.name.clone();

            let runner = Arc::new(Task::new(ts, &self.pool, &self.client));

            let task_spec = TaskSpec {
                id: Uuid::new_v4(),
                name: task_name.clone(),
                retry_options: RetryOptions::default(),
                execution_mode: ExecutionMode::Once,
                tools: Arc::new(Default::default()),
                runnable: runner,
            };
            let task_spec_ref: TaskSpecRef = TaskSpecRef::from(task_spec);
            task_spec_refs.insert(task_name, task_spec_ref);
        }
        let mut tasks_specs: TaskDependenciesSpecs = HashMap::new();
        task_spec_refs.values().for_each(|v| {
            let deps: Vec<TaskSpecRef> = self
                .task_dependencies
                .iter()
                .filter_map(|k| task_spec_refs.get(&k.name))
                .cloned()
                .collect();
            tasks_specs.insert(v.clone(), deps);
        });

        // let deps: Vec<TaskSpecRef> = self
        //     .task_dependencies
        //     .iter()
        //     .map(|k| task_spec_refs.get(&k.name).unwrap().clone())
        //     .collect();

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
