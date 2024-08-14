use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;

use crate::configuration::{
    DatabaseSettings, HttpClientSettings, SecretKeys, Settings, TaskDependency, TaskName,
    TaskSetting,
};

use crate::actions::action::create_action;
use crate::dag_schedule::schedule::{Schedule, TaskDependenciesSpecs, TaskSpec, TaskSpecRef};
use crate::dag_schedule::task::{ExecutionMode, RetryOptions};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub struct Application {
    pool: PgPool,
    task_dependencies: Vec<TaskDependency>,
    task_settings: Vec<TaskSetting>,
    client: Client,
    secrets: Option<SecretKeys>,
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
            secrets: configuration.application.secrets,
        }
    }

    #[allow(clippy::mutable_key_type)]
    #[tracing::instrument(name = "Run application", skip(self))]
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        // init specs from config
        let task_specs = build_task_specs(
            &self.task_settings,
            &self.task_dependencies,
            &self.pool,
            &self.client,
            &self.secrets,
        );

        // build adj list from specs
        let task_dep_specs = add_dependencies_to_task_specs(task_specs, &self.task_dependencies);

        // schedule, check resulting dag and run
        let mut schedule = Schedule::new();
        schedule.schedule_tasks(task_dep_specs).await;
        schedule.run_checks().await;
        schedule.run_schedule().await;
        Ok(())
    }
}

fn build_task_specs(
    task_settings: &[TaskSetting],
    task_dependencies: &[TaskDependency],
    pool: &PgPool,
    client: &Client,
    secrets: &Option<SecretKeys>,
) -> HashMap<TaskName, TaskSpecRef> {
    let required_tasks: Vec<TaskName> = task_dependencies.iter().map(|t| t.name.clone()).collect();

    task_settings
        .iter()
        // only tasks in the dependency list shall be executed
        .filter(|ts| required_tasks.contains(&ts.name))
        .map(|ts| {
            let task_name: TaskName = ts.name.clone();
            let action = create_action(&ts.task_type, pool, client, secrets);
            let task_spec = TaskSpec::new(
                task_name.clone(),
                RetryOptions::default(),
                ExecutionMode::Once,
                Arc::new(Default::default()),
                action,
            );
            let task_spec_ref: TaskSpecRef = TaskSpecRef::from(task_spec);
            (task_name, task_spec_ref)
        })
        .collect()
}

#[allow(clippy::mutable_key_type)]
fn add_dependencies_to_task_specs(
    task_specs_map: HashMap<TaskName, TaskSpecRef>,
    task_dependencies: &[TaskDependency],
) -> TaskDependenciesSpecs {
    task_specs_map
        .values()
        .map(|task_specs_ref| {
            let deps: Vec<TaskSpecRef> = task_dependencies
                .iter()
                .filter(|task_dependency| task_dependency.name == task_specs_ref.name)
                .flat_map(|task_dependency| &task_dependency.dependencies)
                .filter_map(|task_name| task_specs_map.get(task_name))
                .cloned()
                .collect();
            (task_specs_ref.clone(), deps)
        })
        .collect()
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
