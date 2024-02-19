use data_collector::collectors::collector_sources::CollectorSource;

use data_collector::collectors::sp500_fields::Fields;
use data_collector::configuration::{get_configuration, DatabaseSettings, TaskSetting};
use data_collector::startup::Application;
use data_collector::tasks::actions::action::ActionType::Collect;
use data_collector::utils::telemetry::{get_subscriber, init_subscriber};
use rand::Rng;
use sqlx::types::Uuid;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::OnceLock;

// Ensure that the `tracing` stack is only initialised once using `once_cell`
// to enable logs in tests start test with "TEST_LOG=true cargo test"
fn init_tracing() {
    static TRACING: OnceLock<()> = OnceLock::new();
    TRACING.get_or_init(|| {
        let default_filter_level = "info".to_string();
        let subscriber_name = "test".to_string();

        if std::env::var("TEST_LOG").is_ok_and(|x| x.contains("true")) {
            let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::stdout);
            init_subscriber(subscriber);
        } else {
            let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::sink);
            init_subscriber(subscriber);
        }
    });
}

async fn spawn_app_with_test_tasks(tasks: Vec<TaskSetting>) -> Application {
    init_tracing();

    let mut configuration = get_configuration().expect("Failed to read configuration.");
    configuration.database.database_name = Uuid::new_v4().to_string();
    configuration.application.tasks = tasks;
    configure_database(&configuration.database).await;
    Application::build(configuration).await
}

pub async fn configure_database(config: &DatabaseSettings) -> PgPool {
    // Create database
    let mut connection = PgConnection::connect_with(&config.without_db())
        .await
        .expect("Failed to connect to Postgres");
    connection
        .execute(&*format!(r#"CREATE DATABASE "{}";"#, config.database_name))
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
async fn start_task() {
    // Arrange
    let tasks = vec![TaskSetting {
        name: "task_1".to_string(),
        comment: None,
        actions: vec![],
        sp500_fields: vec![],
        include_sources: vec![CollectorSource::Dummy],
        exclude_sources: vec![],
    }];

    let app = spawn_app_with_test_tasks(tasks).await;

    // Act
    let runner = app.run();

    // Assert
    assert!(runner.await.is_ok())
}

#[tokio::test]
async fn running_collect_action_on_dummmy_source() {
    // Arrange
    let num_tasks = 20;
    let base_task = TaskSetting {
        name: "task".to_string(),
        comment: None,
        actions: vec![Collect],
        sp500_fields: vec![Fields::Nyse],
        include_sources: vec![CollectorSource::Dummy],
        exclude_sources: vec![],
    };
    let mut tasks = vec![];
    let mut rng = rand::thread_rng();
    for n in 0..num_tasks {
        let mut task = base_task.clone();
        task.name = format!("task_{}", n);
        tasks.push(task);
    }

    let app = spawn_app_with_test_tasks(tasks).await;

    // Act
    let runner = app.run();

    // Assert
    assert!(runner.await.is_ok())
}
