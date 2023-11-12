use chrono::Utc;
use data_collector::collectors::collector_sources::CollectorSource;
use data_collector::collectors::collector_sources::CollectorSource::All;
use data_collector::collectors::sp500_fields::Fields;
use data_collector::configuration::{get_configuration, DatabaseSettings, TaskSetting};
use data_collector::tasks::actions::ActionType::Collect;
use data_collector::tasks::build_task_prio_queue;
use data_collector::utils::telemetry::{get_subscriber, init_subscriber};
use rand::Rng;
use sqlx::types::Uuid;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::OnceLock;

mod common;

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

pub struct TestApp {
    pub db_pool: PgPool,
}

// Launch the application in the background
async fn spawn_app() -> TestApp {
    init_tracing();

    let mut configuration = get_configuration().expect("Failed to read configuration.");
    configuration.database.database_name = Uuid::new_v4().to_string();

    let connection_pool = configure_database(&configuration.database).await;

    TestApp {
        db_pool: connection_pool,
    }
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
async fn write_to_db() {
    // Arrange
    let app = spawn_app().await;

    let id = Uuid::new_v4();
    let email = "test";
    let name = "test";
    let created_at = Utc::now();
    // Act
    sqlx::query!(
        r#"INSERT INTO example (id,email,name,created_at) VALUES ($1,$2,$3,$4)"#,
        id,
        email,
        name,
        created_at
    )
    .execute(&app.db_pool)
    .await
    .expect("Failed to write to DB.");

    // Assert
    let saved = sqlx::query!("SELECT email, name FROM example",)
        .fetch_one(&app.db_pool)
        .await
        .expect("Failed to fetch saved data.");

    assert_eq!(saved.email, email);
    assert_eq!(saved.name, name);
}

#[tokio::test]
async fn start_task() {
    // Arrange
    let app = spawn_app().await;

    let task_setting = TaskSetting {
        comment: None,
        actions: vec![],
        sp500_fields: vec![],
        priority: 500,
        include_sources: vec![CollectorSource::NyseEvents],
        exclude_sources: vec![],
    };

    let task_settings = vec![task_setting];

    // Act
    let runner = data_collector::startup::run(app.db_pool, &task_settings);

    // Assert
    assert!(runner.await.is_ok())
}

#[tokio::test]
async fn partition_pruning_enabled() {
    let app = spawn_app().await;
    let saved = sqlx::query!("SHOW enable_partition_pruning")
        .fetch_one(&app.db_pool)
        .await
        .expect("Failed to fetch config of database.");
    assert_eq!("on", saved.enable_partition_pruning.unwrap());
}

#[tokio::test]
async fn prio_queue_order_and_filter() {
    // Arrange
    let app = spawn_app().await;
    let priorities_in = vec![1, 500, 300, 1000, -2, 90, -20];
    let priorities_out = vec![1000, 500, 300, 90, 1];
    let base_task = TaskSetting {
        comment: None,
        actions: vec![Collect],
        sp500_fields: vec![Fields::Nyse],
        priority: 0,
        include_sources: vec![All],
        exclude_sources: vec![],
    };
    let mut tasks = vec![];
    for n in priorities_in {
        let mut task = base_task.clone();
        task.priority = n;
        tasks.push(task);
    }

    // Act
    let mut queue = build_task_prio_queue(&tasks, &app.db_pool).await;

    // Assert
    for n in priorities_out {
        assert_eq!(queue.pop().unwrap().get_priority(), n)
    }
}

#[tokio::test]
async fn running_dummy_action() {
    // Arrange
    let app = spawn_app().await;
    let num_tasks = 200;
    let base_task = TaskSetting {
        comment: None,
        actions: vec![Collect],
        sp500_fields: vec![Fields::Nyse],
        priority: 0,
        include_sources: vec![All],
        exclude_sources: vec![],
    };
    let mut tasks = vec![];
    let mut rng = rand::thread_rng();
    for _n in 0..num_tasks {
        let mut task = base_task.clone();
        task.priority = rng.gen();
        tasks.push(task);
    }

    // Act
    let runner = data_collector::startup::run(app.db_pool, &tasks);

    // Assert
    assert!(runner.await.is_ok())
}
