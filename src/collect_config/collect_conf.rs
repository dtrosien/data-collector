use config::Config;
use serde::Deserialize;
use std::io;

#[derive(Deserialize)]
struct MyConfig {
    database_connection_string: String,
    database_user: String,
    database_pw: String,
    tasks: Vec<LoadTask>,
}

#[derive(Default, Deserialize)]
struct LoadTask {
    comment: String,
    database_connection_string: Option<String>,
    database_user: Option<String>,
    database_pw: Option<String>,
    sp500_fields: Vec<String>,
    priority: f64,
    include_sources: Vec<String>,
    exclude_sources: Vec<String>,
}

pub struct Task {
    pub comment: String,
    pub database_connection_string: String,
    pub database_user: String,
    pub database_pw: String,
    pub sp500_fields: Vec<String>,
    pub priority: f64,
    pub include_sources: Vec<String>,
    pub exclude_sources: Vec<String>,
}

pub fn load_file(path: &str) -> Result<Vec<Task>, io::Error> {
    println!("Loading file {}....", path);
    let settings = Config::builder()
        .add_source(config::File::with_name(path))
        .build()
        .unwrap()
        .try_deserialize::<MyConfig>()
        .unwrap();

    let tasks = settings.tasks;

    let result: Vec<Task> = tasks
        .into_iter()
        .map(|task| Task {
            comment: task.comment,
            sp500_fields: task.sp500_fields,
            priority: task.priority,
            include_sources: task.include_sources,
            exclude_sources: task.exclude_sources,
            database_connection_string: task
                .database_connection_string
                .unwrap_or(settings.database_connection_string.clone()),
            database_user: task.database_user.unwrap_or(settings.database_user.clone()),
            database_pw: task.database_pw.unwrap_or(settings.database_pw.clone()),
        })
        .collect();

    Ok(result)
}
