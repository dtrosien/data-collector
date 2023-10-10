pub mod my_schema {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Config {
        pub database_connection_string: String,
        pub database_user: String,
        pub database_pw: String,
        pub tasks: Vec<Task>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Task {
        comment: String,
        sp500_fields: Vec<String>,
        priority: f32,
        include_sources: Vec<String>,
        exclude_sources: Vec<String>,
    }

    pub fn myTest() -> i32 {
        3
    }
}
