pub mod collect_conf {
    use config::Config;
    use serde::{Deserialize, Serialize};
    use std::io::{self};

    #[derive(Deserialize)]
    struct MyConfig {
        pub database_connection_string: String,
        pub database_user: String,
        pub database_pw: String,
        pub tasks: Vec<LoadTask>,
    }

    #[derive(Default, Deserialize)]
    struct LoadTask {
        comment: String,
        database_connection_string: String,
        database_user: Option<String>,
        database_pw: Option<String>,
        sp500_fields: Vec<String>,
        priority: f32,
        include_sources: Vec<String>,
        exclude_sources: Vec<String>,
    }

    pub struct Task {
        comment: String,
        database_connection_string: String,
        database_user: String,
        database_pw: String,
        sp500_fields: Vec<String>,
        priority: f32,
        include_sources: Vec<String>,
        exclude_sources: Vec<String>,
    }

    pub fn load_file(path: &str) -> Result<Vec<Task>, io::Error> {
        println!("Loading file {}....", path);
        let result: Vec<Task> = Vec::new();
        let settings = Config::builder()
            .add_source(config::File::with_name(path))
            .build()
            .unwrap();

        let loaded_tasks = match settings.get_array("tasks") {
            Ok(tasks) => tasks,
            Err(_) => return Ok(result),
        };

        for task in loaded_tasks {
            for (k, v) in task.into_table().unwrap() {
                println!("Key: {}, Value: {}", k, v);
            }
            println!();
        }

        Ok(result)
    }
}
