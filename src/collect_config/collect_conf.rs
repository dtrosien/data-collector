pub mod collect_conf {
    use config::Config;
    use config::Value;
    use config::ValueKind;
    use serde::{Deserialize, Serialize};
    use std::io::{self};

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
        database_connection_string: String,
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
        let mut result: Vec<Task> = Vec::new();
        let settings = Config::builder()
            .add_source(config::File::with_name(path))
            .build()
            .unwrap();

        let mut loaded_tasks = match settings.get_array("tasks") {
            Ok(tasks) => tasks,
            Err(_) => return Ok(result),
        };

        for raw_task in loaded_tasks {
            let mut config_as_table: std::collections::HashMap<String, config::Value> =
                raw_task.into_table().unwrap();

            // config::Value::new(None, ValueKind::String(String::from("value")));
            // println!(
            //     "read comment: {}",
            //     config_as_table
            //         .remove("comment")
            //         .unwrap_or(Value::from(""))
            //         .into_string()
            //         .expect("Default already added")
            // );

            let task = Task {
                comment: config_as_table
                    .remove("comment")
                    .unwrap_or(Value::from(""))
                    .into_string()
                    .expect("Default already added"),
                database_connection_string: config_as_table
                    .remove("database_connection_string")
                    .unwrap_or(Value::from(""))
                    .into_string()
                    .expect("Default already added"),
                database_user: config_as_table
                    .remove("database_user")
                    .unwrap_or(Value::from(""))
                    .into_string()
                    .expect("Default already added"),
                database_pw: config_as_table
                    .remove("database_pw")
                    .unwrap_or(Value::from(""))
                    .into_string()
                    .expect("Default already added"),
                sp500_fields: config_as_table
                    .remove("sp500_fields")
                    .unwrap_or(Value::from(Vec::<String>::new()))
                    .into_array()
                    .expect("Default already added")
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>(),
                priority: config_as_table
                    .remove("priority")
                    .unwrap_or(Value::from(f64::MAX))
                    .into_float()
                    .expect("Default already added"),
                include_sources: config_as_table
                    .remove("include_sources")
                    .unwrap_or(Value::from(Vec::<String>::new()))
                    .into_array()
                    .expect("Default already added")
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>(),
                exclude_sources: config_as_table
                    .remove("exclude_sources")
                    .unwrap_or(Value::from(Vec::<String>::new()))
                    .into_array()
                    .expect("Default already added")
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>(),
            };
            result.push(task);
        }

        Ok(result)
    }
}
