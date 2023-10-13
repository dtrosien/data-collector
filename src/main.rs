#![allow(dead_code)]
use collect_config::collect_conf::collect_conf;
use config::{Config, FileFormat};
use config::{File, Map, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

mod collect_config;

#[derive(Serialize, Deserialize)]
pub struct my_config {
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

#[derive(Default, Deserialize, Debug)]
pub struct TestConfig<'a> {
    database_connection_string: String,
    tasks: &'a str,
}

#[derive(Deserialize, Debug)]
pub struct Comments<'a> {
    comment: &'a str,
}

fn main() -> Result<(), Box<dyn Error>> {
    collect_conf::load_file("configs/config.yaml");
    // println!("{}", say_hello());
    // let settings = Config::builder()
    //     .add_source(config::File::with_name("configs/config.yaml"))
    //     .build()
    //     .unwrap();
    // settings
    //     .get::<String>("database_connection_string")
    //     .unwrap();
    // let array_real_content = settings.get_array("tasks").unwrap();
    // println!("length real config: {}", array_real_content.len());

    // let t = Config::builder()
    //     .set_default("test", "a")?
    //     .add_source(File::new("configs/test.yaml", FileFormat::Yaml))
    //     .build()
    //     .unwrap();
    // let result: String = t.get("database_connection_string").unwrap();

    // let result2: Vec<Value> = t.get_array("tasks").unwrap();
    // println!("{}", result2.len());
    // for k in result2 {
    //     println!("{}", k);
    //     println!(
    //         "{}",
    //         k.into_table().unwrap().get("test").unwrap_or(&Value::new(
    //             Option::Some(&"origin".to_string()),
    //             "myDefaultValue"
    //         ))
    //     );
    // }

    Ok(())
}

fn say_hello() -> String {
    "Hello, world!".to_string()
}

#[cfg(test)]
mod tests {
    use crate::say_hello;

    #[test]
    fn say_hello_test() {
        assert_eq!(say_hello(), format!("Hello, world!"))
    }
}
