#![allow(dead_code)]
use collect_config::collect_conf::collect_conf;
use config::FileFormat::Yaml;
use config::{Config, FileFormat};
use config::{File, Map, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
mod collect_config;

#[derive(Serialize, Deserialize)]
pub struct my_config2 {
    database_connection_string: String,
    database_user: String,
    database_pw: String,
    tasks: Vec<Task2>,
}

#[derive(Serialize, Deserialize)]
pub struct Task2 {
    comment: String,
    database_connection_string: Option<String>,
    database_user: Option<String>,
    database_pw: Option<String>,
    sp500_fields: Vec<String>,
    priority: f64,
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
    let tasks = collect_conf::load_file("configs/config.yaml").unwrap();
    println!("Connection string: {}", tasks[0].database_pw);
    println!("Connection string: {}", tasks[1].database_pw);
    println!("Connection string: {}", tasks[2].database_pw);

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
