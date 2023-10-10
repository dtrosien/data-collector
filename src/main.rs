use serde::{Deserialize, Serialize};
use std::{env, fs};

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

fn main() {
    println!("{}", say_hello());
    let args: Vec<String> = env::args().collect();
    dbg!(args[1].to_string());
    let json = fs::read_to_string(&args[1]).expect("Error while reading config files");
    let p: self::Config = serde_json::from_str(&json).expect("msg");
    println!("database: {}", p.database_connection_string);
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
