#![allow(dead_code)]
use collect_config::collect_conf::collect_conf;
use std::error::Error;
mod collect_config;

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
