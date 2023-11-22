use serde::de;

use crate::utils::errors::Result;

pub fn my_test() {
    println!("test");
}

pub fn parse_response<'a, T: de::Deserialize<'a>>(response: &'a str) -> Result<T> {
    let peak_response = match serde_json::from_str(response) {
        Ok(ok) => ok,
        Err(error) => {
            tracing::error!(
                "Failed to parse line {} in column {} for response: {}",
                error.column(),
                error.line(),
                response
            );
            return Err(Box::new(error));
        }
    };
    Ok(peak_response)
}

pub fn pages_available(items_available: u32, page_size: u32) -> u32 {
    (items_available as f32 / page_size as f32).ceil() as u32
}
