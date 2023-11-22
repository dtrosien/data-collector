use serde::de;

use crate::utils::errors::Result;

/// Try to interpret the string as .json and parse it into the deserializable struct given as generic parameter.
/// On failure the input will be displayed along with the failed line/column as error.
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

/// Calculates the number of pages needed to request all items.
pub fn pages_available(items_available: u32, page_size: u32) -> u32 {
    (items_available as f32 / page_size as f32).ceil() as u32
}
