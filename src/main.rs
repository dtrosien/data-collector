use std::collections::HashMap;

use serde::Serialize;

mod NYSE;

#[derive(Serialize)]
struct NYSE_request {
    action_date__gte: String,
    action_date__lte: String,
    page: u32,
    page_size: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let p = NYSE::nyse::load_and_store_missing_data().await;
    // dbg!(&p);
    print!("done{}", "");
    Ok(())
}
