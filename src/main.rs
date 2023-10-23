mod nyse;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nyse_records = nyse::nyse::load_and_store_missing_data().await;
    println!("Loaded {} records.", nyse_records.unwrap().len());
    print!("done{}", "");
    Ok(())
}
