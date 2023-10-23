mod nyse;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nyse_records = nyse::nyse::load_and_store_missing_data().await;
    if let Ok(x) = nyse_records {
        println!("Loaded {} records.", x.len());
    }
    print!("done{}", "");
    Ok(())
}
