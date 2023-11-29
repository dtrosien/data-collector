use crate::{
    collectors::{self, collector_sources, sp500_fields, Collector},
    utils::errors::Result,
};
use async_trait::async_trait;
use chrono::{DateTime, Days, Duration, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::{
    fmt::Display,
    fs::{self, File},
    io::{Bytes, Cursor},
    ops::Add,
    path::Path,
    time::SystemTime,
    u8,
};
use std::{fs::rename, io::copy};
use tempfile::Builder;
use tracing::debug;

use crate::tasks::runnable::Runnable;

const download_target: &str = "https://www.rust-lang.org/logos/rust-logo-512x512.png";

#[derive(Clone)]
pub struct SecCompanyCollector {
    pool: PgPool,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
pub struct exchanges {
    tickers: Vec<Option<String>>,
    exchanges: Vec<Option<String>>,
}

impl SecCompanyCollector {
    pub fn new(pool: PgPool) -> Self {
        SecCompanyCollector { pool }
    }
}

impl Display for SecCompanyCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecCompanyCollector struct.")
    }
}

#[async_trait]
impl Runnable for SecCompanyCollector {
    async fn run(&self) -> Result<()> {
        load_and_store_missing_data(self.pool.clone()).await
    }
}

impl Collector for SecCompanyCollector {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        vec![sp500_fields::Fields::Nyse]
    }

    fn get_source(&self) -> collector_sources::CollectorSource {
        collector_sources::CollectorSource::SecCompanies
    }
}

pub async fn load_and_store_missing_data(connection_pool: PgPool) -> Result<()> {
    load_and_store_missing_data_with_target(connection_pool, download_target).await
}

pub async fn load_and_store_missing_data_with_target(
    connection_pool: PgPool,
    url: &str,
) -> Result<()> {
    let target_location = "/home/telijas/submissions.zip";
    let metadata = fs::metadata(target_location)?;
    let modification_date: DateTime<Utc> = metadata.modified().unwrap().into();
    if modification_date.checked_add_days(Days::new(7)).unwrap() < Utc::now() {
        debug!("Downloading {}", url);
        download_url(url, target_location).await?;
    }
    let file = File::open(target_location)?;
    let mut zip = zip::ZipArchive::new(file)?;
    for i in 0..10000 {
        let mut file = zip.by_index(i)?;
        if !file.name().contains("submission") {
            let mut buffer: Vec<u8> = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            std::io::copy(&mut file, &mut cursor);
            let v = &cursor.into_inner();
            let output = String::from_utf8_lossy(v).to_string();
            // println!("output {}", output);
            let infos: exchanges = collectors::utils::parse_response(&output)?;
            if infos.exchanges.len() > 0 {
                println!("Filename: {}", file.name());
                println!("Found: {:?}", infos);
            }
        }
    }

    Ok(())
}

async fn download_url(url: &str, destination: &str) -> Result<()> {
    let response = reqwest::get(url).await?;
    let mut content = Cursor::new(response.bytes().await?);
    let mut target_destination = File::create(destination)?;
    copy(&mut content, &mut target_destination)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        collectors::source_apis::sec_companies::{
            download_target, load_and_store_missing_data_with_target,
        },
        utils::errors::Result,
    };
    use sqlx::{Pool, Postgres};
    use tracing_test::traced_test;

    #[traced_test]
    #[sqlx::test]
    async fn query_http_and_write_to_db(pool: Pool<Postgres>) -> Result<()> {
        load_and_store_missing_data_with_target(pool.clone(), download_target).await?;
        Ok(())
    }

    fn outdated_time_is_correctly_detected() {}

    fn file_is_only_loaded_when_outdated() {}
}
