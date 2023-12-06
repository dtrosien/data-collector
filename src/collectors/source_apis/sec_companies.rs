use crate::{
    collectors::{self, collector_sources, sp500_fields, Collector},
    utils::errors::Result,
};
use async_trait::async_trait;
use chrono::{DateTime, Days, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::io::{copy, Seek, Write};
use std::{
    fmt::Display,
    fs::{self, File},
    io::Cursor,
    u8,
};
use zip::ZipArchive;

use tracing::debug;

use crate::tasks::runnable::Runnable;

const DOWNLOAD_TARGET: &str = "https://www.rust-lang.org/logos/rust-logo-512x512.png";

#[derive(Clone)]
pub struct SecCompanyCollector {
    pool: PgPool,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecCompany {
    cik: String,
    sic: String,
    name: String,
    tickers: Vec<Option<String>>,
    exchanges: Vec<Option<String>>,
    state_of_incorporation: String,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TransposedSecCompany {
    pub cik: Vec<i32>,
    pub sic: Vec<Option<i32>>,
    pub name: Vec<String>,
    pub tickers: Vec<String>,
    pub exchanges: Vec<Option<String>>,
    pub state_of_incorporation: Vec<String>,
}

impl TransposedSecCompany {
    ///Creates empty object
    fn new() -> TransposedSecCompany {
        TransposedSecCompany {
            cik: vec![],
            sic: vec![],
            name: vec![],
            tickers: vec![],
            exchanges: vec![],
            state_of_incorporation: vec![],
        }
    }
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
    load_and_store_missing_data_with_target(connection_pool, DOWNLOAD_TARGET).await
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
    let mut zip = ZipArchive::new(file)?;

    let shrunk_File = File::create(target_location.clone().to_string() + ".tmp")?;
    let mut new_zip = zip::ZipWriter::new(shrunk_File);
    let mut data: Vec<SecCompany> = vec![];
    let num = &zip.len();
    for i in 0..*num {
        // Check if file is needed
        let mut is_file_needed = false;
        {
            let mut file = zip.by_index(i)?;
            if !(file.name().contains("submission") || file.name().contains("placeholder.txt")) {
                let mut buffer: Vec<u8> = Vec::new();
                let mut cursor = Cursor::new(&mut buffer);
                std::io::copy(&mut file, &mut cursor)?;

                let v = cursor.into_inner();
                let output = String::from_utf8_lossy(v).to_string();
                let infos: SecCompany = collectors::utils::parse_response(&output)?;
                is_file_needed = infos.exchanges.len() > 0 || infos.tickers.len() > 0;
                data.push(infos);
            }
        }
        if is_file_needed {
            new_zip.raw_copy_file(zip.by_index(i)?)?;
        }
    }
    println!("Calling finsih");
    new_zip.finish()?;

    println!("Finsih called");
    let transposed_data = transpose_sec_companies(data);

    sqlx::query!("INSERT INTO sec_companies (cik, sic, \"name\", ticker, exchange, state_of_incorporation) Select * from UNNEST ($1::int4[],$2::int[],$3::text[],$4::text[],$5::text[],$6::text[]) on conflict do nothing",
    &transposed_data.cik[..],
    &transposed_data.sic[..] as _,
    &transposed_data.name[..],
    &transposed_data.tickers[..],
    &transposed_data.exchanges[..] as _,
    &transposed_data.state_of_incorporation[..])
    .execute(&connection_pool).await?;

    Ok(())
}

async fn download_url(url: &str, destination: &str) -> Result<()> {
    let response = reqwest::get(url).await?;
    let mut content = Cursor::new(response.bytes().await?);
    let mut target_destination = File::create(destination)?;
    copy(&mut content, &mut target_destination)?;
    Ok(())
}

fn transpose_sec_companies(companies: Vec<SecCompany>) -> TransposedSecCompany {
    let mut result = TransposedSecCompany::new();
    for company in companies {
        for i in 0..company.tickers.len() {
            result.cik.push(
                company
                    .cik
                    .parse::<i32>()
                    .expect("Only numbers expected here."),
            );
            result.sic.push(company.sic.parse::<i32>().ok());
            result.name.push(company.name.clone());
            result.tickers.push(
                company.tickers[i]
                    .to_owned()
                    .expect("Iterating over available tickers."),
            );
            result.exchanges.push(company.exchanges[i].to_owned());
            result
                .state_of_incorporation
                .push(company.state_of_incorporation.clone());
        }
    }
    result
}

#[cfg(test)]
mod test {
    use crate::{
        collectors::source_apis::sec_companies::{
            load_and_store_missing_data_with_target, DOWNLOAD_TARGET,
        },
        configuration::get_configuration,
        utils::errors::Result,
    };
    use sqlx::{PgPool, Pool, Postgres};
    use tracing_test::traced_test;

    #[traced_test]
    #[sqlx::test]
    async fn query_http_and_write_to_db(pool: Pool<Postgres>) -> Result<()> {
        // let configuration = get_configuration().expect("Failed to read configuration.");
        // let connection_pool = PgPool::connect_with(configuration.database.with_db())
        //     .await
        //     .expect("Failed to connect to Postgres.");
        // load_and_store_missing_data_with_target(connection_pool, download_target).await?;
        load_and_store_missing_data_with_target(pool.clone(), DOWNLOAD_TARGET).await?;
        Ok(())
    }

    fn outdated_time_is_correctly_detected() {}

    fn file_is_only_loaded_when_outdated() {}

    fn file_smaller_10MB_will_be_skipped() {}
}
