use crate::{
    collectors::{self, collector_sources, sp500_fields},
    utils::errors::Result,
};
use async_trait::async_trait;
use chrono::{DateTime, Days, Utc};
use filetime::FileTime;
use serde::Deserialize;
use sqlx::PgPool;
use std::{
    fmt::Display,
    fs::{self, File},
    io::Cursor,
    path::PathBuf,
    u8,
};
use std::{io::copy, path::Path};

use zip::ZipArchive;

use tokio_stream::StreamExt;

use crate::collectors::collector::Collector;
use tracing::debug;

use crate::tasks::runnable::Runnable;
use crate::utils::telemetry::spawn_blocking_with_tracing;

const DOWNLOAD_SOURCE: &str =
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip";

const TARGET_SUBDIRECTORIES: &str = "data-collector/sec_companies";
const TARGET_FILE_NAME: &str = "submissions.zip";
const TARGET_TMP_FILE_NAME: &str = "submissions.zip.tmp";

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
    state_of_incorporation: Option<String>,
}

#[derive(Default, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TransposedSecCompany {
    pub cik: Vec<i32>,
    pub sic: Vec<Option<i32>>,
    pub name: Vec<String>,
    pub tickers: Vec<String>,
    pub exchanges: Vec<Option<String>>,
    pub state_of_incorporation: Vec<Option<String>>,
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
    let target_zip_location = prepare_generic_zip_location(TARGET_FILE_NAME)?;
    load_and_store_missing_data_with_targets(connection_pool, DOWNLOAD_SOURCE, &target_zip_location)
        .await
}

pub async fn load_and_store_missing_data_with_targets(
    connection_pool: PgPool,
    url: &str,
    zip_file_location_ref: &PathBuf,
) -> Result<()> {
    download_archive_if_needed(zip_file_location_ref, url).await?;
    let zip_file_location = zip_file_location_ref.clone();
    let zip_archive =
        spawn_blocking_with_tracing(move || get_zip_file(zip_file_location)).await??;
    let found_data = search_and_shrink_zip(zip_archive, zip_file_location_ref)?;
    let transposed_data = transpose_sec_companies(found_data);

    sqlx::query!("INSERT INTO sec_companies (cik, sic, \"name\", ticker, exchange, state_of_incorporation) Select * from UNNEST ($1::int4[],$2::int[],$3::text[],$4::text[],$5::text[],$6::text[]) on conflict do nothing",
    &transposed_data.cik[..],
    &transposed_data.sic[..] as _, //cast due to None's in the vector
    &transposed_data.name[..],
    &transposed_data.tickers[..],
    &transposed_data.exchanges[..] as _, //cast due to None's in the vector
    &transposed_data.state_of_incorporation[..] as _ ) //cast due to None's in the vector
    .execute(&connection_pool).await?;

    Ok(())
}

fn search_and_shrink_zip(
    mut zip_archive: ZipArchive<File>,
    target_location: &PathBuf,
) -> Result<Vec<SecCompany>> {
    let tmp_location = compute_tmp_location(target_location);
    let mut new_zip = zip::ZipWriter::new(File::create(tmp_location.clone())?);
    let mut found_data: Vec<SecCompany> = vec![];
    for i in 0..zip_archive.len() {
        let mut buffer: Vec<u8> = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);
        {
            let mut file = zip_archive.by_index(i)?;
            if file.name().contains("submission") || file.name().contains("placeholder.txt") {
                continue;
            }
            std::io::copy(&mut file, &mut cursor)?;
        }
        let output = String::from_utf8_lossy(cursor.into_inner()).to_string();
        let infos: SecCompany = collectors::utils::parse_response(&output)?;
        if !infos.exchanges.is_empty() || !infos.tickers.is_empty() {
            found_data.push(infos);
            new_zip.raw_copy_file(zip_archive.by_index(i)?)?;
        }
    }
    new_zip.finish()?;
    //Change modified date of newly created file
    let file_modification_date = target_location.metadata()?.modified()?;
    filetime::set_file_mtime(
        &tmp_location,
        FileTime::from_system_time(file_modification_date),
    )?;
    fs::rename(tmp_location, target_location)?;
    Ok(found_data)
}

fn compute_tmp_location(target_location: &Path) -> PathBuf {
    let mut tmp_location = PathBuf::from(target_location);
    tmp_location.pop();
    tmp_location.push(TARGET_TMP_FILE_NAME);
    tmp_location
}

fn get_zip_file(target_location: PathBuf) -> Result<ZipArchive<File>> {
    let file = File::open(target_location.to_str().unwrap())?;
    let zip_archive = ZipArchive::new(file)?;
    Ok(zip_archive)
}

async fn download_archive_if_needed(target_location: &PathBuf, url: &str) -> Result<()> {
    if is_download_needed(target_location) {
        debug!("Downloading {}", url);
        download_url(url, target_location.to_str().unwrap()).await?;
    }
    Ok(())
}

///A download is needed, if either the file has 0 bytes or is strictly older than 7 days
fn is_download_needed(target_location: &PathBuf) -> bool {
    match fs::metadata(target_location) {
        Ok(metadata) => {
            let modification_date: DateTime<Utc> = metadata.modified().unwrap().into();
            modification_date.checked_add_days(Days::new(7)).unwrap() < Utc::now()
                || metadata.len() == 0
        }
        Err(_) => true,
    }
}

/// Creates directories if needed and return the location to the zip file, independent, if it is existing or not.
fn prepare_generic_zip_location(filename: &str) -> Result<std::path::PathBuf> {
    prepare_zip_location(
        home::home_dir().unwrap().to_str().unwrap(),
        TARGET_SUBDIRECTORIES,
        filename,
    )
}

/// Creates directories if needed and return the location to the zip file, independent, if it is existing or not.
fn prepare_zip_location(
    root_path: &str,
    intermediate_path: &str,
    file_name: &str,
) -> Result<std::path::PathBuf> {
    let mut path_buf = PathBuf::from(root_path);
    path_buf.push(intermediate_path);
    fs::create_dir_all(
        path_buf
            .to_str()
            .ok_or("Invalid character for path to SEC zip file.")?,
    )?;
    path_buf.push(file_name);
    Ok(path_buf)
}

async fn download_url(url: &str, destination: &str) -> Result<()> {
    // let  response = reqwest::get(url).await?;
    let client = reqwest::Client::new();
    let mut response = client
        .get(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        )
        .header(
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        )
        .header("Accept-Language", "en-US,en;q=0.5")
        .header("Accept-Encoding", "gzip, deflate, br")
        .header("Connection", "keep-alive")
        .header("Upgrade-Insecure-Requests", "Requests: 1")
        .header("Sec-Fetch-Dest", "document")
        .header("Sec-Fetch-Mode", "navigate")
        .header("Sec-Fetch-Site", "none")
        .header("Sec-Fetch-User", "?1")
        .header("TE", "trailers")
        .send()
        .await?
        .bytes_stream();

    // let mut content = Cursor::new(response.bytes().await?);
    let mut target_destination = File::create(destination)?;
    while let Some(item) = response.next().await {
        let mut chunk = item.or(Err("Error while downloading file"))?;
        let mut cursor = Cursor::new(&mut chunk);
        copy(&mut cursor, &mut target_destination)?;
    }
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
    use std::{fs::File, path::PathBuf};

    use crate::{
        collectors::source_apis::sec_companies::{
            load_and_store_missing_data_with_targets, prepare_zip_location, TARGET_FILE_NAME,
        },
        utils::errors::Result,
    };
    use chrono::{Days, Duration, Utc};
    use filetime::FileTime;
    use httpmock::{Method::GET, MockServer};
    use sqlx::{Pool, Postgres};
    use std::io::prelude::*;
    use std::io::BufReader;
    use tempfile::TempDir;
    use tracing_test::traced_test;

    use super::{download_archive_if_needed, download_url, is_download_needed};

    #[test]
    fn given_new_file_when_checked_then_returns_false() -> Result<()> {
        let file = tempfile::Builder::new().tempfile()?;
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_and_exchange.zip");
        std::fs::copy(d, &file)?;
        let file_path = PathBuf::from(file.path());

        assert_eq!(is_download_needed(&file_path), false);
        Ok(())
    }

    #[test]
    fn given_outdated_file_when_checked_then_returns_true() -> Result<()> {
        let file = tempfile::Builder::new().tempfile()?;
        let file_path = PathBuf::from(file.path());
        let time = Utc::now()
            .checked_sub_days(Days::new(8))
            .unwrap()
            .timestamp();
        filetime::set_file_mtime(&file_path, FileTime::from_unix_time(time, 0))?;
        assert_eq!(is_download_needed(&file_path), true);
        Ok(())
    }

    #[test]
    fn given_almost_outdated_file_when_checked_then_returns_false() -> Result<()> {
        let file = tempfile::Builder::new().tempfile()?;
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_and_exchange.zip");
        std::fs::copy(d, &file)?;
        let file_path = PathBuf::from(file.path());
        let time = Utc::now()
            .checked_sub_days(Days::new(7))
            .unwrap()
            .checked_add_signed(Duration::minutes(10))
            .unwrap()
            .timestamp();
        filetime::set_file_mtime(&file_path, FileTime::from_unix_time(time, 0))?;
        assert_eq!(is_download_needed(&file_path), false);
        Ok(())
    }

    #[test]
    fn given_no_file_when_checked_then_returns_true() -> Result<()> {
        let file_path = PathBuf::new();

        assert_eq!(is_download_needed(&file_path), true);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn file_is_downloaded_successfully() -> Result<()> {
        //Read file from resources
        let mut file_content: Vec<u8> = vec![];
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_and_exchange.zip");
        BufReader::new(File::open(d.to_str().unwrap().to_string())?)
            .read_to_end(&mut file_content)?;

        //Prepare http server and target location
        let target_file = tempfile::Builder::new().tempfile()?;

        let server = MockServer::start();
        let url = server.base_url();
        server.mock(|when, then| {
            when.method(GET)
            .header(
                "User-Agent",
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
            )
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            )
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Accept-Encoding", "gzip, deflate, br")
            .header("Connection", "keep-alive")
            .header("Upgrade-Insecure-Requests", "Requests: 1")
            .header("Sec-Fetch-Dest", "document")
            .header("Sec-Fetch-Mode", "navigate")
            .header("Sec-Fetch-Site", "none")
            .header("Sec-Fetch-User", "?1")
            .header("TE", "trailers");
            then.status(200)
                .header("content-type", "application/zip")
                .body(file_content);
        });

        //Act
        download_url(&url, target_file.path().to_str().unwrap()).await?;

        //Assert that new file exists and has correct size
        assert!(target_file.path().exists());
        assert_eq!(target_file.as_file().metadata().unwrap().len(), 3109);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn file_is_loaded_when_outdated() -> Result<()> {
        let file = tempfile::Builder::new().tempfile()?;
        let file_path = PathBuf::from(file.path());
        let time = Utc::now()
            .checked_sub_days(Days::new(8))
            .unwrap()
            .timestamp();
        filetime::set_file_mtime(&file_path, FileTime::from_unix_time(time, 0))?;

        //Read file from resources
        let mut file_content: Vec<u8> = vec![];
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_and_exchange.zip");
        BufReader::new(File::open(d.to_str().unwrap().to_string())?)
            .read_to_end(&mut file_content)?;

        //Prepare http server
        let server = MockServer::start();
        let url = server.base_url();
        server.mock(|when, then| {
                    when.method(GET)
                    .header(
                        "User-Agent",
                        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
                    )
                    .header(
                        "Accept",
                        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    )
                    .header("Accept-Language", "en-US,en;q=0.5")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("Connection", "keep-alive")
                    .header("Upgrade-Insecure-Requests", "Requests: 1")
                    .header("Sec-Fetch-Dest", "document")
                    .header("Sec-Fetch-Mode", "navigate")
                    .header("Sec-Fetch-Site", "none")
                    .header("Sec-Fetch-User", "?1")
                    .header("TE", "trailers");
                    then.status(200)
                        .header("content-type", "application/zip")
                        .body(file_content);
                });
        download_archive_if_needed(&file_path, &url).await?;

        //Assert that new file exists and has correct size
        assert!(file_path.exists());
        assert_eq!(file_path.metadata().unwrap().len(), 3109);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn file_is_not_loaded_when_new() -> Result<()> {
        let file = tempfile::Builder::new().tempfile()?;
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_and_exchange.zip");
        std::fs::copy(d, &file)?;
        let file_path = PathBuf::from(file.path());

        //Read file from resources
        let mut file_content: Vec<u8> = vec![];
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_without_exchange.zip");
        BufReader::new(File::open(d.to_str().unwrap().to_string())?)
            .read_to_end(&mut file_content)?;

        //Prepare http server
        let server = MockServer::start();
        let url = server.base_url();
        server.mock(|when, then| {
                    when.method(GET)
                    .header(
                        "User-Agent",
                        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
                    )
                    .header(
                        "Accept",
                        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    )
                    .header("Accept-Language", "en-US,en;q=0.5")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("Connection", "keep-alive")
                    .header("Upgrade-Insecure-Requests", "Requests: 1")
                    .header("Sec-Fetch-Dest", "document")
                    .header("Sec-Fetch-Mode", "navigate")
                    .header("Sec-Fetch-Site", "none")
                    .header("Sec-Fetch-User", "?1")
                    .header("TE", "trailers");
                    then.status(200)
                        .header("content-type", "application/zip")
                        .body(file_content);
                });
        download_archive_if_needed(&file_path, &url).await?;

        //Assert that new file exists and has correct size
        assert!(file_path.exists());
        assert_eq!(file_path.metadata().unwrap().len(), 3109);
        Ok(())
    }

    #[traced_test]
    #[test]
    fn check_if_zip_location_is_created() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let tmp_dir_location = tmp_dir.path().to_str().unwrap();
        let sub_dir = "test/my/dir";
        let zip_location = prepare_zip_location(tmp_dir_location, sub_dir, TARGET_FILE_NAME)?;
        let mut target_dir = PathBuf::new();
        target_dir.push(tmp_dir_location);
        target_dir.push(sub_dir);
        assert!(target_dir.exists());
        target_dir.push(TARGET_FILE_NAME);
        assert_eq!(target_dir, zip_location);
        Ok(())
    }

    #[traced_test]
    #[sqlx::test]
    async fn check_if_zip_content_is_in_db(pool: Pool<Postgres>) -> Result<()> {
        //Tmp file location
        let file = tempfile::Builder::new().tempfile()?;
        let file_path = PathBuf::from(file.path());

        //Read file from resources
        let mut file_content: Vec<u8> = vec![];
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_without_exchange.zip");
        BufReader::new(File::open(d.to_str().unwrap().to_string())?)
            .read_to_end(&mut file_content)?;

        //Prepare http server
        let server = MockServer::start();
        let url = server.base_url();
        server.mock(|when, then| {
            when.method(GET)
            .header(
                "User-Agent",
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
            )
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            )
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Accept-Encoding", "gzip, deflate, br")
            .header("Connection", "keep-alive")
            .header("Upgrade-Insecure-Requests", "Requests: 1")
            .header("Sec-Fetch-Dest", "document")
            .header("Sec-Fetch-Mode", "navigate")
            .header("Sec-Fetch-Site", "none")
            .header("Sec-Fetch-User", "?1")
            .header("TE", "trailers");
            then.status(200)
                .header("content-type", "application/zip")
                .body(file_content);
        });

        load_and_store_missing_data_with_targets(pool.clone(), &url, &file_path).await?;
        let record = sqlx::query!("SELECT cik, sic, \"name\", ticker, exchange, state_of_incorporation, date_loaded, is_staged FROM sec_companies").fetch_one(&pool).await?;
        assert_eq!(record.cik, 1962554);
        assert_eq!(record.sic.unwrap(), 4210);
        assert_eq!(record.name, "OUI Global");
        assert_eq!(record.ticker, "TKE");
        assert_eq!(record.exchange, None);
        assert_eq!(record.state_of_incorporation, Some("NY".to_string()));
        assert_eq!(record.date_loaded, Utc::now().date_naive());
        assert!(!record.is_staged);

        Ok(())
    }

    #[sqlx::test]
    async fn read_write_zip_and_reread_again(pool: Pool<Postgres>) -> Result<()> {
        //Tmp file location
        let file = tempfile::Builder::new().tempfile()?;
        let file_path = PathBuf::from(file.path());

        //Read file from resources
        let mut file_content: Vec<u8> = vec![];
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/resources/SEC_companies_1_of_3_with_stock_without_exchange.zip");
        BufReader::new(File::open(d.to_str().unwrap().to_string())?)
            .read_to_end(&mut file_content)?;

        //Prepare http server
        let server = MockServer::start();
        let url = server.base_url();
        server.mock(|when, then| {
                    when.method(GET)
                    .header(
                        "User-Agent",
                        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
                    )
                    .header(
                        "Accept",
                        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    )
                    .header("Accept-Language", "en-US,en;q=0.5")
                    .header("Accept-Encoding", "gzip, deflate, br")
                    .header("Connection", "keep-alive")
                    .header("Upgrade-Insecure-Requests", "Requests: 1")
                    .header("Sec-Fetch-Dest", "document")
                    .header("Sec-Fetch-Mode", "navigate")
                    .header("Sec-Fetch-Site", "none")
                    .header("Sec-Fetch-User", "?1")
                    .header("TE", "trailers");
                    then.status(200)
                        .header("content-type", "application/zip")
                        .body(file_content);
                });
        //Load data
        load_and_store_missing_data_with_targets(pool.clone(), &url, &file_path).await?;
        sqlx::query!("Truncate table sec_companies")
            .fetch_all(&pool)
            .await?;
        //Verify that .zip file was shrunken
        assert_eq!(file_path.metadata().unwrap().len(), 855);
        //Load again and if db is not empty
        load_and_store_missing_data_with_targets(pool.clone(), &url, &file_path).await?;
        let record = sqlx::query!("SELECT cik, sic, \"name\", ticker, exchange, state_of_incorporation, date_loaded, is_staged FROM sec_companies").fetch_one(&pool).await?;
        assert_eq!(record.cik, 1962554);
        Ok(())
    }
}
