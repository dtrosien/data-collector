use crate::api_keys::api_key::{ApiKey, ApiKeyPlatform, Status};
use crate::api_keys::key_manager::KeyManager;
use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap};
use crate::database::warden_service::{WardenService, WardenType};
use crate::database::xfinlink_market_cap_service::{
    XfinlinkMarketCapEntry, XfinlinkMarketCapService,
};
use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use futures_util::TryFutureExt;
use reqwest::Client;
use secrecy::ExposeSecret;
use serde::Deserialize;
use sqlx::PgPool;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

const BASE_URL: &str = "https://api.xfinlink.com/v1/prices/";
const FIELDS: &str = "date,open,high,low,close,adj_close,volume,return_daily,shares_outstanding,exchange_code,split_ratio,dividend,market_cap";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Xfinlink;
const WAIT_FOR_KEY: bool = false;

#[derive(Clone, Debug)]
pub struct XfinlinkMarketCapCollector {
    pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
}

impl XfinlinkMarketCapCollector {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(pool: PgPool, client: Client, key_manager: Arc<Mutex<KeyManager>>) -> Self {
        XfinlinkMarketCapCollector {
            pool,
            client,
            key_manager,
        }
    }
}

impl Display for XfinlinkMarketCapCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "XfinlinkMarketCapCollector struct.")
    }
}

#[async_trait]
impl Runnable for XfinlinkMarketCapCollector {
    #[tracing::instrument(name = "Run XfinlinkMarketCapCollector", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, crate::dag_schedule::task::TaskError> {
        load_and_store_missing_data(
            self.pool.clone(),
            self.client.clone(),
            self.key_manager.clone(),
        )
        .map_err(UnexpectedError)
        .await?;
        Ok(None)
    }
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct XfinlinkResponse {
    data: Vec<XfinlinkDataPoint>,
    meta: XfinlinkMeta,
}

#[derive(Debug, Clone, Deserialize)]
struct XfinlinkMeta {
    has_more: bool,
    next_cursor: Option<String>,
    tickers_unresolved: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct XfinlinkDataPoint {
    ticker: String,
    entity_name: Option<String>,
    gics_sector: Option<String>,
    date: NaiveDate,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    adj_close: Option<f64>,
    volume: Option<f64>,
    return_daily: Option<f64>,
    shares_outstanding: Option<f64>,
    exchange_code: Option<String>,
    split_ratio: Option<f64>,
    dividend: Option<f64>,
    market_cap: Option<f64>,
}

// ── Main logic ────────────────────────────────────────────────────────────────

#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_and_store_missing_data(
    connection_pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Result<(), anyhow::Error> {
    info!("Starting Xfinlink Market Cap Collector.");

    let db_service = XfinlinkMarketCapService::new(connection_pool.clone());
    let warden_service = WardenService::new(connection_pool.clone());

    let mut already_searched: Vec<String> = vec![];
    let unavailable_symbols = warden_service
        .get_missing_symbols(WardenType::Xfinlink)
        .await?;

    let mut potential_symbol =
        get_next_symbol(&connection_pool, &already_searched, &unavailable_symbols).await?;

    let mut general_api_key =
        KeyManager::get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, PLATFORM).await;

    while let (Some(symbol), Some(mut api_key)) = (
        potential_symbol.as_ref(),
        general_api_key.take_if(|_| potential_symbol.is_some()),
    ) {
        info!("Processing symbol: {}", symbol);
        already_searched.push(symbol.clone());

        let start_date = db_service.get_start_date(symbol).await?;
        let today = Utc::now().date_naive();

        if start_date >= today {
            info!("Symbol {} is up-to-date, skipping.", symbol);
        } else {
            fetch_and_store(
                symbol,
                start_date,
                today,
                &client,
                &mut api_key,
                &db_service,
                &warden_service,
            )
            .await?;
        }

        potential_symbol =
            get_next_symbol(&connection_pool, &already_searched, &unavailable_symbols).await?;
        general_api_key = KeyManager::exchange_apikey_or_wait_if_non_ready(
            key_manager.clone(),
            WAIT_FOR_KEY,
            api_key,
            PLATFORM,
        )
        .await;
    }

    if let Some(api_key) = general_api_key {
        let mut km = key_manager
            .lock()
            .expect("KeyManager lock should not be poisoned");
        km.add_key_by_platform(api_key);
    }

    Ok(())
}

/// Fetches data for one symbol (with cursor pagination) and stores it.
#[tracing::instrument(level = "debug", skip_all, fields(symbol = %symbol))]
async fn fetch_and_store(
    symbol: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    client: &Client,
    api_key: &mut Box<dyn ApiKey>,
    db_service: &XfinlinkMarketCapService,
    warden_service: &WardenService,
) -> Result<(), anyhow::Error> {
    let mut cursor: Option<String> = None;
    let mut any_data_found = false;

    loop {
        if api_key.get_status() != Status::Ready {
            warn!("API key exhausted while fetching symbol {}", symbol);
            break;
        }

        let url = build_url(symbol, start_date, end_date, cursor.as_deref());
        debug!("Xfinlink request URL: {}", url);

        let secret = api_key.get_secret().expose_secret().clone();
        let response_text = client
            .get(&url)
            .header("X-API-Key", &secret)
            .send()
            .await?
            .text()
            .await?;

        debug!("Xfinlink response: {}", response_text);

        let parsed: XfinlinkResponse = serde_json::from_str(&response_text).map_err(|e| {
            anyhow::anyhow!("Failed to parse Xfinlink response for {}: {}", symbol, e)
        })?;

        if !parsed.meta.tickers_unresolved.is_empty() {
            info!(
                "Symbol {} unresolved in Xfinlink, marking in warden.",
                symbol
            );
            warden_service
                .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                .await?;
            return Ok(());
        }

        if parsed.data.is_empty() && !any_data_found {
            info!("No data returned for symbol {}, marking in warden.", symbol);
            warden_service
                .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                .await?;
            return Ok(());
        }

        if !parsed.data.is_empty() {
            any_data_found = true;
            let entries: Vec<XfinlinkMarketCapEntry> = parsed
                .data
                .into_iter()
                .map(|p| XfinlinkMarketCapEntry {
                    symbol: p.ticker,
                    business_date: p.date,
                    entity_name: p.entity_name,
                    gics_sector: p.gics_sector,
                    open: p.open,
                    high: p.high,
                    low: p.low,
                    close: p.close,
                    adj_close: p.adj_close,
                    volume: p.volume,
                    return_daily: p.return_daily,
                    shares_outstanding: p.shares_outstanding,
                    exchange_code: p.exchange_code,
                    split_ratio: p.split_ratio,
                    dividend: p.dividend,
                    market_cap: p.market_cap,
                })
                .collect();
            db_service.save_all(entries).await?;
        }

        if parsed.meta.has_more {
            cursor = parsed.meta.next_cursor;
        } else {
            break;
        }
    }

    Ok(())
}

fn build_url(symbol: &str, start: NaiveDate, end: NaiveDate, cursor: Option<&str>) -> String {
    let mut url = format!(
        "{}{}?start={}&end={}&fields={}",
        BASE_URL, symbol, start, end, FIELDS
    );
    if let Some(c) = cursor {
        url.push_str("&cursor=");
        url.push_str(c);
    }
    url
}

// ── Symbol selection (3-tier priority, same as FMP collector) ─────────────────

async fn get_next_symbol(
    connection_pool: &PgPool,
    already_searched: &Vec<String>,
    unavailable_symbols: &Vec<String>,
) -> Result<Option<String>, anyhow::Error> {
    // Tier 1: symbols with a clean IPO date and no xfinlink data yet
    let result = sqlx::query!(
        "select issue_symbol from master_data_eligible mde
         where
        (start_nyse != '1792-05-17' or start_nyse is null) and
        (start_nyse_arca != '1792-05-17' or start_nyse_arca is null) and
        (start_nyse_american != '1792-05-17' or start_nyse_american is null) and
        (start_nasdaq != '1792-05-17' or start_nasdaq is null) and
        (start_nasdaq_global_select_market != '1792-05-17' or start_nasdaq_global_select_market is null) and
        (start_nasdaq_select_market != '1792-05-17' or start_nasdaq_select_market is null) and
        (start_nasdaq_capital_market != '1792-05-17' or start_nasdaq_capital_market is null) and
        (start_cboe != '1792-05-17' or start_cboe is null) and
        issue_symbol not in (select unnest($1::text[])) and
        issue_symbol not in (select unnest($2::text[])) and
        issue_symbol not in (select distinct(symbol) from xfinlink_market_cap)
        order by issue_symbol limit 1",
        unavailable_symbols,
        already_searched
    )
    .fetch_one(connection_pool)
    .await;

    if let Ok(r) = result {
        return Ok(r.issue_symbol);
    }

    // Tier 2: symbols without a clean IPO date and no xfinlink data yet
    let result = sqlx::query!(
        "select issue_symbol from master_data_eligible mde
         where
        not ((start_nyse != '1792-05-17' or start_nyse is null) and
        (start_nyse_arca != '1792-05-17' or start_nyse_arca is null) and
        (start_nyse_american != '1792-05-17' or start_nyse_american is null) and
        (start_nasdaq != '1792-05-17' or start_nasdaq is null) and
        (start_nasdaq_global_select_market != '1792-05-17' or start_nasdaq_global_select_market is null) and
        (start_nasdaq_select_market != '1792-05-17' or start_nasdaq_select_market is null) and
        (start_nasdaq_capital_market != '1792-05-17' or start_nasdaq_capital_market is null) and
        (start_cboe != '1792-05-17' or start_cboe is null)) and
        issue_symbol not in (select unnest($1::text[])) and
        issue_symbol not in (select unnest($2::text[])) and
        issue_symbol not in (select distinct(symbol) from xfinlink_market_cap)
        order by issue_symbol limit 1",
        unavailable_symbols,
        already_searched
    )
    .fetch_one(connection_pool)
    .await;

    if let Ok(r) = result {
        return Ok(r.issue_symbol);
    }

    // Tier 3: symbols that have data but are outdated
    get_next_outdated_symbol(connection_pool, already_searched, unavailable_symbols).await
}

async fn get_next_outdated_symbol(
    connection_pool: &PgPool,
    already_searched: &Vec<String>,
    unavailable_symbols: &Vec<String>,
) -> Result<Option<String>, anyhow::Error> {
    let one_year_ago = Utc::now()
        .date_naive()
        .checked_sub_days(chrono::Days::new(365))
        .expect("Subtracting 365 days should not fail");

    let result = sqlx::query!(
        r#"
        select r.symbol from
            (select symbol, max(business_date) as max_date
             from xfinlink_market_cap
             group by symbol) as r
        where r.symbol not in (select unnest($1::text[]))
        and r.symbol not in (select unnest($2::text[]))
        and r.max_date < $3
        order by r.max_date asc limit 1
        "#,
        unavailable_symbols,
        already_searched,
        one_year_ago,
    )
    .fetch_one(connection_pool)
    .await?;

    Ok(Some(result.symbol))
}

#[cfg(test)]
mod test {
    use super::build_url;
    use chrono::NaiveDate;

    #[test]
    fn build_url_without_cursor() {
        let start = NaiveDate::parse_from_str("2026-01-01", "%Y-%m-%d").unwrap();
        let end = NaiveDate::parse_from_str("2026-01-10", "%Y-%m-%d").unwrap();
        let url = build_url("AAPL", start, end, None);
        assert!(url.starts_with(
            "https://api.xfinlink.com/v1/prices/AAPL?start=2026-01-01&end=2026-01-10&fields="
        ));
        assert!(!url.contains("cursor"));
    }

    #[test]
    fn build_url_with_cursor() {
        let start = NaiveDate::parse_from_str("2026-01-01", "%Y-%m-%d").unwrap();
        let end = NaiveDate::parse_from_str("2026-01-10", "%Y-%m-%d").unwrap();
        let url = build_url("AAPL", start, end, Some("abc123"));
        assert!(url.contains("&cursor=abc123"));
    }
}
