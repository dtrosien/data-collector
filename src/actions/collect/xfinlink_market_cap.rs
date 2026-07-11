use crate::api_keys::api_key::{ApiKey, ApiKeyPlatform, Status};
use crate::api_keys::key_manager::KeyManager;
use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap};
use crate::database::warden_service::{WardenService, WardenType};
use crate::database::xfinlink_market_cap_service::{
    XfinlinkMarketCapEntry, XfinlinkMarketCapService,
};
use async_trait::async_trait;
use chrono::{Months, NaiveDate, Utc};
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
#[serde(untagged)]
enum XfinlinkResponses {
    Success(XfinlinkResponse),
    Error(XfinlinkErrorResponse),
}

#[derive(Debug, Clone, Deserialize)]
struct XfinlinkResponse {
    data: Vec<XfinlinkDataPoint>,
    meta: XfinlinkMeta,
}

#[derive(Debug, Clone, Deserialize)]
struct XfinlinkErrorResponse {
    error: String,
    status: u16,
    detail: String,
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
            let canonical_ticker = fetch_and_store(
                symbol,
                start_date,
                today,
                &client,
                &mut api_key,
                &db_service,
                &warden_service,
            )
            .await?;
            // If the response used a different canonical ticker, add it to already_searched
            // so it isn't re-selected (and re-fetched) in the same run.
            if let Some(ct) = canonical_ticker {
                already_searched.push(ct);
            }
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
/// Returns the canonical ticker if the requested symbol was an alias for a different ticker.
#[tracing::instrument(level = "debug", skip_all, fields(symbol = %symbol))]
async fn fetch_and_store(
    symbol: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    client: &Client,
    api_key: &mut Box<dyn ApiKey>,
    db_service: &XfinlinkMarketCapService,
    warden_service: &WardenService,
) -> Result<Option<String>, anyhow::Error> {
    let mut cursor: Option<String> = None;
    let mut any_data_found = false;
    let mut canonical_ticker: Option<String> = None;
    let mut first_page = true;
    let mut max_date_seen: Option<NaiveDate> = None;
    let mut completed_normally = false;

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

        let parsed =
            crate::utils::action_helpers::parse_response::<XfinlinkResponses>(&response_text)?;

        let response = match parsed {
            XfinlinkResponses::Success(r) => r,
            XfinlinkResponses::Error(e) if e.error == "not_found" => {
                info!(
                    "Symbol {} not found in Xfinlink ({}), marking in warden.",
                    symbol, e.detail
                );
                warden_service
                    .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                    .await?;
                return Ok(None);
            }
            XfinlinkResponses::Error(e) => {
                return Err(anyhow::anyhow!(
                    "Xfinlink API error {} for symbol {}: {}",
                    e.status,
                    symbol,
                    e.detail
                ));
            }
        };

        if !response.meta.tickers_unresolved.is_empty() {
            info!(
                "Symbol {} unresolved in Xfinlink, marking in warden.",
                symbol
            );
            warden_service
                .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                .await?;
            return Ok(None);
        }

        if response.data.is_empty() && !any_data_found {
            info!("No data returned for symbol {}, marking in warden.", symbol);
            warden_service
                .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                .await?;
            return Ok(None);
        }

        if !response.data.is_empty() {
            any_data_found = true;

            // On the first page only, detect alias mapping (e.g. AACI → AACIU).
            // Mark the requested symbol in the warden before saving so it is silenced
            // even if the subsequent save fails.
            if first_page {
                if let Some(first_point) = response.data.first() {
                    let rt = first_point.ticker.clone();
                    if rt != symbol {
                        info!(
                            "Symbol {} is an alias for {} in Xfinlink, marking in warden.",
                            symbol, rt
                        );
                        warden_service
                            .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                            .await?;
                        canonical_ticker = Some(rt);
                    }
                }
                first_page = false;
            }

            // Track the most recent date seen across all pages.
            let page_max = response.data.iter().map(|p| p.date).max();
            max_date_seen = match (max_date_seen, page_max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };

            let entries: Vec<XfinlinkMarketCapEntry> = response
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

        if response.meta.has_more {
            cursor = response.meta.next_cursor;
        } else {
            completed_normally = true;
            break;
        }
    }

    // After a complete fetch, check whether the most recent data point is stale
    // (older than 1 calendar month). If so, mark the symbol in the warden so it
    // is not re-selected on the next run until the 30-day warden cutoff passes.
    if completed_normally {
        let one_month_ago = end_date
            .checked_sub_months(Months::new(1))
            .expect("Subtracting 1 month from end_date should never fail");
        if let Some(max_date) = max_date_seen {
            if max_date <= one_month_ago {
                info!(
                    "Symbol {} has stale data (newest: {}), marking in warden.",
                    symbol, max_date
                );
                warden_service
                    .add_or_update(&symbol.to_string(), WardenType::Xfinlink)
                    .await?;
                // Also warden the canonical ticker if this was an alias.
                if let Some(ref ct) = canonical_ticker {
                    info!(
                        "Canonical ticker {} also has stale data, marking in warden.",
                        ct
                    );
                    warden_service
                        .add_or_update(ct, WardenType::Xfinlink)
                        .await?;
                }
            }
        }
    }

    Ok(canonical_ticker)
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
    let yesterday = Utc::now()
        .date_naive()
        .checked_sub_days(chrono::Days::new(1))
        .expect("Subtracting 1 day should not fail");

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
        yesterday,
    )
    .fetch_one(connection_pool)
    .await?;

    Ok(Some(result.symbol))
}

#[cfg(test)]
mod test {
    use super::{build_url, XfinlinkErrorResponse, XfinlinkResponses};
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

    #[test]
    fn parse_not_found_error_response() {
        let json =
            r#"{"error":"not_found","status":404,"detail":"No matching entities for: AAMRQ"}"#;
        let parsed =
            crate::utils::action_helpers::parse_response::<XfinlinkResponses>(json).unwrap();
        match parsed {
            XfinlinkResponses::Error(XfinlinkErrorResponse {
                error,
                status,
                detail,
            }) => {
                assert_eq!(error, "not_found");
                assert_eq!(status, 404);
                assert!(detail.contains("AAMRQ"));
            }
            XfinlinkResponses::Success(_) => panic!("Expected Error variant"),
        }
    }

    #[test]
    fn alias_detected_when_response_ticker_differs_from_requested() {
        // Requesting AACI but the API returns data for AACIU — alias mapping.
        let json = r#"{
            "data": [
                {
                    "entity_id": 48869,
                    "ticker": "AACIU",
                    "entity_name": "Armada Acquisition Corp. III",
                    "gics_sector": "Industrials",
                    "date": "2026-01-02",
                    "open": 10.85, "high": 10.85, "low": 10.85, "close": 10.85,
                    "adj_close": 10.85, "volume": 0, "return_daily": 0,
                    "shares_outstanding": 7127000, "exchange_code": null,
                    "split_ratio": null, "dividend": null, "market_cap": 77327950
                }
            ],
            "meta": {
                "tickers_requested": ["AACI"],
                "tickers_resolved": {"AACI": 48869},
                "tickers_unresolved": [],
                "segments": [],
                "interval": "1d",
                "data_through": "2026-01-02",
                "count": 1,
                "limit": 1000,
                "has_more": false,
                "next_cursor": null
            }
        }"#;
        let parsed =
            crate::utils::action_helpers::parse_response::<XfinlinkResponses>(json).unwrap();
        match parsed {
            XfinlinkResponses::Success(r) => {
                let requested_symbol = "AACI";
                let response_ticker = r.data.first().map(|p| p.ticker.as_str());
                assert_eq!(response_ticker, Some("AACIU"));
                assert_ne!(response_ticker, Some(requested_symbol));
            }
            XfinlinkResponses::Error(_) => panic!("Expected Success variant"),
        }
    }

    #[test]
    fn stale_data_detected_when_newest_date_older_than_one_month() {
        use chrono::Months;

        // today = 2026-07-11; one month ago = 2026-06-11
        // data_through = 2026-01-09 (clearly stale)
        let json = r#"{
            "data": [
                {
                    "entity_id": 1,
                    "ticker": "AAPL",
                    "entity_name": "Apple Inc",
                    "gics_sector": "Information Technology",
                    "date": "2026-01-02",
                    "open": 272.26, "high": 277.84, "low": 269.0, "close": 271.01,
                    "adj_close": 271.01, "volume": 37838100, "return_daily": -0.003,
                    "shares_outstanding": 15115823000, "exchange_code": null,
                    "split_ratio": null, "dividend": null, "market_cap": 4096539191230.0
                },
                {
                    "entity_id": 1,
                    "ticker": "AAPL",
                    "entity_name": "Apple Inc",
                    "gics_sector": "Information Technology",
                    "date": "2026-01-09",
                    "open": 259.08, "high": 260.21, "low": 256.22, "close": 259.37,
                    "adj_close": 259.37, "volume": 39997000, "return_daily": 0.001,
                    "shares_outstanding": 15115823000, "exchange_code": null,
                    "split_ratio": null, "dividend": null, "market_cap": 3920591011510.0
                }
            ],
            "meta": {
                "tickers_requested": ["AAPL"],
                "tickers_resolved": {"AAPL": 1},
                "tickers_unresolved": [],
                "segments": [],
                "interval": "1d",
                "data_through": "2026-01-09",
                "count": 2,
                "limit": 1000,
                "has_more": false,
                "next_cursor": null
            }
        }"#;
        let parsed =
            crate::utils::action_helpers::parse_response::<XfinlinkResponses>(json).unwrap();
        let XfinlinkResponses::Success(r) = parsed else {
            panic!("Expected Success variant");
        };

        let max_date = r.data.iter().map(|p| p.date).max().unwrap();
        assert_eq!(
            max_date,
            NaiveDate::parse_from_str("2026-01-09", "%Y-%m-%d").unwrap()
        );

        // Simulate today = 2026-07-11; one month ago = 2026-06-11
        let today = NaiveDate::parse_from_str("2026-07-11", "%Y-%m-%d").unwrap();
        let one_month_ago = today.checked_sub_months(Months::new(1)).unwrap();
        assert_eq!(
            one_month_ago,
            NaiveDate::parse_from_str("2026-06-11", "%Y-%m-%d").unwrap()
        );
        assert!(
            max_date <= one_month_ago,
            "data should be detected as stale"
        );
    }
}
