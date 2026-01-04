use chrono::NaiveDate;
use reqwest::Client;
use sqlx::{Pool, Postgres};
use tracing::error;

#[derive(Clone, Debug)]
pub struct MasterDataService {
    pool: Pool<sqlx::Postgres>,
}

struct MasterDataEntry {
    issuer_name: String,
    issue_symbol: String,
    location: Option<String>,
    start_nyse: Option<NaiveDate>,
    start_nyse_arca: Option<NaiveDate>,
    start_nyse_american: Option<NaiveDate>,
    start_nasdaq: Option<NaiveDate>,
    start_nasdaq_global_select_market: Option<NaiveDate>,
    start_nasdaq_select_market: Option<NaiveDate>,
    start_nasdaq_capital_market: Option<NaiveDate>,
    start_cboe: Option<NaiveDate>,
    instrument: Option<String>,
    category: Option<String>,
    renamed_to_issuer_name: Option<String>,
    renamed_to_issue_symbol: Option<String>,
    renamed_at_date: Option<NaiveDate>,
    current_name: Option<String>,
    suspended: Option<bool>,
    suspension_date: Option<NaiveDate>,
}

struct TransposedMasterDataEntry {
    issuer_name: Vec<String>,
    issue_symbol: Vec<String>,
    location: Vec<Option<String>>,
    start_nyse: Vec<Option<NaiveDate>>,
    start_nyse_arca: Vec<Option<NaiveDate>>,
    start_nyse_american: Vec<Option<NaiveDate>>,
    start_nasdaq: Vec<Option<NaiveDate>>,
    start_nasdaq_global_select_market: Vec<Option<NaiveDate>>,
    start_nasdaq_select_market: Vec<Option<NaiveDate>>,
    start_nasdaq_capital_market: Vec<Option<NaiveDate>>,
    start_cboe: Vec<Option<NaiveDate>>,
    instrument: Vec<Option<String>>,
    category: Vec<Option<String>>,
    renamed_to_issuer_name: Vec<Option<String>>,
    renamed_to_issue_symbol: Vec<Option<String>>,
    renamed_at_date: Vec<Option<NaiveDate>>,
    current_name: Vec<Option<String>>,
    suspended: Vec<Option<bool>>,
    suspension_date: Vec<Option<NaiveDate>>,
}

impl MasterDataService {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn get_all_companies(&self) -> Vec<String> {
        let res = sqlx::query!(
            "select distinct(issue_symbol) as issue_symbol from master_data md order by issue_symbol;"
        )
        .fetch_all(&self.pool)
        .await;

        match res {
            Ok(res2) => {
                let a: Vec<String> = res2.into_iter().map(|x| x.issue_symbol).collect();
                a
            }
            Err(_) => {
                error!("Unable to get companies from master table.");
                vec![]
            }
        }
    }

    pub async fn get_next_company_symbol(
        &self,
        exclude_company_symbols_until: String,
    ) -> Option<String> {
        let res = sqlx::query!(
            "select issue_symbol from master_data md where issue_symbol > $1::text order by issue_symbol limit 1",
            exclude_company_symbols_until)
            .fetch_one(&self.pool)
            .await;
        match res {
            Ok(res2) => Some(res2.issue_symbol),
            Err(_) => None,
        }
    }
}
