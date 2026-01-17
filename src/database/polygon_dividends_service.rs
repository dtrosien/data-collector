use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};

use crate::database::master_data_service::MasterDataService;

#[derive(Clone, Debug)]
pub struct PolygonDividendsService {
    pool: Pool<Postgres>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolygonDividendsEntry {
    pub ticker: String,
    pub record_date: Option<NaiveDate>,
    pub pay_date: Option<NaiveDate>,
    pub declaration_date: Option<NaiveDate>,
    pub ex_dividend_date: NaiveDate,
    pub frequency: Option<String>,
    pub cash_amount: f64,
    pub currency: String,
    pub distribution_type: Option<String>,
    pub historical_adjustment_factor: Option<f64>,
    pub split_adjusted_cash_amount: Option<f64>,
    pub is_staged: bool,
}

#[derive(Default, Deserialize, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TransposedPolygonDividendsEntry {
    ticker: Vec<String>,
    record_date: Vec<Option<NaiveDate>>,
    pay_date: Vec<Option<NaiveDate>>,
    declaration_date: Vec<Option<NaiveDate>>,
    ex_dividend_date: Vec<NaiveDate>,
    frequency: Vec<Option<String>>,
    cash_amount: Vec<f64>,
    currency: Vec<String>,
    distribution_type: Vec<Option<String>>,
    historical_adjustment_factor: Vec<Option<f64>>,
    split_adjusted_cash_amount: Vec<Option<f64>>,
    is_staged: Vec<bool>,
}

impl PolygonDividendsService {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
    fn save(&self, data: PolygonDividendsEntry) -> Result<(), anyhow::Error> {
        Self::saveAll(self, vec![data])
    }
    fn saveAll(&self, data: Vec<PolygonDividendsEntry>) -> Result<(), anyhow::Error> {
        todo!();
        Ok(())
    }

    // TODO: Add logic if all symbols have been gathered at least once
    pub async fn get_next_issue_symbol_candidate(
        &self,
        lower_symbol_bound: String,
    ) -> Option<String> {
        // select uncollected symbols
        let query_result = sqlx::query!(
            "select distinct(issue_symbol) 
            from master_data md 
            where 
                issue_symbol not in (select distinct ticker from polygon_dividends pd) 
            and issue_symbol > $1::text limit 1",
            lower_symbol_bound
        )
        .fetch_one(&self.pool)
        .await;
        match query_result {
            Ok(symbol_candidate) => Some(symbol_candidate.issue_symbol),
            Err(_) => None,
        }

        // TODO: Handle outdated symbols
    }
}
