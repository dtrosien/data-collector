use chrono::{Days, Months, NaiveDate, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};

use crate::database::master_data_service::MasterDataService;

#[derive(Clone, Debug)]
pub struct PolygonDividendsService {
    pool: Pool<Postgres>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolygonDividendsEntry {
    ticker: String,
    record_date: Option<NaiveDate>,
    pay_date: Option<NaiveDate>,
    declaration_date: Option<NaiveDate>,
    ex_dividend_date: NaiveDate,
    frequency: Option<String>,
    cash_amount: f64,
    currency: String,
    distribution_type: Option<String>,
    historical_adjustment_factor: Option<f64>,
    split_adjusted_cash_amount: Option<f64>,
    is_staged: bool,
}

#[derive(Default, Deserialize, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TransposedPolygonDividendsEntry {
    pub ticker: Vec<String>,
    pub record_date: Vec<Option<NaiveDate>>,
    pub pay_date: Vec<Option<NaiveDate>>,
    pub declaration_date: Vec<Option<NaiveDate>>,
    pub ex_dividend_date: Vec<NaiveDate>,
    pub frequency: Vec<Option<String>>,
    pub cash_amount: Vec<f64>,
    pub currency: Vec<String>,
    pub distribution_type: Vec<Option<String>>,
    pub historical_adjustment_factor: Vec<Option<f64>>,
    pub split_adjusted_cash_amount: Vec<Option<f64>>,
    pub is_staged: Vec<bool>,
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
        let master_date_service = MasterDataService::new(self.pool.clone());
        return master_date_service
            .get_next_company_symbol(lower_symbol_bound)
            .await;
    }
}
