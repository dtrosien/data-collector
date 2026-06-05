use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};

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
    pub frequency: Option<i64>,
    pub cash_amount: f64,
    pub currency: String,
    pub distribution_type: Option<String>,
    pub historical_adjustment_factor: Option<f64>,
    pub split_adjusted_cash_amount: Option<f64>,
    pub is_staged: bool,
}

#[derive(Default, Deserialize, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TransposedPolygonDividendsEntry {
    ticker: Vec<String>,
    record_date: Vec<Option<NaiveDate>>,
    pay_date: Vec<Option<NaiveDate>>,
    declaration_date: Vec<Option<NaiveDate>>,
    ex_dividend_date: Vec<NaiveDate>,
    frequency: Vec<Option<i64>>,
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
    pub async fn save(&self, data: PolygonDividendsEntry) -> Result<(), anyhow::Error> {
        Self::save_all(self, vec![data]).await
    }
    pub async fn save_all(&self, data: Vec<PolygonDividendsEntry>) -> Result<(), anyhow::Error> {
        let dividends = self.transpose_polygon_dividends_entry(data);

        sqlx::query!(
            r#"
    INSERT INTO polygon_dividends (
        ticker,
        record_date,
        pay_date,
        declaration_date,
        ex_dividend_date,
        frequency,
        cash_amount,
        currency,
        distribution_type,
        historical_adjustment_factor,
        split_adjusted_cash_amount,
        is_staged
    )
    SELECT * FROM UNNEST (
        $1::text[],
        $2::date[],
        $3::date[],
        $4::date[],
        $5::date[],
        $6::bigint[],
        $7::float8[],
        $8::text[],
        $9::text[],
        $10::float8[],
        $11::float8[],
        $12::bool[]
    )
    ON CONFLICT DO NOTHING
    "#,
            &dividends.ticker[..],
            &dividends.record_date[..] as _,
            &dividends.pay_date[..] as _,
            &dividends.declaration_date[..] as _,
            &dividends.ex_dividend_date[..],
            &dividends.frequency[..] as _,
            &dividends.cash_amount[..],
            &dividends.currency[..],
            &dividends.distribution_type[..] as _,
            &dividends.historical_adjustment_factor[..] as _,
            &dividends.split_adjusted_cash_amount[..] as _,
            &dividends.is_staged[..],
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // TODO: Add logic if all symbols have been gathered at least once
    pub async fn get_next_issue_symbol_candidate(
        &self,
        lower_symbol_bound: String,
        skippable_symbols: &Vec<String>,
    ) -> Option<String> {
        // select uncollected symbols
        let query_result = sqlx::query!(
            "select distinct(issue_symbol) 
            from master_data md 
            where 
            issue_symbol > $1::text
            AND issue_symbol not in (select unnest($2::text[]))
            AND issue_symbol not in (SELECT distinct pd.ticker  
                                    FROM polygon_dividends pd 
                                    WHERE pd.declaration_date  >= CURRENT_DATE - INTERVAL '14 days')
            limit 1",
            lower_symbol_bound,
            skippable_symbols
        )
        .fetch_one(&self.pool)
        .await;
        match query_result {
            Ok(symbol_candidate) => Some(symbol_candidate.issue_symbol),
            Err(_) => None,
        }

        // TODO: Handle outdated symbols
    }

    fn transpose_polygon_dividends_entry(
        &self,
        dividends: Vec<PolygonDividendsEntry>,
    ) -> TransposedPolygonDividendsEntry {
        let mut result = TransposedPolygonDividendsEntry {
            ticker: vec![],
            record_date: vec![],
            pay_date: vec![],
            declaration_date: vec![],
            ex_dividend_date: vec![],
            frequency: vec![],
            cash_amount: vec![],
            currency: vec![],
            distribution_type: vec![],
            historical_adjustment_factor: vec![],
            split_adjusted_cash_amount: vec![],
            is_staged: vec![],
        };

        for data in dividends {
            result.ticker.push(data.ticker);
            result.record_date.push(data.record_date);
            result.pay_date.push(data.pay_date);
            result.declaration_date.push(data.declaration_date);
            result.ex_dividend_date.push(data.ex_dividend_date);
            result.frequency.push(data.frequency);
            result.cash_amount.push(data.cash_amount);
            result.currency.push(data.currency);
            result.distribution_type.push(data.distribution_type);
            result
                .historical_adjustment_factor
                .push(data.historical_adjustment_factor);
            result
                .split_adjusted_cash_amount
                .push(data.split_adjusted_cash_amount);
            result.is_staged.push(data.is_staged);
        }
        result
    }
}
