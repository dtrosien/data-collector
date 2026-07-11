use chrono::{Days, NaiveDate, Utc};
use sqlx::{Pool, Postgres};

#[derive(Clone, Debug)]
pub struct XfinlinkMarketCapService {
    pool: Pool<Postgres>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct XfinlinkMarketCapEntry {
    pub symbol: String,
    pub business_date: NaiveDate,
    pub entity_name: Option<String>,
    pub gics_sector: Option<String>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub adj_close: Option<f64>,
    pub volume: Option<f64>,
    pub return_daily: Option<f64>,
    pub shares_outstanding: Option<f64>,
    pub exchange_code: Option<String>,
    pub split_ratio: Option<f64>,
    pub dividend: Option<f64>,
    pub market_cap: Option<f64>,
}

struct Transposed {
    symbol: Vec<String>,
    business_date: Vec<NaiveDate>,
    entity_name: Vec<Option<String>>,
    gics_sector: Vec<Option<String>>,
    open: Vec<Option<f64>>,
    high: Vec<Option<f64>>,
    low: Vec<Option<f64>>,
    close: Vec<Option<f64>>,
    adj_close: Vec<Option<f64>>,
    volume: Vec<Option<f64>>,
    return_daily: Vec<Option<f64>>,
    shares_outstanding: Vec<Option<f64>>,
    exchange_code: Vec<Option<String>>,
    split_ratio: Vec<Option<f64>>,
    dividend: Vec<Option<f64>>,
    market_cap: Vec<Option<f64>>,
}

impl Transposed {
    fn new(data: Vec<XfinlinkMarketCapEntry>) -> Self {
        let mut t = Transposed {
            symbol: vec![],
            business_date: vec![],
            entity_name: vec![],
            gics_sector: vec![],
            open: vec![],
            high: vec![],
            low: vec![],
            close: vec![],
            adj_close: vec![],
            volume: vec![],
            return_daily: vec![],
            shares_outstanding: vec![],
            exchange_code: vec![],
            split_ratio: vec![],
            dividend: vec![],
            market_cap: vec![],
        };
        for e in data {
            t.symbol.push(e.symbol);
            t.business_date.push(e.business_date);
            t.entity_name.push(e.entity_name);
            t.gics_sector.push(e.gics_sector);
            t.open.push(e.open);
            t.high.push(e.high);
            t.low.push(e.low);
            t.close.push(e.close);
            t.adj_close.push(e.adj_close);
            t.volume.push(e.volume);
            t.return_daily.push(e.return_daily);
            t.shares_outstanding.push(e.shares_outstanding);
            t.exchange_code.push(e.exchange_code);
            t.split_ratio.push(e.split_ratio);
            t.dividend.push(e.dividend);
            t.market_cap.push(e.market_cap);
        }
        t
    }
}

impl XfinlinkMarketCapService {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Returns the start date for fetching.
    /// - If prior data exists: returns last_in_db + 1 day (no cap).
    /// - If no prior data: returns today - 365 days.
    pub async fn get_start_date(&self, symbol: &str) -> Result<NaiveDate, anyhow::Error> {
        let result = sqlx::query!(
            "SELECT max(business_date) FROM xfinlink_market_cap WHERE symbol = $1::text",
            symbol
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = result {
            if let Some(max_date) = row.max {
                let next_date = max_date
                    .checked_add_days(Days::new(1))
                    .expect("Adding 1 day should never fail");
                return Ok(next_date);
            }
        }

        let one_year_ago = Utc::now()
            .date_naive()
            .checked_sub_days(Days::new(365))
            .expect("Subtracting 365 days should never fail");
        Ok(one_year_ago)
    }

    pub async fn save_all(&self, data: Vec<XfinlinkMarketCapEntry>) -> Result<(), anyhow::Error> {
        if data.is_empty() {
            return Ok(());
        }
        let t = Transposed::new(data);

        sqlx::query!(
            r#"
            INSERT INTO xfinlink_market_cap (
                symbol, business_date, entity_name, gics_sector,
                open, high, low, close, adj_close, volume, return_daily,
                shares_outstanding, exchange_code, split_ratio, dividend, market_cap
            )
            SELECT * FROM UNNEST (
                $1::text[], $2::date[], $3::text[], $4::text[],
                $5::float8[], $6::float8[], $7::float8[], $8::float8[], $9::float8[],
                $10::float8[], $11::float8[], $12::float8[], $13::text[],
                $14::float8[], $15::float8[], $16::float8[]
            )
            ON CONFLICT DO NOTHING
            "#,
            &t.symbol[..],
            &t.business_date[..],
            &t.entity_name[..] as _,
            &t.gics_sector[..] as _,
            &t.open[..] as _,
            &t.high[..] as _,
            &t.low[..] as _,
            &t.close[..] as _,
            &t.adj_close[..] as _,
            &t.volume[..] as _,
            &t.return_daily[..] as _,
            &t.shares_outstanding[..] as _,
            &t.exchange_code[..] as _,
            &t.split_ratio[..] as _,
            &t.dividend[..] as _,
            &t.market_cap[..] as _,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
