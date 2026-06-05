use chrono::{Days, NaiveDate, Utc};
use sqlx::{FromRow, Pool, Postgres};

const MASSIVE_DIVIDENDS_CUTOFF_DAYS: u64 = 30;

#[derive(Clone, Debug)]
pub struct WardenService {
    pool: Pool<Postgres>,
}

#[derive(Debug, FromRow)]
struct _WardenEntry {
    pub issue_symbol: String,

    pub financial_modeling_prep: Option<bool>,
    pub polygon: Option<bool>,
    pub sec: Option<bool>,
    pub nyse: Option<bool>,

    pub massive_dividends: Option<NaiveDate>,
}

#[derive(Debug, Clone, Copy)]
pub enum WardenType {
    FinancialModelingPrep,
    Polygon,
    Sec,
    Nyse,
    MassiveDividends,
}

impl WardenService {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn add_or_update(
        &self,
        symbol: &String,
        source_system: WardenType,
    ) -> Result<(), anyhow::Error> {
        match source_system {
            WardenType::FinancialModelingPrep => self.add_or_update_fin_mod_prep(symbol).await,
            WardenType::Polygon => self.add_or_update_polygon(symbol).await,
            WardenType::Sec => self.add_or_update_sec(symbol).await,
            WardenType::Nyse => self.add_or_update_nyse(symbol).await,
            WardenType::MassiveDividends => self.add_or_update_massive_dividends(symbol).await?,
        }

        Ok(())
    }

    async fn add_or_update_fin_mod_prep(&self, _symbol: &String) {
        todo!()
    }

    async fn add_or_update_polygon(&self, _symbol: &String) {
        todo!()
    }

    async fn add_or_update_sec(&self, _symbol: &String) {
        todo!()
    }

    async fn add_or_update_nyse(&self, _symbol: &String) {
        todo!()
    }

    async fn add_or_update_massive_dividends(&self, symbol: &String) -> Result<(), anyhow::Error> {
        let today = chrono::Utc::now().date_naive();

        sqlx::query!(
            r#"
        INSERT INTO source_symbol_warden (issue_symbol, massive_dividends)
        VALUES ($1, $2)
        ON CONFLICT (issue_symbol)
        DO UPDATE SET
            massive_dividends = EXCLUDED.massive_dividends
        "#,
            symbol,
            today
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_missing_symbols(
        &self,
        source_system: WardenType,
    ) -> Result<Vec<String>, anyhow::Error> {
        match source_system {
            WardenType::FinancialModelingPrep => todo!(),
            WardenType::Polygon => todo!(),
            WardenType::Sec => todo!(),
            WardenType::Nyse => todo!(),
            WardenType::MassiveDividends => self.get_missing_massive_dividend_symbols().await,
        }
    }

    async fn get_missing_massive_dividend_symbols(&self) -> Result<Vec<String>, anyhow::Error> {
        let cutoff = Utc::now()
            .date_naive()
            .checked_sub_days(Days::new(MASSIVE_DIVIDENDS_CUTOFF_DAYS))
            .unwrap();

        let rows = sqlx::query!(
            r#"
        SELECT distinct issue_symbol
        FROM source_symbol_warden
        WHERE massive_dividends >= $1
        "#,
            cutoff
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.issue_symbol).collect())
    }
}
