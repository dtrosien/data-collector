use async_trait::async_trait;
use chrono::{Days, NaiveDate, Utc};
use sqlx::{FromRow, Pool, Postgres};

const MASSIVE_DIVIDENDS_CUTOFF_DAYS: u64 = 30;
const XFINLINK_CUTOFF_DAYS: u64 = 30;

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
    pub xfinlink: Option<NaiveDate>,
}

#[derive(Debug, Clone, Copy)]
pub enum WardenType {
    FinancialModelingPrep,
    Polygon,
    Sec,
    Nyse,
    MassiveDividends,
    Xfinlink,
}

impl WardenService {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn add_or_update(
        &self,
        symbol: &str,
        source_system: WardenType,
    ) -> Result<(), anyhow::Error> {
        match source_system {
            WardenType::FinancialModelingPrep => self.add_or_update_fin_mod_prep(symbol).await,
            WardenType::Polygon => self.add_or_update_polygon(symbol).await,
            WardenType::Sec => self.add_or_update_sec(symbol).await,
            WardenType::Nyse => self.add_or_update_nyse(symbol).await,
            WardenType::MassiveDividends => self.add_or_update_massive_dividends(symbol).await?,
            WardenType::Xfinlink => self.add_or_update_xfinlink(&symbol.to_string()).await?,
        }

        Ok(())
    }

    async fn add_or_update_fin_mod_prep(&self, _symbol: &str) {
        todo!()
    }

    async fn add_or_update_polygon(&self, _symbol: &str) {
        todo!()
    }

    async fn add_or_update_sec(&self, _symbol: &str) {
        todo!()
    }

    async fn add_or_update_nyse(&self, _symbol: &str) {
        todo!()
    }

    async fn add_or_update_massive_dividends(&self, symbol: &str) -> Result<(), anyhow::Error> {
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

    async fn add_or_update_xfinlink(&self, symbol: &String) -> Result<(), anyhow::Error> {
        let today = chrono::Utc::now().date_naive();

        sqlx::query!(
            r#"
        INSERT INTO source_symbol_warden (issue_symbol, xfinlink)
        VALUES ($1, $2)
        ON CONFLICT (issue_symbol)
        DO UPDATE SET
            xfinlink = EXCLUDED.xfinlink
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
            WardenType::Xfinlink => self.get_missing_xfinlink_symbols().await,
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

    async fn get_missing_xfinlink_symbols(&self) -> Result<Vec<String>, anyhow::Error> {
        let cutoff = Utc::now()
            .date_naive()
            .checked_sub_days(Days::new(XFINLINK_CUTOFF_DAYS))
            .unwrap();

        let rows = sqlx::query!(
            r#"
        SELECT distinct issue_symbol
        FROM source_symbol_warden
        WHERE xfinlink >= $1
        "#,
            cutoff
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.issue_symbol).collect())
    }
}

#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait WardenServiceTrait: Send + Sync {
    async fn get_missing_symbols(
        &self,
        source_system: crate::database::warden_service::WardenType,
    ) -> Result<Vec<String>, anyhow::Error>;
    async fn add_or_update(
        &self,
        symbol: &str,
        source_system: crate::database::warden_service::WardenType,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl WardenServiceTrait for WardenService {
    async fn get_missing_symbols(
        &self,
        source_system: crate::database::warden_service::WardenType,
    ) -> Result<Vec<String>, anyhow::Error> {
        self.get_missing_symbols(source_system).await
    }
    async fn add_or_update(
        &self,
        symbol: &str,
        source_system: crate::database::warden_service::WardenType,
    ) -> Result<(), anyhow::Error> {
        self.add_or_update(symbol, source_system).await
    }
}
