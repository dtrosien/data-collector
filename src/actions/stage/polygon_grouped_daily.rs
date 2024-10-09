use async_trait::async_trait;
use chrono::NaiveDate;
use futures_util::{TryFutureExt, TryStreamExt};

use rand::Error;
use sqlx::types::BigDecimal;
use sqlx::PgPool;
// use tokio_stream::StreamExt;
use futures_util::{pin_mut, stream, Stream, StreamExt};
use std::fmt::Display;
use tracing::info;

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

#[derive(Clone, Debug)]
pub struct market_data_transposed {
    symbol: Vec<String>,
    business_date: Vec<NaiveDate>,
    year_month: Vec<Option<u16>>,
    stock_price: Vec<BigDecimal>,
    open: Vec<Option<BigDecimal>>,
    close: Vec<Option<BigDecimal>>,
    volume_trade: Vec<Option<BigDecimal>>,
    shares_traded: Vec<Option<i32>>,
    after_hours: Vec<Option<BigDecimal>>,
    pre_market: Vec<Option<BigDecimal>>,
    market_capitalization: Vec<Option<BigDecimal>>,
}

#[derive(Clone, Debug)]
pub struct market_data {
    symbol: String,
    business_date: NaiveDate,
    year_month: Option<u16>,
    stock_price: BigDecimal,
    open: Option<BigDecimal>,
    close: Option<BigDecimal>,
    volume_trade: Option<BigDecimal>,
    shares_traded: Option<i32>,
    after_hours: Option<BigDecimal>,
    pre_market: Option<BigDecimal>,
    market_capitalization: Option<BigDecimal>,
}

#[derive(Clone, Debug)]
pub struct market_data_builder {
    symbol: Option<String>,
    business_date: Option<NaiveDate>,
    year_month: Option<u16>,
    stock_price: Option<BigDecimal>,
    open: Option<BigDecimal>,
    close: Option<BigDecimal>,
    volume_trade: Option<BigDecimal>,
    shares_traded: Option<i32>,
    after_hours: Option<BigDecimal>,
    pre_market: Option<BigDecimal>,
    market_capitalization: Option<BigDecimal>,
}

impl market_data_builder {
    fn builder() -> market_data_builder {
        market_data_builder {
            symbol: None,
            business_date: None,
            year_month: None,
            stock_price: None,
            open: None,
            close: None,
            volume_trade: None,
            shares_traded: None,
            after_hours: None,
            pre_market: None,
            market_capitalization: None,
        }
    }

    fn symbol(mut self, symbol: String) -> Self {
        self.symbol = Some(symbol);
        self
    }

    fn business_date(mut self, business_date: NaiveDate) -> Self {
        self.business_date = Some(business_date);
        self
    }

    fn year_month(mut self, year_month: Option<u16>) -> Self {
        self.year_month = year_month;
        self
    }

    fn stock_price(mut self, stock_price: BigDecimal) -> Self {
        self.stock_price = Some(stock_price);
        self
    }

    fn open(mut self, open: Option<BigDecimal>) -> Self {
        self.open = open;
        self
    }

    fn close(mut self, close: Option<BigDecimal>) -> Self {
        self.close = close;
        self
    }

    fn volume_trade(mut self, volume_trade: Option<BigDecimal>) -> Self {
        self.volume_trade = volume_trade;
        self
    }

    fn shares_traded(mut self, shares_traded: Option<i32>) -> Self {
        self.shares_traded = shares_traded;
        self
    }
    fn after_hours(mut self, after_hours: Option<BigDecimal>) -> Self {
        self.after_hours = after_hours;
        self
    }
    fn pre_market(mut self, pre_market: Option<BigDecimal>) -> Self {
        self.pre_market = pre_market;
        self
    }
    fn market_capitalization(mut self, market_capitalization: Option<BigDecimal>) -> Self {
        self.market_capitalization = market_capitalization;
        self
    }

    fn build(self) -> Result<market_data, Error> {
        if self.symbol.is_some() && self.business_date.is_some() && self.stock_price.is_some() {
            return Ok(market_data {
                symbol: self.symbol.expect("Checked earlier"),
                business_date: self.business_date.expect("Checked earlier"),
                year_month: self.year_month,
                stock_price: self.stock_price.expect("Checked earlier"),
                open: self.open,
                close: self.close,
                volume_trade: self.volume_trade,
                shares_traded: self.shares_traded,
                after_hours: self.after_hours,
                pre_market: self.pre_market,
                market_capitalization: self.market_capitalization,
            });
        }
        Err(Error::new("err"))
    }
}

#[derive(Clone, Debug)]
pub struct PolygonGroupedDailyStager {
    pool: PgPool,
}

impl PolygonGroupedDailyStager {
    pub fn new(pool: PgPool) -> Self {
        PolygonGroupedDailyStager { pool }
    }
}

impl Display for PolygonGroupedDailyStager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PolygonGroupedDailyStager struct.")
    }
}

#[async_trait]
impl Runnable for PolygonGroupedDailyStager {
    #[tracing::instrument(name = "Run Polygon Grouped Daily Stager", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        stage_data(self.pool.clone())
            .map_err(UnexpectedError)
            .await?;
        Ok(None)
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn stage_data(connection_pool: PgPool) -> Result<(), anyhow::Error> {
    let partitions = get_existing_partitions(&connection_pool).await?;
    get_stageable_data(&connection_pool).await?;
    // stage_nulls(&connection_pool).await?;

    //Copy initial public offering dates to master data
    // move_ipo_date_to_master_data(&connection_pool).await?;

    //Mark as staged in
    // mark_ipo_date_as_staged(&connection_pool).await?;
    Ok(())
}

// select pgd.symbol, pgd."open", pgd."close", pgd.business_date,  pgd.stock_volume, pgd.traded_volume from polygon_grouped_daily pgd where pgd.is_staged = false;

#[tracing::instrument(level = "debug", skip_all)]
async fn get_stageable_data(connection_pool: &PgPool) -> Result<Vec<u32>, anyhow::Error> {
    let mut a = sqlx::query!(
        r##"select pgd.symbol, pgd."open", pgd."close", pgd.business_date, pgd.stock_volume, pgd.traded_volume from polygon_grouped_daily pgd where pgd.is_staged = false limit 30"##).fetch(connection_pool);
    while let Some(batch) = a.as_mut().chunks(10).next().await {
        let f: Vec<market_data> = batch
            .into_iter()
            .filter(|x| x.is_ok())
            .map(|x| {
                let a = x.expect("Checked before");
                return market_data_builder::builder()
                    .symbol(a.symbol)
                    .stock_price(a.close.clone())
                    .open(Some(a.open))
                    .close(Some(a.close))
                    .business_date(a.business_date)
                    .shares_traded(a.stock_volume)
                    .volume_trade(Some(a.traded_volume))
                    .build()
                    .unwrap();
            })
            .collect();

        // foo(batch).await; // Pass each chunk of rows to foo
        println!("Batch result: {:?}", f);
    }

    Ok(vec![])
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_existing_partitions(connection_pool: &PgPool) -> Result<Vec<u32>, anyhow::Error> {
    let oid = get_table_oid(connection_pool).await?;
    get_existing_partition_ranges_for_oid(connection_pool, oid).await
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_table_oid(connection_pool: &PgPool) -> Result<u32, anyhow::Error> {
    let result = sqlx::query!(
        "select c.oid
    from pg_catalog.pg_class c
    where relname ='market_data'"
    )
    .fetch_one(connection_pool)
    .await?;

    Ok(result.oid.0)
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_existing_partition_ranges_for_oid(
    connection_pool: &PgPool,
    table_oid: u32,
) -> Result<Vec<u32>, anyhow::Error> {
    let help_int: i64 = table_oid.into();
    info!("table id: {}", help_int);
    // Returns strings of type "FOR VALUES IN (200002)" or "DEFAULT"
    let result = sqlx::query!(
        "SELECT
            pg_catalog.pg_get_expr(c.relpartbound, c.oid)
        FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
        WHERE c.oid = i.inhrelid
        AND i.inhparent = $1::int8",
        help_int
    )
    .fetch_all(connection_pool)
    .await?;
    let a: Vec<u32> = result
        .into_iter()
        .filter(|x| {
            x.pg_get_expr
                .as_ref()
                .expect("Not expecting empty partition information")
                .ne("DEFAULT")
        })
        .map(|x| {
            let partition_info = x
                .pg_get_expr
                .expect("Not expecting empty partition information");
            let partition_info = partition_info
                .replace("FOR VALUES IN (", "")
                .replace(")", "");
            return partition_info.parse::<u32>().unwrap();
        })
        .collect();
    Ok(a)
}

// #[tracing::instrument(level = "debug", skip_all)]
// async fn stage_nulls(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
//     sqlx::query!(
//         "update financialmodelingprep_company_profile
//             set is_staged = true
//             where ipo_date is null"
//     )
//     .execute(connection_pool)
//     .await?;
//     Ok(())
// }

/// Take initial public offering dates from financialmodelingprep_company_profile and copy to master data table.
// #[tracing::instrument(level = "debug", skip_all)]
// async fn move_ipo_date_to_master_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
//     sqlx::query!(r##"
//      update master_data md set
//         start_nyse                         = case WHEN md.start_nyse is not null then r.ipo_date end,
//         start_nyse_arca                    = case WHEN md.start_nyse_arca is not null then r.ipo_date end,
//         start_nyse_american                = case WHEN md.start_nyse_american is not null then r.ipo_date end,
//         start_nasdaq_global_select_market  = case WHEN md.start_nasdaq_global_select_market is not null then r.ipo_date end,
//         start_nasdaq_select_market         = case WHEN md.start_nasdaq_select_market is not null then r.ipo_date end,
//         start_nasdaq_capital_market        = case WHEN md.start_nasdaq_capital_market is not null then r.ipo_date end,
//         start_nasdaq                       = case WHEN md.start_nasdaq is not null then r.ipo_date end,
//         start_cboe                         = case WHEN md.start_cboe is not null then r.ipo_date end
//     from
//         (select fcp.ipo_date, md.issue_symbol, md.start_nyse, md.start_nyse_arca, md.start_nyse_american, md.start_nasdaq, md.start_nasdaq_global_select_market, md.start_nasdaq_select_market, md.start_nasdaq_capital_market, md.start_cboe
//             from master_data md
//             join financialmodelingprep_company_profile fcp
//             on md.issue_symbol = fcp.symbol
//             where fcp.is_staged = false and fcp.ipo_date is not null
//         ) as r
//     where md.issue_symbol = r.issue_symbol;"##)
//     .execute(connection_pool)
//     .await?;
//     Ok(())
// }

///Select the master data with non start date 1792-05-17 and mark corresponding entries in 1792-05-17 financialmodelingprep_company_profile as staged.
// #[tracing::instrument(level = "debug", skip_all)]
// async fn mark_ipo_date_as_staged(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
//     sqlx::query!(
//         r##"
//         update financialmodelingprep_company_profile fcp set
//         is_staged = true
//         from (
//           select issue_symbol
//           from master_data md
//           where
//              start_nyse != '1792-05-17'
//           or start_nyse_arca != '1792-05-17'
//           or start_nyse_american != '1792-05-17'
//           or start_nasdaq_global_select_market != '1792-05-17'
//           or start_nasdaq_select_market != '1792-05-17'
//           or start_nasdaq_capital_market != '1792-05-17'
//           or start_nasdaq != '1792-05-17'
//           or start_cboe != '1792-05-17'
//           ) as r
//         where fcp.symbol = r.issue_symbol and fcp.is_staged = false"##
//     )
//     .execute(connection_pool)
//     .await?;
//     Ok(())
// }

#[cfg(test)]
mod test {
    use sqlx::{Pool, Postgres};

    use crate::actions::stage::polygon_grouped_daily::{
        get_existing_partition_ranges_for_oid, get_table_oid,
    };

    #[sqlx::test()]
    async fn given_market_data_table_then_returns_oid(pool: Pool<Postgres>) {
        let oid = get_table_oid(&pool).await;
        assert!(oid.is_ok());
    }

    #[sqlx::test()]
    async fn given_table_without_partitions_then_returns_0_partitions(pool: Pool<Postgres>) {
        let oid = get_table_oid(&pool).await.unwrap();
        let partitions = get_existing_partition_ranges_for_oid(&pool, oid).await;
        assert!(partitions.is_ok());
        assert_eq!(partitions.unwrap().len(), 0);
    }

    #[sqlx::test(fixtures(
            "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_add_partitions.sql"
        ))]
    async fn given_table_with_partitions_then_returns_correct_partitions(pool: Pool<Postgres>) {
        let oid = get_table_oid(&pool).await.unwrap();
        let partitions = get_existing_partition_ranges_for_oid(&pool, oid).await;
        assert!(partitions.is_ok());
        assert_eq!(partitions.as_ref().unwrap().len(), 3);
        assert!(partitions.as_ref().unwrap().contains(&202401));
        assert!(partitions.as_ref().unwrap().contains(&202402));
        assert!(partitions.as_ref().unwrap().contains(&200002));
    }
}
