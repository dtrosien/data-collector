use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
use futures_util::{TryFutureExt, TryStreamExt};

use rand::Error;
use sqlx::postgres::PgQueryResult;
use sqlx::types::BigDecimal;
use sqlx::{PgPool, Postgres};
// use tokio_stream::StreamExt;
use futures_util::StreamExt;
use std::clone;
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use tracing::{error, info};

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};
use futures::future::join_all;

#[derive(Clone, Debug)]
pub struct MarketDataTransposed {
    symbol: Vec<String>,
    business_date: Vec<NaiveDate>,
    year_month: Vec<Option<i32>>,
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
pub struct MarketData {
    symbol: String,
    business_date: NaiveDate,
    year_month: Option<i32>,
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
pub struct MarketDataBuilder {
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

impl MarketDataBuilder {
    fn builder() -> MarketDataBuilder {
        MarketDataBuilder {
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

    fn build(self) -> Result<MarketData, Error> {
        if self.symbol.is_some() && self.business_date.is_some() && self.stock_price.is_some() {
            return Ok(MarketData {
                symbol: self.symbol.expect("Checked earlier"),
                business_date: self.business_date.expect("Checked earlier"),
                year_month: Self::calculate_year_month(
                    self.business_date.expect("Checked earlier"),
                ),
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

    fn calculate_year_month(value: NaiveDate) -> Option<i32> {
        Some(TryInto::<i32>::try_into(value.year_ce().1 * 100 + value.month0() + 1).unwrap())
    }
}

impl Into<MarketDataTransposed> for Vec<MarketData> {
    fn into(self) -> MarketDataTransposed {
        let mut result = MarketDataTransposed {
            symbol: vec![],
            business_date: vec![],
            year_month: vec![],
            stock_price: vec![],
            open: vec![],
            close: vec![],
            volume_trade: vec![],
            shares_traded: vec![],
            after_hours: vec![],
            pre_market: vec![],
            market_capitalization: vec![],
        };
        self.into_iter().for_each(|x| {
            result.symbol.push(x.symbol);
            result.business_date.push(x.business_date);
            result.year_month.push(x.year_month);
            result.stock_price.push(x.stock_price);
            result.open.push(x.open);
            result.close.push(x.close);
            result.volume_trade.push(x.volume_trade);
            result.shares_traded.push(x.shares_traded);
            result.after_hours.push(x.after_hours);
            result.pre_market.push(x.pre_market);
            result.market_capitalization.push(x.market_capitalization);
        });
        result
    }
}

impl MarketDataTransposed {
    fn extract_partitions(&self) -> HashSet<u32> {
        let mut set: HashSet<u32> = HashSet::new();
        self.business_date.iter().for_each(|x| {
            let month = x.month();
            let year = x.year_ce().1;
            let year_month = year * 100 + month;
            set.insert(year_month);
        });
        set
        // Vec::from_iter(set)
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
        stage_data(&self.pool).map_err(UnexpectedError).await?;
        Ok(None)
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn stage_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    let partitions: HashSet<u32> = get_existing_partitions(&connection_pool).await?;
    info!("Partitions found: {:?}", partitions);
    let input_data = get_stageable_data(&connection_pool);
    stage_data_stream(&connection_pool, input_data, partitions).await?;
    // stage_nulls(&connection_pool).await?;

    //Copy initial public offering dates to master data
    // move_ipo_date_to_master_data(&connection_pool).await?;

    //Mark as staged in
    // mark_ipo_date_as_staged(&connection_pool).await?;
    Ok(())
}

async fn stage_data_stream<'a>(
    connection_pool: &'a PgPool,
    mut input_data: Pin<
        Box<
            dyn futures_util::Stream<Item = Result<polygon_grouped_daily_table, sqlx::Error>>
                + 'a
                + std::marker::Send,
        >,
    >,
    mut existing_partitions: HashSet<u32>,
) -> Result<(), anyhow::Error> {
    while let Some(batch_grouped_daily) = input_data.as_mut().chunks(10000).next().await {
        let batch_market_data: Vec<MarketData> = batch_grouped_daily
            .into_iter()
            .filter(|grouped_daily_entry| grouped_daily_entry.is_ok())
            .map(|grouped_daily_entry| {
                let grouped_daily = grouped_daily_entry.expect("Checked before");
                return MarketDataBuilder::builder()
                    .symbol(grouped_daily.symbol)
                    .stock_price(grouped_daily.close.clone())
                    .open(Some(grouped_daily.open))
                    .close(Some(grouped_daily.close))
                    .business_date(grouped_daily.business_date)
                    .shares_traded(grouped_daily.stock_volume)
                    .volume_trade(Some(grouped_daily.traded_volume))
                    .build()
                    .unwrap();
            })
            .collect();
        let market_data_transposed: MarketDataTransposed = batch_market_data.into();
        let batch_partitions = market_data_transposed.extract_partitions();
        let new_partitions: HashSet<&u32> =
            batch_partitions.difference(&existing_partitions).collect();
        if new_partitions.len() > 0 {
            // Create partitions
            let result_partition_creation = futures::future::join_all(new_partitions.iter().map(
                |partition_value| async move {
                    let result = create_partition(&connection_pool, &partition_value).await;
                    match result {
                        Ok(_) => return true,
                        Err(_) => {
                            error!(
                                "Error while creating partition {}: {:?}",
                                partition_value, result
                            );
                            return false;
                        }
                    }
                },
            ))
            .await;
            if result_partition_creation.iter().any(|&x| x == false) {
                return Err(anyhow::Error::msg(
                    "Failed to create partition for market data.",
                ));
            }
            // Add created partitions to `partitions`
            existing_partitions.extend(&batch_partitions);
        }
        // Add data
        sqlx::query!(
                r##"INSERT INTO market_data
                (symbol, business_date, stock_price, "open", "close", volume_trade, shares_traded, after_hours, pre_market, market_capitalization)
                Select * from UNNEST($1::text[], $2::date[], $3::float[], $4::float[], $5::float[], $6::float[], $7::float[], $8::float[], $9::float[], $10::float[]) on conflict do nothing;"##,
                &market_data_transposed.symbol,
                &market_data_transposed.business_date,
                market_data_transposed.stock_price as _,
                market_data_transposed.open as _,
                market_data_transposed.close as _,
                market_data_transposed.volume_trade as _,
                market_data_transposed.shares_traded as _,
                market_data_transposed.after_hours as _,
                market_data_transposed.pre_market as _,
                market_data_transposed.market_capitalization as _,
            ).execute(connection_pool).await?;
        // println!("Batch result: {:?}", market_data_transposed);s
    }

    Ok(())
}

// select pgd.symbol, pgd."open", pgd."close", pgd.business_date,  pgd.stock_volume, pgd.traded_volume from polygon_grouped_daily pgd where pgd.is_staged = false;

async fn create_partition(
    connection_pool: &PgPool,
    partition: &u32,
) -> Result<PgQueryResult, sqlx::Error> {
    let partition_name = format!("market_data_{partition}");
    let sql_create_partition_query = format!(
        "CREATE TABLE {partition_name} PARTITION OF market_data FOR VALUES in ({partition})"
    );
    Ok(sqlx::query::<Postgres>(&sql_create_partition_query)
        .execute(connection_pool)
        .await?)
}

struct polygon_grouped_daily_table {
    symbol: String,
    close: BigDecimal,
    open: BigDecimal,
    business_date: NaiveDate,
    stock_volume: Option<i32>,
    traded_volume: BigDecimal,
}

#[tracing::instrument(level = "debug", skip_all)]
fn get_stageable_data<'a>(
    connection_pool: &'a PgPool,
) -> Pin<
    Box<
        dyn futures_util::Stream<Item = Result<polygon_grouped_daily_table, sqlx::Error>>
            + 'a
            + std::marker::Send,
    >,
> {
    // Pin<Box<dyn futures_util::Stream<Item = Result<polygon_grouped_daily_table, sqlx::Error>> + std::marker::Send>>
    let a = sqlx::query_as!( polygon_grouped_daily_table,
        r##"select pgd.symbol, pgd."open", pgd."close", pgd.business_date, pgd.stock_volume, pgd.traded_volume from polygon_grouped_daily pgd where pgd.is_staged = false"##).fetch(connection_pool);
    // while let Some(batch) = a.as_mut().chunks(10).next().await {
    //     let f: Vec<MarketData> = batch
    //         .into_iter()
    //         .filter(|x| x.is_ok())
    //         .map(|x| {
    //             let a = x.expect("Checked before");
    //             return MarketDataBuilder::builder()
    //                 .symbol(a.symbol)
    //                 .stock_price(a.close.clone())
    //                 .open(Some(a.open))
    //                 .close(Some(a.close))
    //                 .business_date(a.business_date)
    //                 .shares_traded(a.stock_volume)
    //                 .volume_trade(Some(a.traded_volume))
    //                 .build()
    //                 .unwrap();
    //         })
    //         .collect();
    //     let a : MarketDataTransposed = f.into();
    //     // foo(batch).await; // Pass each chunk of rows to foo
    //     println!("Batch result: {:?}", a);
    // }

    return a;
    // Ok(vec![])
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_existing_partitions(connection_pool: &PgPool) -> Result<HashSet<u32>, anyhow::Error> {
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
) -> Result<HashSet<u32>, anyhow::Error> {
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
    let a: HashSet<u32> = result
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
        create_partition, get_existing_partition_ranges_for_oid, get_table_oid,
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

    #[sqlx::test()]
    fn given_table_when_partitions_created_then_detects_partitions(pool: Pool<Postgres>) {
        let partition_value: u32 = 202311;
        let oid = get_table_oid(&pool).await.unwrap();
        let creation_result = create_partition(&pool, &partition_value).await;
        let partitions = get_existing_partition_ranges_for_oid(&pool, oid).await;
        assert!(creation_result.is_ok());
        assert_eq!(partitions.as_ref().unwrap().len(), 1);
        assert!(partitions.as_ref().unwrap().contains(&partition_value))
    }
}