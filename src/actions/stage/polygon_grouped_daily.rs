use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
use futures_util::TryFutureExt;

use num_bigint::ToBigInt;
use rand::Error;
use sqlx::postgres::PgQueryResult;
use sqlx::types::BigDecimal;
use sqlx::{PgPool, Postgres};
// use tokio_stream::StreamExt;
use futures_util::StreamExt;
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use tracing::{debug, error, info};

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

#[derive(Clone, Debug)]
pub struct MarketDataTransposed {
    symbol: Vec<String>,
    business_date: Vec<NaiveDate>,
    year_month: Vec<i32>,
    stock_price: Vec<BigDecimal>,
    open: Vec<Option<BigDecimal>>,
    close: Vec<Option<BigDecimal>>,
    order_amount: Vec<Option<i32>>,
    shares_traded: Vec<Option<BigDecimal>>,
    after_hours: Vec<Option<BigDecimal>>,
    pre_market: Vec<Option<BigDecimal>>,
    market_capitalization: Vec<Option<BigDecimal>>,
}

#[derive(Clone, Debug)]
pub struct MarketData {
    symbol: String,
    business_date: NaiveDate,
    year_month: i32,
    stock_price: BigDecimal,
    open: Option<BigDecimal>,
    close: Option<BigDecimal>,
    order_amount: Option<i32>,
    shares_traded: Option<BigDecimal>,
    after_hours: Option<BigDecimal>,
    pre_market: Option<BigDecimal>,
    market_capitalization: Option<BigDecimal>,
}

impl MarketData {
    // If one of the two values is NULL and the other is 0, then the NULL gets replaced with 0.
    fn get_missing_zero_values(
        order_amount: Option<i32>,
        shares_traded: Option<BigDecimal>,
    ) -> (Option<i32>, Option<BigDecimal>) {
        let mut result = (order_amount, shares_traded);
        if order_amount.is_none()
            && result.1.is_some()
            && result.1.as_ref().unwrap() == &BigDecimal::new(0.to_bigint().unwrap(), 0)
        {
            result.0 = Some(0);
        }
        if result.1.is_none() && order_amount.is_some() && order_amount.as_ref().unwrap() == &0 {
            result.1 = Some(BigDecimal::new(0.to_bigint().unwrap(), 0));
        }
        result
    }
}

#[derive(Clone, Debug)]
struct MarketDataBuilder {
    symbol: Option<String>,
    business_date: Option<NaiveDate>,
    stock_price: Option<BigDecimal>,
    open: Option<BigDecimal>,
    close: Option<BigDecimal>,
    stock_traded: Option<BigDecimal>,
    order_amount: Option<i32>,
    after_hours: Option<BigDecimal>,
    pre_market: Option<BigDecimal>,
    market_capitalization: Option<BigDecimal>,
}

impl MarketDataBuilder {
    fn builder() -> MarketDataBuilder {
        MarketDataBuilder {
            symbol: None,
            business_date: None,
            stock_price: None,
            open: None,
            close: None,
            stock_traded: None,
            order_amount: None,
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

    fn stock_traded(mut self, stock_traded: Option<BigDecimal>) -> Self {
        self.stock_traded = stock_traded;
        self
    }

    fn order_amount(mut self, order_amount: Option<i32>) -> Self {
        self.order_amount = order_amount;
        self
    }
    fn _after_hours(mut self, after_hours: Option<BigDecimal>) -> Self {
        self.after_hours = after_hours;
        self
    }
    fn _pre_market(mut self, pre_market: Option<BigDecimal>) -> Self {
        self.pre_market = pre_market;
        self
    }
    fn _market_capitalization(mut self, market_capitalization: Option<BigDecimal>) -> Self {
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
                order_amount: self.order_amount,
                shares_traded: self.stock_traded,
                after_hours: self.after_hours,
                pre_market: self.pre_market,
                market_capitalization: self.market_capitalization,
            });
        }
        //TODO: Improve error message
        Err(Error::new("err"))
    }

    fn calculate_year_month(value: NaiveDate) -> i32 {
        TryInto::<i32>::try_into(value.year_ce().1 * 100 + value.month0() + 1).unwrap()
    }
}

impl From<Vec<MarketData>> for MarketDataTransposed {
    fn from(val: Vec<MarketData>) -> Self {
        let mut result = MarketDataTransposed {
            symbol: vec![],
            business_date: vec![],
            year_month: vec![],
            stock_price: vec![],
            open: vec![],
            close: vec![],
            order_amount: vec![],
            shares_traded: vec![],
            after_hours: vec![],
            pre_market: vec![],
            market_capitalization: vec![],
        };
        val.into_iter().for_each(|x| {
            let order_amount_shares_traded =
                MarketData::get_missing_zero_values(x.order_amount, x.shares_traded);
            result.symbol.push(x.symbol);
            result.business_date.push(x.business_date);
            result.year_month.push(x.year_month);
            result.stock_price.push(x.stock_price);
            result.open.push(x.open);
            result.close.push(x.close);
            result.order_amount.push(order_amount_shares_traded.0);
            result.shares_traded.push(order_amount_shares_traded.1);
            result.after_hours.push(x.after_hours);
            result.pre_market.push(x.pre_market);
            result.market_capitalization.push(x.market_capitalization);
        });
        result
    }
}

impl TryFrom<PolygonGroupedDailyTable> for MarketData {
    type Error = Error;

    fn try_from(grouped_daily: PolygonGroupedDailyTable) -> Result<Self, Self::Error> {
        MarketDataBuilder::builder()
            .symbol(grouped_daily.symbol)
            .stock_price(grouped_daily.close.clone())
            .open(Some(grouped_daily.open))
            .close(Some(grouped_daily.close))
            .business_date(grouped_daily.business_date)
            .order_amount(grouped_daily.order_amount)
            .stock_traded(Some(grouped_daily.stock_traded))
            .build()
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
        info!("Start polygon grouped daily stager.");
        stage_data(&self.pool).map_err(UnexpectedError).await?;
        Ok(None)
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn stage_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    // Mark already existing data as staged (comes maybe from other sources)
    debug!("Mark already existing data as staged");
    mark_staged(connection_pool).await?;

    let partitions: HashSet<u32> = get_existing_partitions(connection_pool).await?;
    debug!("Partitions found: {:?}", partitions);

    let input_data = get_stageable_data(connection_pool);
    stage_data_stream(connection_pool, input_data, partitions).await?;
    debug!("Mark new staged data");
    mark_staged(connection_pool).await?;

    stage_updatable_data(connection_pool).await?;
    mark_staged(connection_pool).await?;
    Ok(())
}

async fn stage_updatable_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(r#"
    update market_data md 
    set stock_traded = coalesce(md.stock_traded, pgd.stock_traded ),
              "open" = coalesce(md.open, pgd."open"),
             "close" = coalesce(md."close" , pgd."close"),
        order_amount = coalesce(md.order_amount , pgd.order_amount),
         stock_price = coalesce(md.stock_price , pgd."close")
    from polygon_grouped_daily pgd 
    where 
        pgd.is_staged = false 
    and pgd.symbol = md.symbol
    and pgd.business_date = md.business_date
    and md.year_month = (EXTRACT(YEAR FROM pgd.business_date) * 100) + EXTRACT(MONTH FROM pgd.business_date)"#).execute(connection_pool).await?;
    Ok(())
}

async fn stage_data_stream<'a>(
    connection_pool: &'a PgPool,
    mut input_data: Pin<
        Box<
            dyn futures_util::Stream<Item = Result<PolygonGroupedDailyTable, sqlx::Error>>
                + 'a
                + std::marker::Send,
        >,
    >,
    mut existing_partitions: HashSet<u32>,
) -> Result<(), anyhow::Error> {
    debug!("Start staging data.");
    while let Some(batch_grouped_daily) = input_data.as_mut().chunks(10000).next().await {
        let batch_market_data: Vec<MarketData> = batch_grouped_daily
            .into_iter()
            .filter(|grouped_daily_entry| grouped_daily_entry.is_ok())
            .map(|grouped_daily_entry| {
                let grouped_daily = grouped_daily_entry.expect("Checked before");
                //TODO: Handle error
                grouped_daily.try_into().unwrap()
            })
            .collect();
        let market_data_transposed: MarketDataTransposed = batch_market_data.into();
        let batch_partitions = market_data_transposed.extract_partitions();
        let new_partitions: HashSet<&u32> =
            batch_partitions.difference(&existing_partitions).collect();
        if !new_partitions.is_empty() {
            // Create partitions
            create_partitions(new_partitions, connection_pool).await?;
            // Add created partitions to `partitions`
            existing_partitions.extend(&batch_partitions);
        }
        // Add data
        // println!("Batch result: {:?}", market_data_transposed);
        add_data(connection_pool, market_data_transposed).await?;
    }
    debug!("End staging data.");
    Ok(())
}

async fn create_partitions(
    new_partitions: HashSet<&u32>,
    connection_pool: &sqlx::Pool<Postgres>,
) -> Result<(), anyhow::Error> {
    let result_partition_creation = create_partitions_on_db(new_partitions, connection_pool).await;
    if result_partition_creation.iter().any(|&x| !x) {
        return Err(anyhow::Error::msg(
            "Failed to create partition for market data.",
        ));
    }
    Ok(())
}

async fn create_partitions_on_db(
    new_partitions: HashSet<&u32>,
    connection_pool: &sqlx::Pool<Postgres>,
) -> Vec<bool> {
    futures::future::join_all(new_partitions.iter().map(|partition_value| async move {
        let result = create_partition(connection_pool, partition_value).await;
        match result {
            Ok(_) => true,
            Err(_) => {
                error!(
                    "Error while creating partition {}: {:?}",
                    partition_value, result
                );
                false
            }
        }
    }))
    .await
}

async fn create_partition(
    connection_pool: &PgPool,
    partition: &u32,
) -> Result<PgQueryResult, sqlx::Error> {
    let partition_name = format!("market_data_{partition}");
    let sql_create_partition_query = format!(
        "CREATE TABLE {partition_name} PARTITION OF market_data FOR VALUES in ({partition})"
    );
    sqlx::query::<Postgres>(&sql_create_partition_query)
        .execute(connection_pool)
        .await
}

async fn add_data(
    connection_pool: &PgPool,
    data: MarketDataTransposed,
) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"INSERT INTO market_data
        (symbol, business_date, stock_price, "open", "close", stock_traded, order_amount, after_hours, pre_market, market_capitalization, year_month)
        Select * from UNNEST($1::text[], $2::date[], $3::float[], $4::float[], $5::float[], $6::float[], $7::float[], $8::float[], $9::float[], $10::float[], $11::int[]) on conflict do nothing;"##,
        &data.symbol,
        &data.business_date,
        data.stock_price as _,
        data.open as _,
        data.close as _,
        data.shares_traded as _,
        data.order_amount as _,
        data.after_hours as _,
        data.pre_market as _,
        data.market_capitalization as _,
        data.year_month as _,
    ).execute(connection_pool).await?;
    Ok(())
}

struct PolygonGroupedDailyTable {
    symbol: String,
    close: BigDecimal,
    open: BigDecimal,
    business_date: NaiveDate,
    order_amount: Option<i32>,
    stock_traded: BigDecimal,
}

#[tracing::instrument(level = "debug", skip_all)]
fn get_stageable_data<'a>(
    connection_pool: &'a PgPool,
) -> Pin<
    Box<
        dyn futures_util::Stream<Item = Result<PolygonGroupedDailyTable, sqlx::Error>>
            + 'a
            + std::marker::Send,
    >,
> {
    // Pin<Box<dyn futures_util::Stream<Item = Result<polygon_grouped_daily_table, sqlx::Error>> + std::marker::Send>>
    let polygon_grouped_daily_stream = sqlx::query_as!( PolygonGroupedDailyTable,
        r##"select pgd.symbol, pgd."open", pgd."close", pgd.business_date, pgd.order_amount, pgd.stock_traded from polygon_grouped_daily pgd where pgd.is_staged = false"##).fetch(connection_pool);
    return polygon_grouped_daily_stream;
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
            partition_info.parse::<u32>().unwrap()
        })
        .collect();
    Ok(a)
}

#[tracing::instrument(level = "debug", skip_all)]
async fn mark_staged(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r#"update polygon_grouped_daily pgd
            set is_staged = true
            from (
                select pgd.symbol, pgd.business_date from polygon_grouped_daily pgd join market_data md on
                    md.symbol = pgd.symbol 
                and md.business_date = pgd.business_date 
                and  not (pgd."close" is not null 
                    and  md."close" is null )
                and  not (pgd."close" is not null 
                    and  md.stock_price is null )       
                and  not (pgd."open" is not null 
                    and  md."open" is null )
                and  not (pgd.order_amount is not null 
                    and  md.order_amount is null )
                and  not (pgd.stock_traded is not null 
                    and  md.stock_traded is null )
                where is_staged = false
            ) as r
            where pgd.symbol = r.symbol and pgd.business_date = r.business_date"#
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::Utc;
    use num_bigint::{BigInt, Sign::Plus, ToBigInt};
    use sqlx::{types::BigDecimal, Pool, Postgres};

    use crate::actions::stage::polygon_grouped_daily::{
        add_data, create_partition, get_existing_partition_ranges_for_oid, get_table_oid,
        mark_staged, stage_updatable_data, MarketData, MarketDataBuilder, MarketDataTransposed,
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

    #[sqlx::test()]
    fn given_empty_database_when_one_record_added_then_no_error(pool: Pool<Postgres>) {
        let stock_price = BigDecimal::new(BigInt::new(Plus, vec![1]), 1);
        let data: MarketDataTransposed = vec![MarketDataBuilder::builder()
            .symbol("A".to_string())
            .business_date(Utc::now().date_naive())
            .order_amount(Some(1))
            .stock_price(stock_price)
            .build()
            .unwrap()]
        .into();
        let result = add_data(&pool, data).await;

        assert!(result.is_ok());
    }

    #[sqlx::test()]
    fn given_empty_database_when_two_records_added_then_no_error(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let stock_price = BigDecimal::new(BigInt::new(Plus, vec![1]), 1);
        let data: MarketDataTransposed = vec![MarketDataBuilder::builder()
            .symbol("A".to_string())
            .business_date(Utc::now().date_naive())
            .order_amount(Some(1))
            .stock_price(stock_price)
            .build()
            .unwrap()]
        .into();
        let data2 = data.clone();
        add_data(&pool, data).await?;
        add_data(&pool, data2).await?;
        Ok(())
    }

    #[test]
    fn get_missing_zero_values_given_none_then_none() {
        assert_eq!(
            MarketData::get_missing_zero_values(None, None),
            (None, None)
        );
    }

    #[test]
    fn get_missing_zero_values_given_none_and_zero_then_zero_and_zero() {
        assert_eq!(
            MarketData::get_missing_zero_values(
                None,
                Some(BigDecimal::new(0.to_bigint().unwrap(), 0))
            ),
            (Some(0), Some(BigDecimal::new(0.to_bigint().unwrap(), 0)))
        );
    }

    #[test]
    fn get_missing_zero_values_given_zero_and_none_then_zero_and_zero() {
        assert_eq!(
            MarketData::get_missing_zero_values(Some(0), None),
            (Some(0), Some(BigDecimal::new(0.to_bigint().unwrap(), 0)))
        );
    }

    #[test]
    fn get_missing_zero_values_given_5_and_none_then_5_and_none() {
        assert_eq!(
            MarketData::get_missing_zero_values(Some(5), None),
            (Some(5), None)
        );
    }

    #[test]
    fn get_missing_zero_values_given_10_and_20_then_unchanged() {
        assert_eq!(
            MarketData::get_missing_zero_values(
                Some(10),
                Some(BigDecimal::new(20.to_bigint().unwrap(), 0))
            ),
            (Some(10), Some(BigDecimal::new(20.to_bigint().unwrap(), 0)))
        );
    }

    #[test]
    fn get_missing_zero_values_given_0_and_0_then_unchanged() {
        assert_eq!(
            MarketData::get_missing_zero_values(
                Some(0),
                Some(BigDecimal::new(0.to_bigint().unwrap(), 0))
            ),
            (Some(0), Some(BigDecimal::new(0.to_bigint().unwrap(), 0)))
        );
    }

    #[test]
    fn get_missing_zero_values_given_0_and_5_then_unchanged() {
        assert_eq!(
            MarketData::get_missing_zero_values(
                Some(0),
                Some(BigDecimal::new(5.to_bigint().unwrap(), 0))
            ),
            (Some(0), Some(BigDecimal::new(5.to_bigint().unwrap(), 0)))
        );
    }

    // Data exists is both and values are unstaged -> Both values staged
    // Data exists in source and Null is in target -> Marking staged does not happen
    // Data exists in source and Null is in target -> value is loaded
    // Data exists in neither table -> Staging does not change a thing
    // Data exists in neither table -> Mark data as staged
    // Data not is source, but target -> Staging leaves data there

    // Data not is source, but target -> Marking staged in source
    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
    ))]
    async fn given_data_in_both_tables_and_unstaged_status_when_mark_staged_then_values_marked_staged(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-04'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-04'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(is_staged_result);
        Ok(())
    }

    // Data exists in source and Null is in target -> Marking staged does not happen
    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
    ))]
    async fn given_missing_data_in_target_and_unstaged_status_when_mark_staged_then_no_change(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-07'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-07'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_result);
        Ok(())
    }

    // Data exists in source and Null is in target -> value is loaded
    #[sqlx::test(fixtures(
            "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
            "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
        ))]
    async fn given_missing_data_in_target_and_unstaged_status_when_staging_then_data_in_target(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let order_amount_prerequisite =
            sqlx::query!("select order_amount from market_data where business_date = '2022-03-07'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .order_amount;
        assert!(order_amount_prerequisite.is_none());
        stage_updatable_data(&pool).await?;
        let order_amount_result =
            sqlx::query!("select order_amount from market_data where business_date = '2022-03-07'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .order_amount;
        assert_eq!(order_amount_result.unwrap(), f64::from(36120));
        Ok(())
    }

    // Data exists in neither table -> Staging does not change a thing
    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
    ))]
    async fn given_missing_data_in_both_tables_when_staging_then_no_change(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let order_amount_prerequisite =
            sqlx::query!("select order_amount from market_data where business_date = '2022-03-08'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .order_amount;
        assert!(order_amount_prerequisite.is_none());
        stage_updatable_data(&pool).await?;
        let order_amount_result =
            sqlx::query!("select order_amount from market_data where business_date = '2022-03-08'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .order_amount;
        assert!(order_amount_result.is_none());
        Ok(())
    }

    // Data exists in neither table -> Mark data as staged
    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
    ))]
    async fn given_missing_data_in_both_tables_when_marking_staged_then_marked_staged(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-08'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-08'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(is_staged_result);
        Ok(())
    }

    // Data not is source, but target -> Staging leaves data there
    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
    ))]
    async fn given_missing_data_in_source_when_staging_then_no_change(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let order_amount_prerequisite =
            sqlx::query!("select order_amount from market_data where business_date = '2022-03-09'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .order_amount;
        assert_eq!(order_amount_prerequisite.unwrap(), f64::from(10));
        stage_updatable_data(&pool).await?;
        let order_amount_result =
            sqlx::query!("select order_amount from market_data where business_date = '2022-03-09'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .order_amount;
        assert_eq!(order_amount_result.unwrap(), f64::from(10));
        Ok(())
    }

    // Data not is source, but target -> Marking staged in source
    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/polygon_grouped_daily_staging/polygon_grouped_daily_data_source.sql"
    ))]
    async fn given_missing_data_in_source_when_marking_staged_then_marked_staged(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-09'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from polygon_grouped_daily where business_date = '2022-03-09'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(is_staged_result);
        Ok(())
    }
}
