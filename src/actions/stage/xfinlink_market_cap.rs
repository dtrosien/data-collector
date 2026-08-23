use async_trait::async_trait;
use bigdecimal::FromPrimitive;
use chrono::{Datelike, NaiveDate};
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use rand::Error;
use sqlx::postgres::PgQueryResult;
use sqlx::types::BigDecimal;
use sqlx::{PgPool, Postgres};
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

    fn market_capitalization(mut self, market_capitalization: Option<BigDecimal>) -> Self {
        self.market_capitalization = market_capitalization;
        self
    }

    // xfinlink never supplies order_amount, after_hours or pre_market, so these always stay
    // None (kept here only to mirror the sibling stager structs' field set).
    fn _order_amount(mut self, order_amount: Option<i32>) -> Self {
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

    fn build(self) -> Result<MarketData, Error> {
        if let (Some(symbol), Some(business_date), Some(stock_price)) =
            (self.symbol, self.business_date, self.stock_price)
        {
            return Ok(MarketData {
                symbol,
                business_date,
                year_month: Self::calculate_year_month(business_date),
                stock_price,
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
            result.symbol.push(x.symbol);
            result.business_date.push(x.business_date);
            result.year_month.push(x.year_month);
            result.stock_price.push(x.stock_price);
            result.open.push(x.open);
            result.close.push(x.close);
            result.order_amount.push(x.order_amount);
            result.shares_traded.push(x.shares_traded);
            result.after_hours.push(x.after_hours);
            result.pre_market.push(x.pre_market);
            result.market_capitalization.push(x.market_capitalization);
        });
        result
    }
}

impl TryFrom<XfinlinkMarketCapTable> for MarketData {
    type Error = Error;

    fn try_from(xfinlink: XfinlinkMarketCapTable) -> Result<Self, Self::Error> {
        // market_data.stock_price is NOT NULL, so a row without a close price cannot be staged.
        let close = xfinlink
            .close
            .ok_or_else(|| Error::new("missing close, cannot derive mandatory stock_price"))?;
        let stock_price = BigDecimal::from_f64(close)
            .ok_or_else(|| Error::new("close could not be converted to BigDecimal"))?;

        MarketDataBuilder::builder()
            .symbol(xfinlink.symbol)
            .business_date(xfinlink.business_date)
            .stock_price(stock_price.clone())
            .open(xfinlink.open.and_then(BigDecimal::from_f64))
            .close(Some(stock_price))
            .stock_traded(xfinlink.volume.and_then(BigDecimal::from_f64))
            .market_capitalization(xfinlink.market_cap.and_then(BigDecimal::from_f64))
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
pub struct XfinlinkMarketCapStager {
    pool: PgPool,
}

impl XfinlinkMarketCapStager {
    pub fn new(pool: PgPool) -> Self {
        XfinlinkMarketCapStager { pool }
    }
}

impl Display for XfinlinkMarketCapStager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "XfinlinkMarketCapStager struct.")
    }
}

#[async_trait]
impl Runnable for XfinlinkMarketCapStager {
    #[tracing::instrument(name = "Run Xfinlink Market Cap Stager", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        info!("Start xfinlink market cap stager.");
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
    set stock_traded = coalesce(md.stock_traded, xmc.volume),
              "open" = coalesce(md.open, xmc."open"),
             "close" = coalesce(md."close" , xmc."close"),
         stock_price = coalesce(md.stock_price , xmc."close"),
       market_capitalization = coalesce(md.market_capitalization, xmc.market_cap)
    from xfinlink_market_cap xmc
    where
        xmc.is_staged = false
    and xmc.symbol = md.symbol
    and xmc.business_date = md.business_date
    and md.year_month = (EXTRACT(YEAR FROM xmc.business_date) * 100) + EXTRACT(MONTH FROM xmc.business_date)"#).execute(connection_pool).await?;
    Ok(())
}

async fn stage_data_stream<'a>(
    connection_pool: &'a PgPool,
    mut input_data: Pin<
        Box<
            dyn futures_util::Stream<Item = Result<XfinlinkMarketCapTable, sqlx::Error>>
                + 'a
                + std::marker::Send,
        >,
    >,
    mut existing_partitions: HashSet<u32>,
) -> Result<(), anyhow::Error> {
    debug!("Start staging data.");
    while let Some(batch_xfinlink) = input_data.as_mut().chunks(10000).next().await {
        let batch_market_data: Vec<MarketData> = batch_xfinlink
            .into_iter()
            .filter(|xfinlink_entry| xfinlink_entry.is_ok())
            .filter_map(|xfinlink_entry| {
                let xfinlink = xfinlink_entry.expect("Checked before");
                // Rows without a close price cannot be staged (stock_price is NOT NULL in
                // market_data); skip them rather than failing the whole batch.
                xfinlink.try_into().ok()
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

struct XfinlinkMarketCapTable {
    symbol: String,
    business_date: NaiveDate,
    open: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    market_cap: Option<f64>,
}

#[tracing::instrument(level = "debug", skip_all)]
fn get_stageable_data<'a>(
    connection_pool: &'a PgPool,
) -> Pin<
    Box<
        dyn futures_util::Stream<Item = Result<XfinlinkMarketCapTable, sqlx::Error>>
            + 'a
            + std::marker::Send,
    >,
> {
    let xfinlink_market_cap_stream = sqlx::query_as!(
        XfinlinkMarketCapTable,
        r##"select xmc.symbol, xmc.business_date, xmc."open", xmc."close", xmc.volume, xmc.market_cap
            from xfinlink_market_cap xmc where xmc.is_staged = false"##
    )
    .fetch(connection_pool);
    xfinlink_market_cap_stream
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
        r#"update xfinlink_market_cap xmc
            set is_staged = true
            from (
                select xmc.symbol, xmc.business_date from xfinlink_market_cap xmc join market_data md on
                    md.symbol = xmc.symbol
                and md.business_date = xmc.business_date
                and  not (xmc."close" is not null
                    and  md."close" is null )
                and  not (xmc."close" is not null
                    and  md.stock_price is null )
                and  not (xmc."open" is not null
                    and  md."open" is null )
                and  not (xmc.volume is not null
                    and  md.stock_traded is null )
                and  not (xmc.market_cap is not null
                    and  md.market_capitalization is null )
                where is_staged = false
            ) as r
            where xmc.symbol = r.symbol and xmc.business_date = r.business_date"#
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use bigdecimal::FromPrimitive;
    use chrono::{NaiveDate, Utc};
    use num_bigint::{BigInt, Sign::Plus};
    use sqlx::{types::BigDecimal, Pool, Postgres};

    use crate::actions::stage::xfinlink_market_cap::{
        add_data, create_partition, get_existing_partition_ranges_for_oid, get_table_oid,
        mark_staged, stage_updatable_data, MarketData, MarketDataBuilder, MarketDataTransposed,
        XfinlinkMarketCapTable,
    };

    #[test]
    fn try_from_with_close_and_all_fields_builds_market_data() {
        let table = XfinlinkMarketCapTable {
            symbol: "AAPL".to_string(),
            business_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            open: Some(10.0),
            close: Some(20.0),
            volume: Some(1000.0),
            market_cap: Some(500000.0),
        };
        let market_data: MarketData = table.try_into().expect("close is present, must build");
        let transposed: MarketDataTransposed = vec![market_data].into();
        assert_eq!(transposed.symbol, vec!["AAPL".to_string()]);
        assert_eq!(
            transposed.stock_price,
            vec![BigDecimal::from_f64(20.0).unwrap()]
        );
        assert_eq!(transposed.open, vec![BigDecimal::from_f64(10.0)]);
        assert_eq!(transposed.close, vec![BigDecimal::from_f64(20.0)]);
        assert_eq!(transposed.shares_traded, vec![BigDecimal::from_f64(1000.0)]);
        assert_eq!(
            transposed.market_capitalization,
            vec![BigDecimal::from_f64(500000.0)]
        );
        assert_eq!(transposed.order_amount, vec![None]);
        assert_eq!(transposed.after_hours, vec![None]);
        assert_eq!(transposed.pre_market, vec![None]);
    }

    #[test]
    fn try_from_missing_close_returns_err() {
        let table = XfinlinkMarketCapTable {
            symbol: "AAPL".to_string(),
            business_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            open: Some(10.0),
            close: None,
            volume: Some(1000.0),
            market_cap: Some(500000.0),
        };
        let result: Result<MarketData, _> = table.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn try_from_only_close_present_builds_market_data_with_other_fields_none() {
        let table = XfinlinkMarketCapTable {
            symbol: "AAPL".to_string(),
            business_date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            open: None,
            close: Some(20.0),
            volume: None,
            market_cap: None,
        };
        let market_data: MarketData = table.try_into().expect("close is present, must build");
        let transposed: MarketDataTransposed = vec![market_data].into();
        assert_eq!(transposed.open, vec![None]);
        assert_eq!(transposed.shares_traded, vec![None]);
        assert_eq!(transposed.market_capitalization, vec![None]);
        assert_eq!(transposed.close, vec![BigDecimal::from_f64(20.0)]);
    }

    #[test]
    fn calculate_year_month_matches_expected() {
        let table = XfinlinkMarketCapTable {
            symbol: "AAPL".to_string(),
            business_date: NaiveDate::from_ymd_opt(2024, 3, 5).unwrap(),
            open: None,
            close: Some(20.0),
            volume: None,
            market_cap: None,
        };
        let market_data: MarketData = table.try_into().unwrap();
        let transposed: MarketDataTransposed = vec![market_data].into();
        assert_eq!(transposed.year_month, vec![202403]);
    }

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
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/market_data_add_partitions.sql"
    ))]
    async fn given_table_with_partitions_then_returns_correct_partitions(pool: Pool<Postgres>) {
        let oid = get_table_oid(&pool).await.unwrap();
        let partitions = get_existing_partition_ranges_for_oid(&pool, oid).await;
        assert!(partitions.is_ok());
        assert_eq!(partitions.as_ref().unwrap().len(), 2);
        assert!(partitions.as_ref().unwrap().contains(&202401));
        assert!(partitions.as_ref().unwrap().contains(&200002));
    }

    #[sqlx::test()]
    async fn given_table_when_partitions_created_then_detects_partitions(pool: Pool<Postgres>) {
        let partition_value: u32 = 202311;
        let oid = get_table_oid(&pool).await.unwrap();
        let creation_result = create_partition(&pool, &partition_value).await;
        let partitions = get_existing_partition_ranges_for_oid(&pool, oid).await;
        assert!(creation_result.is_ok());
        assert_eq!(partitions.as_ref().unwrap().len(), 1);
        assert!(partitions.as_ref().unwrap().contains(&partition_value))
    }

    #[sqlx::test()]
    async fn given_empty_database_when_one_record_added_then_no_error(pool: Pool<Postgres>) {
        let stock_price = BigDecimal::new(BigInt::new(Plus, vec![1]), 1);
        let data: MarketDataTransposed = vec![MarketDataBuilder::builder()
            .symbol("A".to_string())
            .business_date(Utc::now().date_naive())
            .stock_price(stock_price)
            .build()
            .unwrap()]
        .into();
        let result = add_data(&pool, data).await;

        assert!(result.is_ok());
    }

    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/xfinlink_market_cap_data_source.sql"
    ))]
    async fn given_data_in_both_tables_and_unstaged_status_when_mark_staged_then_values_marked_staged(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from xfinlink_market_cap where business_date = '2022-03-04'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from xfinlink_market_cap where business_date = '2022-03-04'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(is_staged_result);
        Ok(())
    }

    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/xfinlink_market_cap_data_source.sql"
    ))]
    async fn given_missing_data_in_target_and_unstaged_status_when_mark_staged_then_no_change(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from xfinlink_market_cap where business_date = '2022-03-07'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from xfinlink_market_cap where business_date = '2022-03-07'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_result);
        Ok(())
    }

    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/xfinlink_market_cap_data_source.sql"
    ))]
    async fn given_missing_data_in_target_and_unstaged_status_when_staging_then_data_in_target(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let market_cap_prerequisite = sqlx::query!(
            "select market_capitalization from market_data where business_date = '2022-03-07'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .market_capitalization;
        assert!(market_cap_prerequisite.is_none());
        stage_updatable_data(&pool).await?;
        let market_cap_result = sqlx::query!(
            "select market_capitalization from market_data where business_date = '2022-03-07'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .market_capitalization;
        assert_eq!(market_cap_result.unwrap(), 123456.0);
        Ok(())
    }

    #[sqlx::test(fixtures(
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/market_data_update_entries.sql",
        "../../../tests/resources/collectors/staging/xfinlink_market_cap_staging/xfinlink_market_cap_data_source.sql"
    ))]
    async fn given_missing_data_in_source_when_marking_staged_then_marked_staged(
        pool: Pool<Postgres>,
    ) -> Result<(), anyhow::Error> {
        let is_staged_prerequisite = sqlx::query!(
            "select is_staged from xfinlink_market_cap where business_date = '2022-03-08'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(!is_staged_prerequisite);
        mark_staged(&pool).await?;
        let is_staged_result = sqlx::query!(
            "select is_staged from xfinlink_market_cap where business_date = '2022-03-08'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_staged;
        assert!(is_staged_result);
        Ok(())
    }
}
