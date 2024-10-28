use async_trait::async_trait;
use bigdecimal::FromPrimitive;
use chrono::Datelike;
use chrono::NaiveDate;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use num_bigint::ToBigInt;
use rand::Error;
use sqlx::postgres::PgQueryResult;
use sqlx::types::BigDecimal;
use sqlx::PgPool;
use sqlx::Postgres;
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use tracing::error;
use tracing::{debug, info};

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

#[derive(Clone, Debug)]
pub struct FinancialmodelingprepMarketCapitalizationStager {
    pool: PgPool,
}

impl FinancialmodelingprepMarketCapitalizationStager {
    pub fn new(pool: PgPool) -> Self {
        FinancialmodelingprepMarketCapitalizationStager { pool }
    }
}

impl Display for FinancialmodelingprepMarketCapitalizationStager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FinancialmodelingprepMarketCapitalizationStager struct.")
    }
}

#[derive(Clone, Debug)]
pub struct MarketDataTransposed {
    symbol: Vec<String>,
    business_date: Vec<NaiveDate>,
    year_month: Vec<i32>,
    stock_price: Vec<Option<BigDecimal>>,
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
    stock_price: Option<BigDecimal>,
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

    fn _stock_price(mut self, stock_price: BigDecimal) -> Self {
        self.stock_price = Some(stock_price);
        self
    }

    fn _open(mut self, open: Option<BigDecimal>) -> Self {
        self.open = open;
        self
    }

    fn _close(mut self, close: Option<BigDecimal>) -> Self {
        self.close = close;
        self
    }

    fn _stock_traded(mut self, stock_traded: Option<BigDecimal>) -> Self {
        self.stock_traded = stock_traded;
        self
    }

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
    fn market_capitalization(mut self, market_capitalization: Option<BigDecimal>) -> Self {
        self.market_capitalization = market_capitalization;
        self
    }

    fn build(self) -> Result<MarketData, Error> {
        if self.symbol.is_some()
            && self.business_date.is_some()
            && self.market_capitalization.is_some()
        {
            return Ok(MarketData {
                symbol: self.symbol.expect("Checked earlier"),
                business_date: self.business_date.expect("Checked earlier"),
                year_month: Self::calculate_year_month(
                    self.business_date.expect("Checked earlier"),
                ),
                stock_price: self.stock_price,
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

impl TryFrom<FinModPrepMarketCapTable> for MarketData {
    type Error = Error;

    fn try_from(finprep_market_cap: FinModPrepMarketCapTable) -> Result<Self, Self::Error> {
        MarketDataBuilder::builder()
            .symbol(finprep_market_cap.symbol)
            .business_date(finprep_market_cap.business_date)
            .market_capitalization(BigDecimal::from_f64(finprep_market_cap.market_cap))
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

#[async_trait]
impl Runnable for FinancialmodelingprepMarketCapitalizationStager {
    #[tracing::instrument(
        name = "Run financialmodelingprep market capitalization stager",
        skip(self)
    )]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
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

#[tracing::instrument(level = "debug", skip_all)]
async fn mark_staged(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        "update financialmodelingprep_market_cap fmc 
            set is_staged = true 
            from (
			  select fmc.symbol , fmc.business_date from financialmodelingprep_market_cap fmc join market_data md on 
			    md.symbol = fmc.symbol 
            and md.business_date = fmc.business_date
            and not ( fmc.market_cap is not null  
                  and md.market_capitalization is null)
                  where is_staged = false
			) as r
			where fmc.symbol = r.symbol and fmc.business_date = r.business_date"
    )
    .execute(connection_pool)
    .await?;
    Ok(())
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

struct FinModPrepMarketCapTable {
    symbol: String,
    market_cap: f64,
    business_date: NaiveDate,
}

#[tracing::instrument(level = "debug", skip_all)]
fn get_stageable_data<'a>(
    connection_pool: &'a PgPool,
) -> Pin<
    Box<
        dyn futures_util::Stream<Item = Result<FinModPrepMarketCapTable, sqlx::Error>>
            + 'a
            + std::marker::Send,
    >,
> {
    // Pin<Box<dyn futures_util::Stream<Item = Result<polygon_grouped_daily_table, sqlx::Error>> + std::marker::Send>>
    let polygon_grouped_daily_stream = sqlx::query_as!( FinModPrepMarketCapTable,
        r##"select fmc.symbol, fmc.business_date, fmc.market_cap  from financialmodelingprep_market_cap fmc where fmc.is_staged = false "##).fetch(connection_pool);
    return polygon_grouped_daily_stream;
}

async fn stage_data_stream<'a>(
    connection_pool: &'a PgPool,
    mut input_data: Pin<
        Box<
            dyn futures_util::Stream<Item = Result<FinModPrepMarketCapTable, sqlx::Error>>
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

async fn stage_updatable_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(r#"
        update market_data md 
        set market_capitalization = coalesce(md.market_capitalization , fmc.market_cap)
        from financialmodelingprep_market_cap fmc
        where 
                fmc.is_staged = false 
            and fmc.symbol = md.symbol
            and fmc.business_date = md.business_date
            and md.year_month = (EXTRACT(YEAR FROM fmc.business_date) * 100) + EXTRACT(MONTH FROM fmc.business_date)"#)
    .execute(connection_pool).await?;
    Ok(())
}

#[cfg(test)]
mod test {}
