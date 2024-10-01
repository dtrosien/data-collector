use async_trait::async_trait;
use futures_util::TryFutureExt;

use sqlx::PgPool;
use std::fmt::Display;

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

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

    // stage_nulls(&connection_pool).await?;

    //Copy initial public offering dates to master data
    // move_ipo_date_to_master_data(&connection_pool).await?;

    //Mark as staged in
    // mark_ipo_date_as_staged(&connection_pool).await?;
    Ok(())
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
