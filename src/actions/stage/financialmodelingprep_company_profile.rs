use async_trait::async_trait;
use futures_util::TryFutureExt;

use sqlx::PgPool;
use std::fmt::Display;

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

#[derive(Clone, Debug)]
pub struct FinancialmodelingprepCompanyProfileStager {
    pool: PgPool,
}

impl FinancialmodelingprepCompanyProfileStager {
    pub fn new(pool: PgPool) -> Self {
        FinancialmodelingprepCompanyProfileStager { pool }
    }
}

impl Display for FinancialmodelingprepCompanyProfileStager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FinancialmodelingprepCompanyProfileStager struct.")
    }
}

#[async_trait]
impl Runnable for FinancialmodelingprepCompanyProfileStager {
    #[tracing::instrument(name = "Run financialmodelingprep Company Profile Stager", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        stage_data(self.pool.clone())
            .map_err(UnexpectedError)
            .await?;
        Ok(None)
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn stage_data(connection_pool: PgPool) -> Result<(), anyhow::Error> {
    stage_nulls(&connection_pool).await?;

    //Copy initial public offering dates to master data
    move_ipo_date_to_master_data(&connection_pool).await?;

    //Mark as staged in
    mark_ipo_date_as_staged(&connection_pool).await?;
    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
async fn stage_nulls(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        "update financialmodelingprep_company_profile 
            set is_staged = true
            where ipo_date is null"
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Take initial public offering dates from financialmodelingprep_company_profile and copy to master data table.
#[tracing::instrument(level = "debug", skip_all)]
async fn move_ipo_date_to_master_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(r##"
     update master_data md set 
        start_nyse                         = case WHEN md.start_nyse is not null then r.ipo_date end,
        start_nyse_arca                    = case WHEN md.start_nyse_arca is not null then r.ipo_date end,
        start_nyse_american                = case WHEN md.start_nyse_american is not null then r.ipo_date end,
        start_nasdaq_global_select_market  = case WHEN md.start_nasdaq_global_select_market is not null then r.ipo_date end,
        start_nasdaq_select_market         = case WHEN md.start_nasdaq_select_market is not null then r.ipo_date end,
        start_nasdaq_capital_market        = case WHEN md.start_nasdaq_capital_market is not null then r.ipo_date end,
        start_nasdaq                       = case WHEN md.start_nasdaq is not null then r.ipo_date end,
        start_cboe                         = case WHEN md.start_cboe is not null then r.ipo_date end  
    from 
        (select fcp.ipo_date, md.issue_symbol, md.start_nyse, md.start_nyse_arca, md.start_nyse_american, md.start_nasdaq, md.start_nasdaq_global_select_market, md.start_nasdaq_select_market, md.start_nasdaq_capital_market, md.start_cboe
            from master_data md 
            join financialmodelingprep_company_profile fcp 
            on md.issue_symbol = fcp.symbol 
            where fcp.is_staged = false and fcp.ipo_date is not null
        ) as r
    where md.issue_symbol = r.issue_symbol;"##)
    .execute(connection_pool)
    .await?;
    Ok(())
}

///Select the master data with non start date 1792-05-17 and mark corresponding entries in 1792-05-17 financialmodelingprep_company_profile as staged.
#[tracing::instrument(level = "debug", skip_all)]
async fn mark_ipo_date_as_staged(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"
        update financialmodelingprep_company_profile fcp set 
        is_staged = true 
        from (
          select issue_symbol   
          from master_data md 
          where  
             start_nyse != '1792-05-17'
          or start_nyse_arca != '1792-05-17'
          or start_nyse_american != '1792-05-17'
          or start_nasdaq_global_select_market != '1792-05-17'
          or start_nasdaq_select_market != '1792-05-17'
          or start_nasdaq_capital_market != '1792-05-17'
          or start_nasdaq != '1792-05-17'
          or start_cboe != '1792-05-17'
          ) as r
        where fcp.symbol = r.issue_symbol and fcp.is_staged = false"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    // use chrono::NaiveDate;
    // use sqlx::{Pool, Postgres};

    // use crate::actions::stage::sec_companies::{
    //     derive_country_from_sec_code, mark_otc_issuers_as_staged, stage_data,
    // };

    // use super::{move_issuers_to_master_data, move_otc_issues_to_master_data};

    // #[derive(Debug)]
    // struct SecCompanyRow {
    //     cik: i32,
    //     sic: Option<i32>,
    //     name: String,
    //     ticker: String,
    //     exchange: Option<String>,
    //     state_of_incorporation: Option<String>,
    //     date_loaded: NaiveDate,
    //     is_staged: bool,
    // }

    // #[derive(Debug)]
    // #[allow(dead_code)]
    // struct MasterDataRow {
    //     issuer_name: String,
    //     issue_symbol: String,
    //     location: Option<String>,
    //     start_nyse: Option<NaiveDate>,
    //     start_nyse_arca: Option<NaiveDate>,
    //     start_nyse_american: Option<NaiveDate>,
    //     start_nasdaq: Option<NaiveDate>,
    //     start_nasdaq_global_select_market: Option<NaiveDate>,
    //     start_nasdaq_select_market: Option<NaiveDate>,
    //     start_nasdaq_capital_market: Option<NaiveDate>,
    //     start_cboe: Option<NaiveDate>,
    //     instrument: Option<String>,
    //     category: Option<String>,
    //     renamed_to_issuer_name: Option<String>,
    //     renamed_to_issue_symbol: Option<String>,
    //     renamed_at_date: Option<NaiveDate>,
    //     current_name: Option<String>,
    //     suspended: Option<bool>,
    //     suspension_date: Option<NaiveDate>,
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql")
    // ))]
    // async fn given_data_in_sec_companies_when_issuers_staged_then_issuers_in_master_data(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Entry should not be in master data table
    //     let db_result = sqlx::query_as!(MasterDataRow, "select * from master_data")
    //         .fetch_all(&pool)
    //         .await
    //         .unwrap();
    //     assert_eq!(
    //         is_row_in_master_data("VIRCO MFG CORPORATION", "VIRC", None, None, db_result),
    //         false
    //     );

    //     // Staging
    //     move_issuers_to_master_data(&pool).await.unwrap();

    //     //Entry should be in master data table
    //     let db_result = sqlx::query_as!(MasterDataRow, "select * from master_data")
    //         .fetch_all(&pool)
    //         .await
    //         .unwrap();
    //     assert_eq!(
    //         is_row_in_master_data("VIRCO MFG CORPORATION", "VIRC", None, None, db_result),
    //         true
    //     );
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "sec_companies_staged.sql")
    // ))]
    // async fn given_data_in_sec_companies_when_issuers_staged_then_staged_issuers_not_in_master_data(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Given staged entry in sec_companies
    //     let sec_result: SecCompanyRow = sqlx::query_as!(
    //         SecCompanyRow,
    //         "select * from sec_companies where cik = 1098462"
    //     )
    //     .fetch_one(&pool)
    //     .await
    //     .unwrap();
    //     assert!(sec_result.is_staged == true);
    //     //Given missing entry in master data
    //     let md_result: Vec<MasterDataRow> = sqlx::query_as!(
    //         MasterDataRow,
    //         "select * from master_data where issuer_name = 'METALINK LTD'"
    //     )
    //     .fetch_all(&pool)
    //     .await
    //     .unwrap();
    //     assert_eq!(md_result.len(), 0);
    //     //Stage sec_companies
    //     move_issuers_to_master_data(&pool).await.unwrap();
    //     //Entry is still not in master data (Since already marked as staged in sec_companies)
    //     let md_result: Vec<MasterDataRow> = sqlx::query_as!(
    //         MasterDataRow,
    //         "select * from master_data where issuer_name = 'METALINK LTD'"
    //     )
    //     .fetch_all(&pool)
    //     .await
    //     .unwrap();
    //     assert_eq!(md_result.len(), 0);
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "master_data_without_otc_category.sql")
    // ))]
    // async fn given_data_in_sec_companies_and_issuers_in_master_data_when_otc_staged_then_issuers_as_otc_in_master_data(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Given master data entry with NULL in category and 'is_company'
    //     let md_result: Vec<MasterDataRow> =
    //         sqlx::query_as!(MasterDataRow, "select * from master_data")
    //             .fetch_all(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(
    //         is_row_in_master_data("VIVEVE MEDICAL, INC.", "VIVE", Some("USA"), None, md_result,),
    //         true
    //     );
    //     //Stage OTC info to master data
    //     move_otc_issues_to_master_data(&pool).await.unwrap();
    //     //Master data entry now contains OTC category and is_company == false
    //     let md_result: MasterDataRow = sqlx::query_as!(
    //         MasterDataRow,
    //         "select * from master_data where issuer_name = 'VIVEVE MEDICAL, INC.'"
    //     )
    //     .fetch_one(&pool)
    //     .await
    //     .unwrap();
    //     assert_eq!(md_result.instrument.as_deref(), Some("OTC"));
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts(
    //         "sec_companies_unstaged.sql",
    //         "sec_companies_staged.sql",
    //         "master_data_without_otc_category.sql"
    //     )
    // ))]
    // async fn given_some_staged_data_in_sec_companies_and_issuers_in_master_data_when_otc_staged_then_some_issuers_as_otc_in_master_data(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Given staged OTC entry in sec companies, but missing OTC info in master data
    //     let sc_result = sqlx::query!("Select * from sec_companies where name = 'METALINK LTD'")
    //         .fetch_one(&pool)
    //         .await
    //         .unwrap();
    //     assert_eq!(sc_result.is_staged, true);
    //     assert_eq!(sc_result.exchange.as_deref(), Some("OTC"));

    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'METALINK LTD'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.instrument, None);

    //     //Staging OTC
    //     move_otc_issues_to_master_data(&pool).await.unwrap();

    //     //Staged OTC sec company entry got not staged and master data entry contains no OTC
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'METALINK LTD'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.instrument, None);

    //     //Unstaged OTC sec company entry got staged and master data entry contains OTC
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.instrument.as_deref(), Some("OTC"));
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "master_data_without_country_code.sql")
    // ))]
    // async fn given_data_in_sec_companies_and_issuers_in_master_data_when_country_derived_then_issuers_have_county_in_master_data(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Given entry with missing country in master data
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIRCO MFG CORPORATION'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.location, None);

    //     //Staging country
    //     derive_country_from_sec_code(&pool).await.unwrap();

    //     //Entry now contains country info
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIRCO MFG CORPORATION'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.location.as_deref(), Some("USA"));
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_staged.sql", "master_data_without_country_code.sql")
    // ))]
    // async fn given_some_staged_data_in_sec_companies_and_issuers_in_master_data_when_country_derived_then_some_issuers_as_otc_in_master_data(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Given staged data + country info in sec companies (and exists in master data - tested in last part )
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'America Great Health'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(sc_result.state_of_incorporation.as_deref(), Some("CA"));
    //     assert_eq!(sc_result.is_staged, true);

    //     //Staging country
    //     derive_country_from_sec_code(&pool).await.unwrap();

    //     //Entry exists and still contains no country data
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'America Great Health'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.location, None);
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "master_data_without_country_code.sql")
    // ))]
    // async fn given_data_in_sec_companies_and_otc_issuers_in_master_data_when_mark_staged_sec_companies_then_otc_sec_companies_marked_staged(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Master data contains entry with OTC instrument and sec_companys corresponding entry is unstaged
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name ='VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.instrument.as_deref(), Some("OTC"));
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(sc_result.is_staged, false);

    //     //Marking sec companies with OTC staged
    //     mark_otc_issuers_as_staged(&pool).await.unwrap();

    //     //Sec company entry is now staged
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(sc_result.is_staged, true);
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql")
    // ))]
    // async fn given_data_in_sec_companies_when_full_staging_then_staged_master_data_and_marked_sec_companies(
    //     pool: Pool<Postgres>,
    // ) {
    //     //Given unstaged data in sec companies and empty master data
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(sc_result.is_staged, false);

    //     let md_result = sqlx::query!("select *  from master_data")
    //         .fetch_all(&pool)
    //         .await
    //         .unwrap();
    //     assert_eq!(md_result.len(), 0);
    //     // Stage everything
    //     stage_data(pool.clone()).await.unwrap();

    //     // Data with OTC and country in master data and
    //     // otc marked as staged in sec companies
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(md_result.instrument.as_deref(), Some("OTC"));
    //     assert_eq!(md_result.location.as_deref(), Some("USA"));

    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await
    //             .unwrap();
    //     assert_eq!(sc_result.is_staged, true);
    // }

    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_staged.sql")
    // ))]
    // async fn check_correct_fixture_loading(pool: Pool<Postgres>) {
    //     let result: Vec<SecCompanyRow> =
    //         sqlx::query_as!(SecCompanyRow, "select * from sec_companies")
    //             .fetch_all(&pool)
    //             .await
    //             .unwrap();
    //     assert!(is_row_in_sec_company_data(
    //         1098009,
    //         Some(2834),
    //         "America Great Health",
    //         "AAGH",
    //         Some("OTC"),
    //         Some("CA"),
    //         "2023-12-12",
    //         true,
    //         result,
    //     ));
    // }

    // fn is_row_in_master_data(
    //     issuer_name: &str,
    //     issue_symbol: &str,
    //     location: Option<&str>,
    //     instrument: Option<&str>,
    //     master_data: Vec<MasterDataRow>,
    // ) -> bool {
    //     master_data.iter().any(|row| {
    //         row.issuer_name.eq(issuer_name)
    //             && row.issue_symbol.eq(issue_symbol)
    //             && row.location.as_deref().eq(&location)
    //             && row.instrument.as_deref().eq(&instrument)
    //     })
    // }

    // fn is_row_in_sec_company_data(
    //     cik: i32,
    //     sic: Option<i32>,
    //     name: &str,
    //     ticker: &str,
    //     exchange: Option<&str>,
    //     state_of_incorporation: Option<&str>,
    //     date_loaded: &str,
    //     is_staged: bool,
    //     result: Vec<SecCompanyRow>,
    // ) -> bool {
    //     result.iter().any(|row| {
    //         row.cik == cik
    //             && row.sic.eq(&sic)
    //             && row.name.eq(name)
    //             && row.ticker.eq(ticker)
    //             && row.exchange.as_deref().eq(&exchange)
    //             && row
    //                 .state_of_incorporation
    //                 .as_deref()
    //                 .eq(&state_of_incorporation)
    //             && row
    //                 .date_loaded
    //                 .eq(&NaiveDate::parse_from_str(date_loaded, "%Y-%m-%d")
    //                     .expect("Parsing constant."))
    //             && row.is_staged.eq(&is_staged)
    //     })
    // }
}
