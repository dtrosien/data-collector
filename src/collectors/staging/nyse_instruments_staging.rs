use crate::collectors::collector::Collector;
use crate::collectors::source_apis::nyse_instruments::NyseInstrumentCollector;
use crate::collectors::stagers::Stager;
use async_trait::async_trait;
use reqwest::Client;
use sqlx::PgPool;
use std::fmt::Display;
use tracing::info;
use tracing_log::log::log;

use crate::collectors::source_apis::sec_companies::SecCompanyCollector;
use crate::tasks::runnable::Runnable;
use crate::{
    collectors::{collector_sources, sp500_fields},
    utils::errors::Result,
};

#[derive(Clone)]
pub struct NyseInstrumentStager {
    pool: PgPool,
}

impl NyseInstrumentStager {
    pub fn new(pool: PgPool) -> Self {
        NyseInstrumentStager { pool }
    }
}

impl Display for NyseInstrumentStager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NyseInstrumentStager struct.")
    }
}

impl Stager for NyseInstrumentStager {
    /// Take fields from the matching collector
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        NyseInstrumentCollector::new(self.pool.clone(), Client::new()).get_sp_fields()
        // todo: do we really want to init a Collector here? (Client is only here so it can compile)
    }

    /// Take fields from the matching collector
    fn get_source(&self) -> collector_sources::CollectorSource {
        NyseInstrumentCollector::new(self.pool.clone(), Client::new()).get_source()
        // todo: do we really want to init a Collector here? (Client is only here so it can compile)
    }

    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Stager of source: {}", Stager::get_source(self))
    }
}

#[async_trait]
impl Runnable for NyseInstrumentStager {
    async fn run(&self) -> Result<()> {
        stage_data(self.pool.clone()).await
    }
}

pub async fn stage_data(connection_pool: PgPool) -> Result<()> {
    info!("Start staging of Nyse Instrument");
    //Mark test data as staged
    mark_test_data_as_staged(&connection_pool).await?;

    //Stage data
    mark_stock_exchange_per_stock_as_current_date(&connection_pool).await?;
    mark_non_companies_in_master_data(&connection_pool).await?;
    mark_companies_in_master_data(&connection_pool).await?;

    //Mark as staged in sec_companies
    mark_already_staged_non_company_instruments_as_staged(&connection_pool).await?;
    mark_already_staged_company_instruments_as_staged(&connection_pool).await?;

    info!("Finished staging of Nyse Instrument");
    Ok(())
}

/// Mark all nyse instrument entries with instrument type 'TEST' as staged.
async fn mark_test_data_as_staged(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        "
    update nyse_instruments 
    set 
        is_staged = true
    where
        instrument_type = 'TEST';"
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Set in master data the begin of trading date per stock exchange per stock as the current date, if possible. (Current date is picked, since there is no date information in NYSE instruments. Other source will be used for that information)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and keep the mic code (Id for stock exchanges).
/// Update master_data depending on mic_code accordingly.
async fn mark_stock_exchange_per_stock_as_current_date(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        r##"update master_data set 
                start_nyse                         = case WHEN r.mic_code = 'XNYS' then current_date end,
                start_nyse_arca                    = case WHEN r.mic_code = 'ARCX' then current_date end,
                start_nyse_american                = case WHEN r.mic_code = 'XASE' then current_date end,
                start_nasdaq_global_select_market  = case WHEN r.mic_code = 'XNGS' then current_date end,
                start_nasdaq_select_market         = case WHEN r.mic_code = 'XNMS' then current_date end,
                start_nasdaq_capital_market        = case WHEN r.mic_code = 'XNCM' then current_date end,
                start_nasdaq                       = case WHEN r.mic_code = 'XNAS' then current_date end,
                start_cboe                         = case WHEN r.mic_code in ('BATS', 'XCBO', 'BATY', 'EDGA', 'EDGX') then current_date end  
            from 
            (select ni.mic_code as mic_code, md.issuer_name as issuer_name, md.issue_symbol as issue_symbol  
                from nyse_instruments ni 
                join master_data md on 
                replace(ni.symbol_esignal_ticker,'/', '-') = md.issue_symbol
                where is_staged = false
            ) as r
            where master_data.issuer_name = r.issuer_name and master_data.issue_symbol = r.issue_symbol"##)
    .execute(connection_pool)
    .await?;
    Ok(())
}

///Take the instrument type from NYSE_instruments and mark master_data as non-company (not eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by non-eligible instrument types.
/// Mark remaining result in master_data as non-company (not eligible for S&P500)
async fn mark_non_companies_in_master_data(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        r##"
    update master_data 
    set 
        is_company = false
    from (select replace(symbol_esignal_ticker,'/','-')  as symbol_esignal_ticker
            from 
              nyse_instruments ni
            where 
              instrument_type in (
                'EXCHANGE_TRADED_FUND',
                'EXCHANGE_TRADED_NOTE',
                'INDEX',
                'NOTE',
                'PREFERRED_STOCK',
                'RIGHT',
                'TEST',
                'TRUST',
                'UNIT') and
                is_staged = false) as r
    where 
        r.symbol_esignal_ticker = issue_symbol"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Take the instrument type from NYSE_instruments and mark master_data as company (eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by eligible instrument types.
/// Mark remaining result in master_data as company (eligible for S&P500)
async fn mark_companies_in_master_data(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        r##" 
        update master_data 
        set 
            is_company = true
        from (select replace(symbol_esignal_ticker,'/','-')  as symbol_esignal_ticker
                from 
                  nyse_instruments ni
                where 
                  instrument_type in (
                    'COMMON_STOCK',
                    'EQUITY',
                    'REIT') and
                    is_staged = false) as r
        where 
            r.symbol_esignal_ticker = issue_symbol"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Take the companies=false in master_data and mark corresponding entries in NYSE instruments as staged (eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by is_company = false.
/// Mark remaining result in NYSE instruments as staged
async fn mark_already_staged_non_company_instruments_as_staged(
    connection_pool: &PgPool,
) -> Result<()> {
    sqlx::query!(
        r##" 
        update nyse_instruments 
        set 
            is_staged = true
        from (
            select replace(md.issue_symbol,'-', '/') as issue_symbol  
            from master_data md 
            join nyse_instruments ni 
                on replace(md.issue_symbol,'-', '/') = ni.symbol_esignal_ticker
            where is_company = false) as r
        where 
            instrument_type in (
            'EXCHANGE_TRADED_FUND',
            'EXCHANGE_TRADED_NOTE',
            'INDEX',
            'NOTE',
            'PREFERRED_STOCK',
            'RIGHT',
            'TEST',
            'TRUST',
            'UNIT') and 
            is_staged = false and
            symbol_esignal_ticker = r.issue_symbol"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Take the companies=true in master_data and mark corresponding entries in NYSE instruments as staged (eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by is_company = true.
/// Mark remaining result in NYSE instruments as staged
async fn mark_already_staged_company_instruments_as_staged(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        r##" 
        update nyse_instruments 
        set 
            is_staged = true
        from (
            select replace(md.issue_symbol,'-', '/') as issue_symbol  
            from master_data md 
            join nyse_instruments ni 
                on replace(md.issue_symbol,'-', '/') = ni.symbol_esignal_ticker
            where is_company = true) as r
        where 
            instrument_type in (
            'COMMON_STOCK',
            'EQUITY',
            'REIT') and 
            is_staged = false and
            symbol_esignal_ticker = r.issue_symbol"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    // use chrono::NaiveDate;
    // use sqlx::{Pool, Postgres};
    // use tracing_test::traced_test;

    // use crate::collectors::staging::sec_companies_staging::stage_data;
    // use crate::utils::errors::Result;

    // use super::{mark_stock_exchange_per_stock_as_current_date, mark_test_data_as_staged};

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
    //     is_company: Option<bool>,
    //     category: Option<String>,
    //     renamed_to_issuer_name: Option<String>,
    //     renamed_to_issue_symbol: Option<String>,
    //     renamed_at_date: Option<NaiveDate>,
    //     current_name: Option<String>,
    //     suspended: Option<bool>,
    //     suspension_date: Option<NaiveDate>,
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql")
    // ))]
    // async fn given_data_in_sec_companies_when_issuers_staged_then_issuers_in_master_data(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Entry should not be in master data table
    //     let db_result = sqlx::query_as!(MasterDataRow, "select * from master_data")
    //         .fetch_all(&pool)
    //         .await?;
    //     assert_eq!(
    //         is_row_in_master_data("VIRCO MFG CORPORATION", "VIRC", None, None, None, db_result),
    //         false
    //     );

    //     // Staging
    //     mark_test_data_as_staged(&pool).await?;

    //     //Entry should be in master data table
    //     let db_result = sqlx::query_as!(MasterDataRow, "select * from master_data")
    //         .fetch_all(&pool)
    //         .await?;
    //     assert_eq!(
    //         is_row_in_master_data("VIRCO MFG CORPORATION", "VIRC", None, None, None, db_result),
    //         true
    //     );
    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "sec_companies_staged.sql")
    // ))]
    // async fn given_data_in_sec_companies_when_issuers_staged_then_staged_issuers_not_in_master_data(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Given staged entry in sec_companies
    //     let sec_result: SecCompanyRow = sqlx::query_as!(
    //         SecCompanyRow,
    //         "select * from sec_companies where cik = 1098462"
    //     )
    //     .fetch_one(&pool)
    //     .await?;
    //     assert!(sec_result.is_staged == true);
    //     //Given missing entry in master data
    //     let md_result: Vec<MasterDataRow> = sqlx::query_as!(
    //         MasterDataRow,
    //         "select * from master_data where issuer_name = 'METALINK LTD'"
    //     )
    //     .fetch_all(&pool)
    //     .await?;
    //     assert_eq!(md_result.len(), 0);
    //     //Stage sec_companies
    //     mark_test_data_as_staged(&pool).await?;
    //     //Entry is still not in master data (Since already marked as staged in sec_companies)
    //     let md_result: Vec<MasterDataRow> = sqlx::query_as!(
    //         MasterDataRow,
    //         "select * from master_data where issuer_name = 'METALINK LTD'"
    //     )
    //     .fetch_all(&pool)
    //     .await?;
    //     assert_eq!(md_result.len(), 0);

    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "master_data_without_otc_category.sql")
    // ))]
    // async fn given_data_in_sec_companies_and_issuers_in_master_data_when_otc_staged_then_issuers_as_otc_in_master_data(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Given master data entry with NULL in category and 'is_company'
    //     let md_result: Vec<MasterDataRow> =
    //         sqlx::query_as!(MasterDataRow, "select * from master_data")
    //             .fetch_all(&pool)
    //             .await?;
    //     assert_eq!(
    //         is_row_in_master_data(
    //             "VIVEVE MEDICAL, INC.",
    //             "VIVE",
    //             Some("USA"),
    //             None,
    //             None,
    //             md_result
    //         ),
    //         true
    //     );
    //     //Stage OTC info to master data
    //     mark_stock_exchange_per_stock_as_current_date(&pool).await?;
    //     //Master data entry now contains OTC category and is_company == false
    //     let md_result: MasterDataRow = sqlx::query_as!(
    //         MasterDataRow,
    //         "select * from master_data where issuer_name = 'VIVEVE MEDICAL, INC.'"
    //     )
    //     .fetch_one(&pool)
    //     .await?;
    //     assert_eq!(md_result.category.as_deref(), Some("OTC"));
    //     assert_eq!(md_result.is_company, Some(false));
    //     Ok(())
    // }

    // #[traced_test]
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
    // ) -> Result<()> {
    //     //Given staged OTC entry in sec companies, but missing OTC info in master data
    //     let sc_result = sqlx::query!("Select * from sec_companies where name = 'METALINK LTD'")
    //         .fetch_one(&pool)
    //         .await?;
    //     assert_eq!(sc_result.is_staged, true);
    //     assert_eq!(sc_result.exchange.as_deref(), Some("OTC"));

    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'METALINK LTD'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.category, None);
    //     assert_eq!(md_result.is_company, None);

    //     //Staging OTC
    //     mark_stock_exchange_per_stock_as_current_date(&pool).await?;

    //     //Staged OTC sec company entry got not staged and master data entry contains no OTC
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'METALINK LTD'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.category, None);
    //     assert_eq!(md_result.is_company, None);
    //     //Unstaged OTC sec company entry got staged and master data entry contains OTC
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.category.as_deref(), Some("OTC"));
    //     assert_eq!(md_result.is_company, Some(false));
    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "master_data_without_country_code.sql")
    // ))]
    // async fn given_data_in_sec_companies_and_issuers_in_master_data_when_country_derived_then_issuers_have_county_in_master_data(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Given entry with missing country in master data
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIRCO MFG CORPORATION'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.location, None);

    //     //Staging country
    //     derive_country_from_sec_code(&pool).await?;

    //     //Entry now contains country info
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIRCO MFG CORPORATION'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.location.as_deref(), Some("USA"));
    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_staged.sql", "master_data_without_country_code.sql")
    // ))]
    // async fn given_some_staged_data_in_sec_companies_and_issuers_in_master_data_when_country_derived_then_some_issuers_as_otc_in_master_data(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Given staged data + country info in sec companies (and exists in master data - tested in last part )
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'America Great Health'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(sc_result.state_of_incorporation.as_deref(), Some("CA"));
    //     assert_eq!(sc_result.is_staged, true);

    //     //Staging country
    //     derive_country_from_sec_code(&pool).await?;

    //     //Entry exists and still contains no country data
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'America Great Health'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.location, None);
    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql", "master_data_without_country_code.sql")
    // ))]
    // async fn given_data_in_sec_companies_and_otc_issuers_in_master_data_when_mark_staged_sec_companies_then_otc_sec_companies_marked_staged(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Master data contains entry with OTC category and sec_companys corresponding entry is unstaged
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name ='VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.category.as_deref(), Some("OTC"));
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(sc_result.is_staged, false);

    //     //Marking sec companies with OTC staged
    //     mark_otc_issuers_as_staged(&pool).await?;

    //     //Sec company entry is now staged
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(sc_result.is_staged, true);
    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_unstaged.sql")
    // ))]
    // async fn given_data_in_sec_companies_when_full_staging_then_staged_master_data_and_marked_sec_companies(
    //     pool: Pool<Postgres>,
    // ) -> Result<()> {
    //     //Given unstaged data in sec companies and empty master data
    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(sc_result.is_staged, false);

    //     let md_result = sqlx::query!("select *  from master_data")
    //         .fetch_all(&pool)
    //         .await?;
    //     assert_eq!(md_result.len(), 0);
    //     // Stage everything
    //     stage_data(pool.clone()).await?;

    //     // Data with OTC and country in master data and
    //     // otc marked as staged in sec companies
    //     let md_result =
    //         sqlx::query!("select * from master_data where issuer_name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(md_result.category.as_deref(), Some("OTC"));
    //     assert_eq!(md_result.location.as_deref(), Some("USA"));

    //     let sc_result =
    //         sqlx::query!("select * from sec_companies where name = 'VIVEVE MEDICAL, INC.'")
    //             .fetch_one(&pool)
    //             .await?;
    //     assert_eq!(sc_result.is_staged, true);

    //     Ok(())
    // }

    // #[traced_test]
    // #[sqlx::test(fixtures(
    //     path = "../../../tests/resources/collectors/staging/sec_companies_staging",
    //     scripts("sec_companies_staged.sql")
    // ))]
    // async fn check_correct_fixture_loading(pool: Pool<Postgres>) -> Result<()> {
    //     let result: Vec<SecCompanyRow> =
    //         sqlx::query_as!(SecCompanyRow, "select * from sec_companies")
    //             .fetch_all(&pool)
    //             .await?;
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
    //     Ok(())
    // }

    // fn is_row_in_master_data(
    //     issuer_name: &str,
    //     issue_symbol: &str,
    //     location: Option<&str>,
    //     is_company: Option<bool>,
    //     category: Option<&str>,
    //     master_data: Vec<MasterDataRow>,
    // ) -> bool {
    //     master_data.iter().any(|row| {
    //         row.issuer_name.eq(issuer_name)
    //             && row.issue_symbol.eq(issue_symbol)
    //             && row.location.as_deref().eq(&location)
    //             && row.is_company.eq(&is_company)
    //             && row.category.as_deref().eq(&category)
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
