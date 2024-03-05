//use crate::collectors::collector::Collector;

//use crate::collectors::stagers::Stager;
use async_trait::async_trait;

use serde::Deserialize;
use sqlx::PgPool;

use std::fmt::Display;
use strum::{Display, EnumIter, IntoEnumIterator};
use tracing::info;

use crate::dag_scheduler::task::TaskError::UnexpectedError;
use crate::dag_scheduler::task::{Runnable, StatsMap, TaskError};

#[derive(Debug, Deserialize, Display, EnumIter, PartialEq)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum NonCompanies {
    ExchangeTradedFund,
    ExchangeTradedNote,
    Index,
    Note,
    PreferredStock,
    Right,
    Test,
    Trust,
    Unit,
}

impl NonCompanies {
    // Return list of all enums as string
    pub fn get_all() -> Vec<String> {
        NonCompanies::iter()
            .map(|non_comp| non_comp.to_string())
            .collect()
    }
}

#[derive(Debug, Deserialize, Display, EnumIter, PartialEq)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum Companies {
    CommonStock,
    Equity,
    Reit,
}

impl Companies {
    // Return list of all enums as string
    pub fn get_all() -> Vec<String> {
        Companies::iter()
            .map(|non_comp| non_comp.to_string())
            .collect()
    }
}

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

// impl Stager for NyseInstrumentStager {
//     /// Take fields from the matching collector
//     fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
//         NyseInstrumentCollector::new(self.pool.clone(), Client::new()).get_sp_fields()
//         // todo: do we really want to init a Collector here? (Client is only here so it can compile)
//     }
//
//     /// Take fields from the matching collector
//     fn get_source(&self) -> collector_sources::CollectorSource {
//         NyseInstrumentCollector::new(self.pool.clone(), Client::new()).get_source()
//         // todo: do we really want to init a Collector here? (Client is only here so it can compile)
//     }
//
//     fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//         write!(f, "Stager of source: {}", Stager::get_source(self))
//     }
// }

#[async_trait]
impl Runnable for NyseInstrumentStager {
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        stage_data(self.pool.clone())
            .await
            .map_err(UnexpectedError)?;
        Ok(None)
    }
}

pub async fn stage_data(connection_pool: PgPool) -> Result<(), anyhow::Error> {
    info!("Start staging of Nyse Instrument");
    //Mark test data as staged
    mark_test_data_as_staged(&connection_pool).await?;

    //Stage data
    mark_stock_exchange_per_stock_as_current_date(&connection_pool).await?;
    mark_non_companies_in_master_data(&connection_pool).await?;
    mark_companies_in_master_data(&connection_pool).await?;

    //Mark as staged in nyse_instruments
    mark_already_staged_non_company_instruments_as_staged(&connection_pool).await?;
    mark_already_staged_company_instruments_as_staged(&connection_pool).await?;

    info!("Finished staging of Nyse Instrument");
    Ok(())
}

/// Mark all nyse instrument entries with instrument type 'TEST' as staged.
async fn mark_test_data_as_staged(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"
    update nyse_instruments 
    set 
        is_staged = true
    where
        instrument_type = 'TEST'"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Set in master data the begin of trading date per stock exchange per stock as the current date, if possible. (Current date is picked, since there is no date information in NYSE instruments. Other source - NYSE events - will be used for that information)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and keep the mic code (Id for stock exchanges).
/// Update master_data depending on mic_code accordingly.
async fn mark_stock_exchange_per_stock_as_current_date(
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
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

/// Take the instrument type from NYSE_instruments and mark master_data as non-company (not eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by non-eligible instrument types.
/// Mark remaining result in master_data as non-company (not eligible for S&P500)
async fn mark_non_companies_in_master_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"
    update master_data 
    set 
        is_company = false
    from (select replace(symbol_esignal_ticker,'/','-')  as symbol_esignal_ticker
            from 
              nyse_instruments ni
            where 
                    instrument_type in (Select unnest($1::text[]))
                and is_staged = false
            ) as r
    where 
        r.symbol_esignal_ticker = issue_symbol"##,
        &NonCompanies::get_all()[..]
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Take the instrument type from NYSE_instruments and mark master_data as company (eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by eligible instrument types.
/// Mark remaining result in master_data as company (eligible for S&P500)
async fn mark_companies_in_master_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##" 
        update master_data 
        set 
            is_company = true
        from    (select replace(symbol_esignal_ticker,'/','-')  as symbol_esignal_ticker
                from 
                  nyse_instruments ni
                where 
                  instrument_type in (Select unnest($1::text[]))
                    and is_staged = false
                ) as r
        where 
            r.symbol_esignal_ticker = issue_symbol"##,
        &Companies::get_all()[..]
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
) -> Result<(), anyhow::Error> {
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
                instrument_type in (Select unnest($1::text[]))
            and is_staged = false
            and symbol_esignal_ticker = r.issue_symbol"##,
        &NonCompanies::get_all()[..]
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

/// Take the companies=true in master_data and mark corresponding entries in NYSE instruments as staged (eligible for S&P500)
/// Query design: Combine the tables nyse_instruments and master_data. The column symbol_esignal_ticker in nyse_instruments is almost the same as in master_data.issue_symbol; / and - must be exchanged.
/// Join nyse_instruments and master_data by issuer_symbol, ignore already staged entries in nyse_instruments and filter by is_company = true.
/// Mark remaining result in NYSE instruments as staged
async fn mark_already_staged_company_instruments_as_staged(
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
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
                instrument_type in (Select unnest($1::text[]))
            and is_staged = false
            and symbol_esignal_ticker = r.issue_symbol"##,
        &Companies::get_all()[..]
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::{NaiveDate, Utc};
    use sqlx::{query, Pool, Postgres};
    use tracing_test::traced_test;

    use crate::collectors::staging::nyse_instruments_staging::mark_non_companies_in_master_data;
    use crate::collectors::staging::nyse_instruments_staging::{
        mark_already_staged_company_instruments_as_staged,
        mark_already_staged_non_company_instruments_as_staged, mark_companies_in_master_data,
    };

    use super::{mark_stock_exchange_per_stock_as_current_date, mark_test_data_as_staged};

    #[derive(Debug)]
    #[allow(dead_code)]
    struct MasterDataRow {
        issuer_name: String,
        issue_symbol: String,
        location: Option<String>,
        start_nyse: Option<NaiveDate>,
        start_nyse_arca: Option<NaiveDate>,
        start_nyse_american: Option<NaiveDate>,
        start_nasdaq: Option<NaiveDate>,
        start_nasdaq_global_select_market: Option<NaiveDate>,
        start_nasdaq_select_market: Option<NaiveDate>,
        start_nasdaq_capital_market: Option<NaiveDate>,
        start_cboe: Option<NaiveDate>,
        is_company: Option<bool>,
        category: Option<String>,
        renamed_to_issuer_name: Option<String>,
        renamed_to_issue_symbol: Option<String>,
        renamed_at_date: Option<NaiveDate>,
        current_name: Option<String>,
        suspended: Option<bool>,
        suspension_date: Option<NaiveDate>,
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts("nyse_instruments_unstaged.sql")
    ))]
    async fn given_unstaged_test_instrument_when_marked_staged_then_staged(pool: Pool<Postgres>) {
        //Entry should not be staged
        let instruments_result =
            sqlx::query!("select * from nyse_instruments where instrument_type = 'TEST'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instruments_result.is_staged, false);

        // Staging
        mark_test_data_as_staged(&pool).await.unwrap();

        //Entry should now be staged
        let instruments_result =
            sqlx::query!("select * from nyse_instruments where instrument_type = 'TEST'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instruments_result.is_staged, true);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_without_dates_when_dates_staged_then_dates_in_correct_columns(
        pool: Pool<Postgres>,
    ) {
        //Given staged entry in sec_companies
        let md_result = query!("select count(*) from master_data")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(md_result.count.unwrap() > 12);

        let md_result: Vec<MasterDataRow> = sqlx::query_as!(
            MasterDataRow,
            "select * from master_data where 
                Start_NYSE notnull or
                Start_NYSE_Arca notnull or
                Start_NYSE_American notnull or
                Start_NASDAQ notnull or
                Start_NASDAQ_Global_Select_Market notnull or
                Start_NASDAQ_Select_Market notnull or
                Start_NASDAQ_Capital_Market notnull or
                Start_CBOE notnull"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(md_result.len() == 0);

        // Stage nyse instruments dates per stock exchange
        mark_stock_exchange_per_stock_as_current_date(&pool)
            .await
            .unwrap();

        // Date is now in correct field
        let current_date = Utc::now().date_naive();
        let md_result: Vec<MasterDataRow> =
            sqlx::query_as!(MasterDataRow, "select * from master_data")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(md_result.iter().any(|row| row
            .issuer_name
            .eq("MAIDEN HOLDINGS LTD 6.625% NTS 14/06/46 USD25")
            && row.start_nyse.unwrap().eq(&current_date)));
        assert!(md_result.iter().any(|row| row
            .issuer_name
            .eq("ETRACS 2X LEVERAGED MSCI US MOMENTUM FACTOR TR ETN")
            && row.start_nyse_arca.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("GENIUS GROUP LTD")
                && row.start_nyse_american.unwrap().eq(&current_date)));
        assert!(md_result.iter().any(|row| row.issuer_name.eq("YANDEX N.V.")
            && row
                .start_nasdaq_global_select_market
                .unwrap()
                .eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("NEXT E GO N V")
                && row.start_nasdaq_select_market.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("KAROOOOO LTD")
                && row.start_nasdaq_capital_market.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("ISHARES INC")
                && row.start_nasdaq.unwrap().eq(&current_date)));
        assert!(md_result.iter().any(|row| row
            .issuer_name
            .eq("GOLDMAN SACHS PHYSICAL GOLD ETF SHARES")
            && row.start_cboe.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("S&P 100 INDEX")
                && row.start_cboe.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("S&P 101 INDEX")
                && row.start_cboe.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("S&P 102 INDEX")
                && row.start_cboe.unwrap().eq(&current_date)));
        assert!(md_result
            .iter()
            .any(|row| row.issuer_name.eq("S&P 103 INDEX")
                && row.start_cboe.unwrap().eq(&current_date)));
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_staged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_without_dates_and_staged_instruments_when_dates_staged_then_no_change_in_master_data(
        pool: Pool<Postgres>,
    ) {
        //Given master data entry with NULL in start_nyse (instrument is registered at NYSE)
        let md_result = sqlx::query_as!(
            MasterDataRow,
            "select * from master_data where issuer_name = 'TEST Company'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(md_result.start_nyse, None);

        // Stage nyse instruments per stock exchange
        mark_stock_exchange_per_stock_as_current_date(&pool)
            .await
            .unwrap();

        //Master data date is still NULL for start_nyse, because source data is marked as staged
        let md_result = sqlx::query_as!(
            MasterDataRow,
            "select * from master_data where issuer_name = 'TEST Company'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(md_result.start_nyse, None);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_wihtout_company_info_when_non_company_instruments_staged_then_master_data_marked_as_non_company(
        pool: Pool<Postgres>,
    ) {
        // Given companies without company info
        let md_result = query!("select count(*) from master_data")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(md_result.count.unwrap() > 10);

        let md_result: Vec<MasterDataRow> = sqlx::query_as!(
            MasterDataRow,
            "select * from master_data where 
            is_company notnull"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(md_result.len() == 0);

        //Staging OTC
        mark_non_companies_in_master_data(&pool).await.unwrap();

        //Then
        let md_result = sqlx::query!(
            "select * from master_data where issue_symbol in ('$ESGE.IV', 'MTUL','$OEX','GNS','MHLA','IGZ','EGOX','ABCD')
            and is_company notnull")
            .fetch_all(&pool)
            .await
            .unwrap();

        assert_eq!(md_result.len(), 8);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_staged.sql",
            "master_data_without_company_and_date.sql",
        )
    ))]
    async fn given_master_data_without_company_info_when_staged_non_company_instruments_is_staged_again_then_no_change_in_master_data(
        pool: Pool<Postgres>,
    ) {
        // Given staged company in instruments
        let sc_result =
            sqlx::query!("Select * from nyse_instruments where instrument_name = 'TEST Company'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(sc_result.is_staged, true);

        //Staging OTC
        mark_non_companies_in_master_data(&pool).await.unwrap();

        //Then no change for master data
        let md_result =
            sqlx::query!("Select * from master_data where issuer_name = 'TEST Company'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(md_result.is_company, None);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_without_company_info_when_company_instruments_is_staged_then_company_in_master_data(
        pool: Pool<Postgres>,
    ) {
        // Given
        let md_result = sqlx::query!("select * from master_data where issue_symbol in ('KARO', 'ABC1', 'ABC2') and is_company isnull").fetch_all(&pool).await.unwrap();
        assert_eq!(md_result.len(), 3);

        //Staging OTC
        mark_companies_in_master_data(&pool).await.unwrap();

        //Then is_company not null
        let md_result = sqlx::query!("select * from master_data where issue_symbol in ('KARO', 'ABC1', 'ABC2') and is_company notnull").fetch_all(&pool).await.unwrap();
        assert_eq!(md_result.len(), 3);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_staged.sql",
            "master_data_without_company_and_date.sql",
        )
    ))]
    async fn given_master_data_without_company_info_when_staged_company_instruments_is_staged_again_then_no_change_in_master_data(
        pool: Pool<Postgres>,
    ) {
        // Given staged entry in instruments
        let instrument_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'KARO2'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instrument_result.is_staged, true);
        assert_eq!(instrument_result.instrument_type, "COMMON_STOCK");

        //Staging OTC
        mark_companies_in_master_data(&pool).await.unwrap();

        //Then
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'KARO2'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.is_company, None);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts("nyse_instruments_unstaged.sql", "master_data_with_company.sql")
    ))]
    async fn given_master_data_with_non_company_info_when_mark_non_company_instruments_staged_then_instruments_marked_staged(
        pool: Pool<Postgres>,
    ) {
        // Given
        let md_result = sqlx::query!("Select * from master_data where issue_symbol = 'ATAKR'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.is_company, Some(false));

        //Staging OTC
        mark_already_staged_non_company_instruments_as_staged(&pool)
            .await
            .unwrap();

        //Then
        let instruments_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'ATAKR'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instruments_result.is_staged, true);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_without_company_info_and_unstaged_instrument_when_mark_company_instruments_staged_then_unstaged_instrument_not_marked_staged(
        pool: Pool<Postgres>,
    ) {
        // Given
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'KARO'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.is_company, None);

        //Staging OTC
        mark_already_staged_company_instruments_as_staged(&pool)
            .await
            .unwrap();

        //Then
        let instrument_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'KARO'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instrument_result.is_staged, false);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts("nyse_instruments_unstaged.sql", "master_data_with_company.sql")
    ))]
    async fn given_master_data_with_company_info_when_mark_instruments_staged_then_instruments_staged(
        pool: Pool<Postgres>,
    ) {
        // Given
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'BOH'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.is_company, Some(true));

        //Staging OTC
        mark_already_staged_company_instruments_as_staged(&pool)
            .await
            .unwrap();

        //Then
        let instrument_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'BOH'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instrument_result.is_staged, true);
    }

    #[traced_test]
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql",
        )
    ))]
    async fn given_master_data_without_company_info_and_unstaged_non_company_instrument_when_mark_instruments_staged_then_unstaged_instrument_not_marked_staged(
        pool: Pool<Postgres>,
    ) {
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'EGOX'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.is_company, None);

        //Staging OTC
        mark_already_staged_non_company_instruments_as_staged(&pool)
            .await
            .unwrap();

        //Then
        let instrument_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'EGOX' and instrument_type = 'TRUST'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instrument_result.is_staged, false);
    }
}
