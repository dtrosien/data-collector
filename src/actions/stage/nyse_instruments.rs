use async_trait::async_trait;

use sqlx::PgPool;

use std::fmt::Display;

use tracing::info;

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

// #[derive(Debug, Deserialize, Display, EnumIter, PartialEq)]
// #[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
// enum NonCompanies {
//     ExchangeTradedFund,
//     ExchangeTradedNote,
//     Index,
//     Note,
//     PreferredStock,
//     Right,
//     Test,
//     Trust,
//     Unit,
// }

// impl NonCompanies {
//     // Return list of all enums as string
//     pub fn get_all() -> Vec<String> {
//         NonCompanies::iter()
//             .map(|non_comp| non_comp.to_string())
//             .collect()
//     }
// }

// #[derive(Debug, Deserialize, Display, EnumIter, PartialEq)]
// #[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
// enum Companies {
//     CommonStock,
//     Equity,
//     Reit,
// }

// impl Companies {
//     // Return list of all enums as string
//     pub fn get_all() -> Vec<String> {
//         Companies::iter()
//             .map(|non_comp| non_comp.to_string())
//             .collect()
//     }
// }

#[derive(Clone, Debug)]
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

#[async_trait]
impl Runnable for NyseInstrumentStager {
    #[tracing::instrument(name = "Run NyseInstrumentStager", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        stage_data(self.pool.clone())
            .await
            .map_err(UnexpectedError)?;
        Ok(None)
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn stage_data(connection_pool: PgPool) -> Result<(), anyhow::Error> {
    info!("Start staging of Nyse Instrument");
    //Mark test data as staged
    mark_test_data_as_staged(&connection_pool).await?;

    //Stage data
    mark_stock_exchange_per_stock_as_current_date(&connection_pool).await?;
    copy_instruments_to_master_data(&connection_pool).await?;

    //Mark as staged in nyse_instruments
    mark_already_staged_instruments_as_staged(&connection_pool).await?;

    info!("Finished staging of Nyse Instrument");
    Ok(())
}

/// Mark all nyse instrument entries with instrument type 'TEST' as staged.
#[tracing::instrument(level = "debug", skip_all)]
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
#[tracing::instrument(level = "debug", skip_all)]
async fn mark_stock_exchange_per_stock_as_current_date(
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"update master_data set 
                start_nyse                         = case WHEN r.mic_code = 'XNYS' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_nyse_arca                    = case WHEN r.mic_code = 'ARCX' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_nyse_american                = case WHEN r.mic_code = 'XASE' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_nasdaq_global_select_market  = case WHEN r.mic_code = 'XNGS' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_nasdaq_select_market         = case WHEN r.mic_code = 'XNMS' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_nasdaq_capital_market        = case WHEN r.mic_code = 'XNCM' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_nasdaq                       = case WHEN r.mic_code = 'XNAS' then TO_DATE('1792-05-17','YYYY-MM-DD') end,
                start_cboe                         = case WHEN r.mic_code in ('BATS', 'XCBO', 'BATY', 'EDGA', 'EDGX') then TO_DATE('1792-05-17','YYYY-MM-DD') end  
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

#[tracing::instrument(level = "debug", skip_all)]
async fn copy_instruments_to_master_data(connection_pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"
        update master_data set instrument = instrument_type
        from (select replace(symbol_esignal_ticker,'/','-') as ni_ticker, instrument_type
                from
                   nyse_instruments ni
                where
                    is_staged = false
                ) as r
        where
            master_data.issue_symbol  = ni_ticker;"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[tracing::instrument(level = "debug", skip_all)]
async fn mark_already_staged_instruments_as_staged(
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r##"
    update nyse_instruments  set is_staged = true
    from   (select issue_symbol
            from master_data
            where instrument notnull
            ) as r
    where
        replace(symbol_esignal_ticker,'/','-') = r.issue_symbol;"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::NaiveDate;
    use sqlx::{query, Pool, Postgres};

    use crate::actions::stage::nyse_instruments::{
        copy_instruments_to_master_data, mark_already_staged_instruments_as_staged,
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
        instrument: Option<String>,
        category: Option<String>,
        renamed_to_issuer_name: Option<String>,
        renamed_to_issue_symbol: Option<String>,
        renamed_at_date: Option<NaiveDate>,
        current_name: Option<String>,
        suspended: Option<bool>,
        suspension_date: Option<NaiveDate>,
    }

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
        let current_date = NaiveDate::parse_from_str("1792-05-17", "%Y-%m-%d").unwrap();
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

    // Stage instrument and it appears - done
    // Stage instrument and non match stays empty - done
    // Mark staged instrument as staged - done
    // Leave unstaged instrument as unstaged
    // Already staged instrument gets ignored

    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_wihtout_company_info_when_instrument_staged_then_master_data_contains_instrument(
        pool: Pool<Postgres>,
    ) {
        // Given company without instrument in master data, but in nyse instruments
        let ni_result =
            query!("select * from nyse_instruments where symbol_exchange_ticker = 'MHLA'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ni_result.instrument_type, "PREFERRED_STOCK");

        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'MHLA'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.instrument, None);

        //Staging instruments
        copy_instruments_to_master_data(&pool).await.unwrap();

        //Then instrument in master data
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'MHLA'")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(md_result.instrument.unwrap(), "PREFERRED_STOCK");
    }

    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql",
        )
    ))]
    async fn given_master_data_without_company_info_when_non_matching_instrument_is_staged_then_no_change_in_master_data(
        pool: Pool<Postgres>,
    ) {
        // Given staged company in instruments
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'ABC'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.instrument, None);

        let ni_result =
            sqlx::query!("select * from nyse_instruments where instrument_type notnull")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(ni_result.len() > 6);
        //Staging OTC
        copy_instruments_to_master_data(&pool).await.unwrap();

        //Then no change for master data
        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'ABC'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.instrument, None);
    }

    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts("nyse_instruments_unstaged.sql", "master_data_with_company.sql")
    ))]
    async fn given_master_data_with_instrument_info_when_instrument_gets_staged_then_instrument_is_marked_staged(
        pool: Pool<Postgres>,
    ) {
        // Given staged instrument (not tested), yet marked unstaged
        let ni_result =
            query!("select * from nyse_instruments where symbol_exchange_ticker = 'BOH'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ni_result.is_staged, false);

        //Marking staged instruments
        mark_already_staged_instruments_as_staged(&pool)
            .await
            .unwrap();

        //Then is_staged is marked true
        let ni_result =
            query!("select * from nyse_instruments where symbol_exchange_ticker = 'BOH'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ni_result.is_staged, true);
    }

    // Leave unstaged instrument as unstaged
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_unstaged.sql",
            "master_data_without_company_and_date.sql",
        )
    ))]
    async fn given_master_data_without_instrument_info_when_unstaged_instrument_gets_marked_staged_then_no_change_in_instruments(
        pool: Pool<Postgres>,
    ) {
        // Given staged entry in instruments
        let ni_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'MHLA'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ni_result.is_staged, false);
        assert_eq!(ni_result.instrument_type, "PREFERRED_STOCK");

        let md_result = sqlx::query!("select * from master_data where issue_symbol = 'MHLA'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.instrument, None);

        //Staging OTC
        mark_already_staged_instruments_as_staged(&pool)
            .await
            .unwrap();

        //Then
        let ni_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'MHLA'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ni_result.is_staged, false);
    }

    /// Already staged instrument gets ignored
    #[sqlx::test(fixtures(
        path = "../../../tests/resources/collectors/staging/nyse_instruments_staging",
        scripts(
            "nyse_instruments_staged.sql",
            "master_data_without_company_and_date.sql"
        )
    ))]
    async fn given_master_data_with_no_instrument_info_when_copy_already_staged_instrument_then_master_data_unchanged(
        pool: Pool<Postgres>,
    ) {
        // Given
        let md_result = sqlx::query!("Select * from master_data where issue_symbol = 'KARO2'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.instrument, None);

        let ni_result =
            sqlx::query!("select * from nyse_instruments where symbol_esignal_ticker = 'KARO2'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(ni_result.is_staged, true);

        //Staging instruments
        copy_instruments_to_master_data(&pool).await.unwrap();

        //Then
        let md_result = sqlx::query!("Select * from master_data where issue_symbol = 'KARO2'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(md_result.instrument, None);
    }
}
