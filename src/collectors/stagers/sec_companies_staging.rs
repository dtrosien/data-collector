use async_trait::async_trait;
use sqlx::PgPool;

use std::fmt::Display;

use crate::tasks::runnable::Runnable;
use crate::{
    collectors::{
        collector_sources, source_apis::sec_companies::SecCompanyCollector, sp500_fields,
        Collector, Stager,
    },
    utils::errors::Result,
};

#[derive(Clone)]
pub struct SecCompanyStager {
    pool: PgPool,
}

impl SecCompanyStager {
    pub fn new(pool: PgPool) -> Self {
        SecCompanyStager { pool }
    }
}

impl Display for SecCompanyStager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecCompanyCollector struct.")
    }
}

impl Stager for SecCompanyStager {
    /// Take fields from the matching collector
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
        SecCompanyCollector::new(self.pool.clone()).get_sp_fields()
    }

    /// Take fields from the matching collector
    fn get_source(&self) -> collector_sources::CollectorSource {
        SecCompanyCollector::new(self.pool.clone()).get_source()
    }

    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Stager of source: {}", Stager::get_source(self))
    }
}

#[async_trait]
impl Runnable for SecCompanyStager {
    async fn run(&self) -> Result<()> {
        stage_data(self.pool.clone()).await
    }
}

pub async fn stage_data(connection_pool: PgPool) -> Result<()> {
    println!("Staging entered");
    //Derive data
    move_issuers_to_master_data(&connection_pool).await?;
    move_otc_issues_to_master_data(&connection_pool).await?;
    derive_country_from_sec_code(&connection_pool).await?;
    //Mark as staged in sec_companies
    // mark_otc_issuers_as_staged(&connection_pool).await?;
    Ok(())
}

/// Move all unstaged issuers from the sec_companies table to the master data table.
async fn move_issuers_to_master_data(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!("insert into master_data (issuer_name, issue_symbol) select name , ticker from sec_companies where is_staged = false on conflict do nothing").execute(connection_pool).await?;
    Ok(())
}

/// Filter for all issuers with category 'OTC' (over the counter), match them with the master data and mark corresponding master data as non-company with category 'OTC'.
async fn move_otc_issues_to_master_data(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        r##" update master_data 
    set
      is_company =  false,
      category =    'OTC' 
    from 
     (select sc.exchange as exchange, sc.ticker as ticker, sc.name as sec_name from sec_companies sc
        join master_data md on
             md.issuer_name = "name"
         and md.issue_symbol = ticker
         where sc.exchange = 'OTC'
           and sc.is_staged = false)
    where 
      sec_name = issuer_name  and issue_symbol = ticker"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

///Take the SIC code from the sec_companies table (state_of_incorporation column), match it with the countries table and write the ISO 3 country codes to the master data table.
async fn derive_country_from_sec_code(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(r##"
    update master_data set 
        location = country_code
    from 
      (select c.country_code , "name" as c_name, ticker , state_of_incorporation from sec_companies sc 
         join (select country_code , sec_code  from countries c) c
           on c.sec_code = sc.state_of_incorporation  
       where is_staged = false) 
    where issuer_name = c_name and issue_symbol = ticker"##)
    .execute(connection_pool)
    .await?;
    Ok(())
}

///Select the master data with category 'OTC' and non-null company and mark corresponding rows in sec_companies as staged (true).
async fn mark_otc_issuers_as_staged(connection_pool: &PgPool) -> Result<()> {
    sqlx::query!(
        r##" 
    update sec_companies 
      set is_staged = true 
    from 
      (select sc."name" as c_name , sc.ticker as c_ticker from sec_companies sc 
       inner join master_data md on
             sc."name" = md.issuer_name 
         and sc.ticker = md.issue_symbol 
       where md.category = 'OTC'
         and md.is_company notnull)
    where "name" = c_name and ticker = c_ticker"##
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}
