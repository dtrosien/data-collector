use crate::api_keys::api_key::Status::{self};
use crate::api_keys::api_key::{ApiKey, ApiKeyPlatform};
use crate::api_keys::key_manager::KeyManager;
use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap};
use async_trait::async_trait;
use chrono::NaiveDate;

use futures_util::TryFutureExt;
use reqwest::Client;
use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr, NoneAsEmptyString};
use sqlx::PgPool;
use std::fmt::{Debug, Display};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

const URL: &str = "https://financialmodelingprep.com/api/v3/profile/";
const PLATFORM: &ApiKeyPlatform = &ApiKeyPlatform::Financialmodelingprep;
const WAIT_FOR_KEY: bool = false;

#[derive(Clone, Debug)]
struct IssueSymbols {
    issue_symbol: String,
}

#[derive(Debug)]
struct FinancialmodelingprepCompanyProfileRequest<'a> {
    base: String,
    api_key: &'a mut Box<dyn ApiKey>,
}

impl FinancialmodelingprepCompanyProfileRequest<'_> {
    fn expose_secret(&mut self) -> String {
        self.base.clone() + self.api_key.get_secret().expose_secret()
    }
}

impl Display for FinancialmodelingprepCompanyProfileRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.base)?;
        Secret::new(self.api_key.expose_secret_for_data_structure().clone()).fmt(f)
    }
}

#[derive(Debug)]
pub struct FinancialmodelingprepCompanyProfileCollector {
    pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
}

impl FinancialmodelingprepCompanyProfileCollector {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(pool: PgPool, client: Client, key_manager: Arc<Mutex<KeyManager>>) -> Self {
        FinancialmodelingprepCompanyProfileCollector {
            pool,
            client,
            key_manager,
        }
    }
}

impl Display for FinancialmodelingprepCompanyProfileCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FinancialmodelingprepCompanyProfileColletor struct.")
    }
}

#[async_trait]
impl Runnable for FinancialmodelingprepCompanyProfileCollector {
    #[tracing::instrument(name = "Run FinancialmodelingprepCompanyProfileColletor", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, crate::dag_schedule::task::TaskError> {
        // TODO: Can I check before if a key exists?
        // if self.api_keys.len() > 0 {
        load_and_store_missing_data(
            self.pool.clone(),
            self.client.clone(),
            self.key_manager.clone(),
        )
        .map_err(UnexpectedError)
        .await?;
        // } else {
        //     error!("No Api key provided for FinancialmodelingprepCompanyProfileColletor");
        //     return Err(UnexpectedError(Error::msg(
        //         "FinancialmodelingprepCompanyProfileColletor key not provided",
        //     )));
        // }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Responses {
    NotFound(Vec<EmptyStruct>),
    Data(Vec<CompanyProfileElement>),
    KeyExhausted(ErrorStruct),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmptyStruct;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorStruct {
    #[serde(rename = "Error Message")]
    error_message: String,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanyProfileElement {
    symbol: String,
    price: Option<f64>,
    beta: Option<f64>,
    vol_avg: Option<i64>,
    mkt_cap: Option<i64>,
    last_div: Option<f64>,
    range: Option<String>,
    changes: Option<f64>,
    company_name: Option<String>,
    currency: Option<String>,
    cik: Option<String>,
    isin: Option<String>,
    cusip: Option<String>,
    exchange: Option<String>,
    exchange_short_name: Option<String>,
    industry: Option<String>,
    website: Option<String>,
    description: Option<String>,
    ceo: Option<String>,
    sector: Option<String>,
    country: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    full_time_employees: Option<i32>,
    phone: Option<String>,
    address: Option<String>,
    city: Option<String>,
    state: Option<String>,
    zip: Option<String>,
    dcf_diff: Option<f64>,
    dcf: Option<f64>,
    image: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    ipo_date: Option<NaiveDate>,
    default_image: Option<bool>,
    is_etf: Option<bool>,
    is_actively_trading: Option<bool>,
    is_adr: Option<bool>,
    is_fund: Option<bool>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn load_and_store_missing_data(
    connection_pool: PgPool,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Result<(), anyhow::Error> {
    load_and_store_missing_data_given_url(connection_pool, client, key_manager, URL).await
}

#[tracing::instrument(level = "debug", skip_all)]
async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    key_manager: Arc<Mutex<KeyManager>>,
    url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load Financialmodelingprep Company Profile Colletor.");
    let mut potential_issue_sybmol: Option<String> =
        get_next_issue_symbol(&connection_pool).await?;
    let mut general_api_key =
        KeyManager::get_new_apikey_or_wait(key_manager.clone(), WAIT_FOR_KEY, PLATFORM).await;
    let mut _successful_request_counter: u16 = 0; // Variable actually used, but clippy is buggy? with the shorthand += below. (clippy 0.1.79)
    while let (Some(issue_sybmol), Some(mut api_key)) = (
        potential_issue_sybmol.as_ref(),
        general_api_key.take_if(|_| potential_issue_sybmol.is_some()),
    ) {
        info!("Requesting symbol {}", issue_sybmol);
        let mut request = create_finprep_company_request(url, issue_sybmol, &mut api_key);
        debug!("Financialmodelingprep Company request: {}", request);
        let response = client
            .get(request.expose_secret())
            .send()
            .await?
            .text()
            .await?;
        debug!("Response: {}", response);

        //TODO: Handle error
        let parsed = crate::utils::action_helpers::parse_response::<Responses>(&response)?;

        match parsed {
            Responses::Data(data) => {
                store_data(data, &connection_pool).await?;
                _successful_request_counter += 1;
            }
            Responses::KeyExhausted(_) => {
                api_key.set_status(Status::Exhausted);
            }
            Responses::NotFound(_) => {
                info!("Stock symbol '{}' not found.", issue_sybmol);
                add_missing_issue_symbol(issue_sybmol, &connection_pool).await?;
            }
        }

        potential_issue_sybmol = get_next_issue_symbol(&connection_pool).await?;
        general_api_key = KeyManager::exchange_apikey_or_wait_if_non_ready(
            key_manager.clone(),
            WAIT_FOR_KEY,
            api_key,
            PLATFORM,
        )
        .await;
    }
    if let Some(api_key) = general_api_key {
        let mut d = key_manager.lock().expect("msg");
        d.add_key_by_platform(api_key);
    }
    Ok(())
}

async fn get_next_issue_symbol(connection_pool: &PgPool) -> Result<Option<String>, anyhow::Error> {
    let missing_issue_symbols = sqlx::query_as!(
        IssueSymbols,
        "SELECT issue_symbol FROM source_symbol_warden ssw  where financial_modeling_prep = false"
    )
    .fetch_all(connection_pool)
    .await?;
    let missing_issue_symbols = missing_issue_symbols
        .into_iter()
        .map(|issue_symbol| issue_symbol.issue_symbol)
        .collect::<Vec<_>>();

    let query_result = sqlx::query!(
        "select issue_symbol
        from master_data md
        where issue_symbol not in 
          (select distinct(symbol) 
           from financialmodelingprep_company_profile fcp) and issue_symbol not in (select unnest($1::text[]))
        order by issue_symbol limit 1",
        &missing_issue_symbols
    )
    .fetch_one(connection_pool)
    .await;

    match query_result {
        Ok(_) => Ok(Some(query_result?.issue_symbol)),
        Err(_) => Ok(Option::None),
    }
}

async fn store_data(
    data: Vec<CompanyProfileElement>,
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    sqlx::query!(r#"INSERT INTO financialmodelingprep_company_profile (symbol, price, beta, vol_avg, mkt_cap, last_div, "range", changes, company_name, currency, cik, isin, cusip, exchange, exchange_short_name, industry, website, description, ceo, sector, country, full_time_employees, phone, address, city, state, zip, dcf_diff, dcf, image, ipo_date, default_image, is_etf, is_actively_trading, is_adr, is_fund)
    Select * from UNNEST ($1::text[], $2::float[], $3::float[], $4::integer[], $5::float[], $6::float[], $7::text[], $8::float[], $9::text[], $10::text[], $11::text[], $12::text[], $13::text[], $14::text[], $15::text[], $16::text[], $17::text[], $18::text[], $19::text[], $20::text[], $21::text[], $22::integer[], $23::text[], $24::text[], $25::text[], $26::text[], $27::text[], $28::float[], $29::float[], $30::text[], $31::date[], $32::bool[], $33::bool[], $34::bool[], $35::bool[], $36::bool[]) on conflict do nothing"#,
   &vec![data[0].symbol.to_string()],
   &vec![data[0].price] as _,
   &vec![data[0].beta] as _,
   &vec![data[0].vol_avg] as _,
   &vec![data[0].mkt_cap] as _,
   &vec![data[0].last_div] as _,
   &vec![data[0].range.clone()] as _,
   &vec![data[0].changes] as _,
   &vec![data[0].company_name.clone()] as _,
   &vec![data[0].currency.clone()] as _,
   &vec![data[0].cik.clone()] as _,
   &vec![data[0].isin.clone()] as _,
   &vec![data[0].cusip.clone()] as _,
   &vec![data[0].exchange.clone()] as _,
   &vec![data[0].exchange_short_name.clone()] as _,
   &vec![data[0].industry.clone()] as _,
   &vec![data[0].website.clone()] as _,
   &vec![data[0].description.clone()] as _,
   &vec![data[0].ceo.clone()] as _,
   &vec![data[0].sector.clone()] as _,
   &vec![data[0].country.clone()] as _,
   &vec![data[0].full_time_employees.clone()] as _,
   &vec![data[0].phone.clone()] as _,
   &vec![data[0].address.clone()] as _,
   &vec![data[0].city.clone()] as _,
   &vec![data[0].state.clone()] as _,
   &vec![data[0].zip.clone()] as _,
   &vec![data[0].dcf_diff] as _,
   &vec![data[0].dcf] as _,
   &vec![data[0].image.clone()] as _,
   &vec![data[0].ipo_date] as _,
   &vec![data[0].default_image] as _,
   &vec![data[0].is_etf] as _,
   &vec![data[0].is_actively_trading] as _,
   &vec![data[0].is_adr] as _,
   &vec![data[0].is_fund] as _
    )
    .execute(connection_pool).await?;

    Ok(())
}

async fn add_missing_issue_symbol(
    issue_symbol: &str,
    connection_pool: &PgPool,
) -> Result<(), anyhow::Error> {
    sqlx::query!(
        r#"INSERT INTO source_symbol_warden (issue_symbol, financial_modeling_prep)
      VALUES($1, false)
      ON CONFLICT(issue_symbol)
      DO UPDATE SET
        financial_modeling_prep = false"#,
        issue_symbol
    )
    .execute(connection_pool)
    .await?;
    Ok(())
}

///  Example output https://financialmodelingprep.com/api/v3/profile/AAPL?apikey=TOKEN
// #[tracing::instrument(level = "debug", skip_all)],
fn create_finprep_company_request<'a>(
    base_url: &'a str,
    issue_symbol: &'a str,
    api_key: &'a mut Box<dyn ApiKey>,
) -> FinancialmodelingprepCompanyProfileRequest<'a> {
    let base_request_url = base_url.to_string() + issue_symbol.to_string().as_str() + "?apikey=";
    FinancialmodelingprepCompanyProfileRequest {
        base: base_request_url,
        api_key,
    }
}

#[cfg(test)]
mod test {
    use super::ErrorStruct;
    use crate::actions::collect::financialmodelingprep_company_profile::{
        CompanyProfileElement, EmptyStruct, Responses,
    };
    use chrono::NaiveDate;

    #[test]
    fn parse_data_response() {
        let input_json = r#"[
            {
              "symbol": "A",
              "price": 137.74,
              "beta": 1.122,
              "volAvg": 1591377,
              "mktCap": 40365395700,
              "lastDiv": 0.94,
              "range": "96.8-151.58",
              "changes": 1.37,
              "companyName": "Agilent Technologies, Inc.",
              "currency": "USD",
              "cik": "0001090872",
              "isin": "US00846U1016",
              "cusip": "00846U101",
              "exchange": "New York Stock Exchange",
              "exchangeShortName": "NYSE",
              "industry": "Medical - Diagnostics & Research",
              "website": "https://www.agilent.com",
              "description": "Agilent Technologies",
              "ceo": "Mr. Michael R. McMullen",
              "sector": "Healthcare",
              "country": "US",
              "fullTimeEmployees": "17700",
              "phone": "800 227 9770",
              "address": "5301 Stevens Creek Boulevard",
              "city": "Santa Clara",
              "state": "CA",
              "zip": "95051",
              "dcfDiff": 53.46901,
              "dcf": 84.27099210145948,
              "image": "https://financialmodelingprep.com/image-stock/A.png",
              "ipoDate": "1999-11-18",
              "defaultImage": false,
              "isEtf": false,
              "isActivelyTrading": true,
              "isAdr": false,
              "isFund": false
            }
          ]"#;
        let parsed = crate::utils::action_helpers::parse_response::<Responses>(input_json).unwrap();

        let instrument = CompanyProfileElement {
            symbol: "A".to_string(),
            price: Some(137.74),
            beta: Some(1.122),
            vol_avg: Some(1591377),
            mkt_cap: Some(40365395700),
            last_div: Some(0.94),
            range: Some("96.8-151.58".to_string()),
            changes: Some(1.37),
            company_name: Some("Agilent Technologies, Inc.".to_string()),
            currency: Some("USD".to_string()),
            cik: Some("0001090872".to_string()),
            isin: Some("US00846U1016".to_string()),
            cusip: Some("00846U101".to_string()),
            exchange: Some("New York Stock Exchange".to_string()),
            exchange_short_name: Some("NYSE".to_string()),
            industry: Some("Medical - Diagnostics & Research".to_string()),
            website: Some("https://www.agilent.com".to_string()),
            description: Some("Agilent Technologies".to_string()),
            ceo: Some("Mr. Michael R. McMullen".to_string()),
            sector: Some("Healthcare".to_string()),
            country: Some("US".to_string()),
            full_time_employees: Some(17700),
            phone: Some("800 227 9770".to_string()),
            address: Some("5301 Stevens Creek Boulevard".to_string()),
            city: Some("Santa Clara".to_string()),
            state: Some("CA".to_string()),
            zip: Some("95051".to_string()),
            dcf_diff: Some(53.46901),
            dcf: Some(84.27099210145948),
            image: Some("https://financialmodelingprep.com/image-stock/A.png".to_string()),
            ipo_date: Some(
                NaiveDate::parse_from_str("1999-11-18", "%Y-%m-%d").expect("Parsing constant."),
            ),
            default_image: Some(false),
            is_etf: Some(false),
            is_actively_trading: Some(true),
            is_adr: Some(false),
            is_fund: Some(false),
        };
        match parsed {
            Responses::Data(data) => {
                assert_eq!(data[0], instrument);
            }
            _ => panic!("Data no parsable"),
        }
    }

    #[test]
    fn parse_data_struct_with_empty_ipo() {
        let input_json = r#"[
            {
              "symbol": "A",
              "price": 137.74,
              "beta": 1.122,
              "volAvg": 1591377,
              "mktCap": 40365395700,
              "lastDiv": 0.94,
              "range": "96.8-151.58",
              "changes": 1.37,
              "companyName": "Agilent Technologies, Inc.",
              "currency": "USD",
              "cik": "0001090872",
              "isin": "US00846U1016",
              "cusip": "00846U101",
              "exchange": "New York Stock Exchange",
              "exchangeShortName": "NYSE",
              "industry": "Medical - Diagnostics & Research",
              "website": "https://www.agilent.com",
              "description": "Agilent Technologies",
              "ceo": "Mr. Michael R. McMullen",
              "sector": "Healthcare",
              "country": "US",
              "fullTimeEmployees": "17700",
              "phone": "800 227 9770",
              "address": "5301 Stevens Creek Boulevard",
              "city": "Santa Clara",
              "state": "CA",
              "zip": "95051",
              "dcfDiff": 53.46901,
              "dcf": 84.27099210145948,
              "image": "https://financialmodelingprep.com/image-stock/A.png",
              "ipoDate": "",
              "defaultImage": false,
              "isEtf": false,
              "isActivelyTrading": true,
              "isAdr": false,
              "isFund": false
            }
          ]"#;
        let parsed =
            crate::utils::action_helpers::parse_response::<Vec<CompanyProfileElement>>(input_json)
                .unwrap();
        // ALTER TABLE public.financialmodelingprep_company_profile ALTER COLUMN company_name DROP NOT NULL;

        let instrument = CompanyProfileElement {
            symbol: "A".to_string(),
            price: Some(137.74),
            beta: Some(1.122),
            vol_avg: Some(1591377),
            mkt_cap: Some(40365395700),
            last_div: Some(0.94),
            range: Some("96.8-151.58".to_string()),
            changes: Some(1.37),
            company_name: Some("Agilent Technologies, Inc.".to_string()),
            currency: Some("USD".to_string()),
            cik: Some("0001090872".to_string()),
            isin: Some("US00846U1016".to_string()),
            cusip: Some("00846U101".to_string()),
            exchange: Some("New York Stock Exchange".to_string()),
            exchange_short_name: Some("NYSE".to_string()),
            industry: Some("Medical - Diagnostics & Research".to_string()),
            website: Some("https://www.agilent.com".to_string()),
            description: Some("Agilent Technologies".to_string()),
            ceo: Some("Mr. Michael R. McMullen".to_string()),
            sector: Some("Healthcare".to_string()),
            country: Some("US".to_string()),
            full_time_employees: Some(17700),
            phone: Some("800 227 9770".to_string()),
            address: Some("5301 Stevens Creek Boulevard".to_string()),
            city: Some("Santa Clara".to_string()),
            state: Some("CA".to_string()),
            zip: Some("95051".to_string()),
            dcf_diff: Some(53.46901),
            dcf: Some(84.27099210145948),
            image: Some("https://financialmodelingprep.com/image-stock/A.png".to_string()),
            ipo_date: None,
            default_image: Some(false),
            is_etf: Some(false),
            is_actively_trading: Some(true),
            is_adr: Some(false),
            is_fund: Some(false),
        };
        assert_eq!(parsed[0], instrument);
    }

    #[test]
    fn parse_empty_response() {
        let input_json = r#"[]"#;
        let parsed_response =
            crate::utils::action_helpers::parse_response::<Responses>(input_json).unwrap();

        match parsed_response {
            Responses::NotFound(a) => assert_eq!(a.len(), 0),
            _ => panic!("Wrong parse!"),
        }
    }

    #[test]
    fn parse_empty_struct() {
        let input_json = r#"[]"#;
        let parsed_response =
            crate::utils::action_helpers::parse_response::<Vec<EmptyStruct>>(input_json).unwrap();

        assert_eq!(parsed_response.len(), 0);
    }

    #[test]
    fn parse_error_response() {
        let input_json = r#"{"Error Message": "Limit Reach."}"#;
        let parsed_response =
            crate::utils::action_helpers::parse_response::<Responses>(input_json).unwrap();

        match parsed_response {
            Responses::KeyExhausted(a) => assert_eq!(a.error_message, "Limit Reach."),
            _ => panic!("Wrong parse!"),
        };
    }

    #[test]
    fn parse_error_struct() {
        let input_json = r#"{"Error Message": "Limit Reach."}"#;
        let parsed_response =
            crate::utils::action_helpers::parse_response::<ErrorStruct>(input_json).unwrap();

        assert_eq!(parsed_response.error_message, "Limit Reach.");
    }
}
