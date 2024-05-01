use anyhow::{anyhow, Error};
use async_trait::async_trait;
use chrono::NaiveDate;
use futures_util::TryFutureExt;
use secrecy::{ExposeSecret, Secret};

use serde_with::{serde_as, DisplayFromStr, NoneAsEmptyString};

use std::fmt::{Debug, Display};

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::dag_schedule::task::TaskError::UnexpectedError;
use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

const URL: &str = "https://financialmodelingprep.com/api/v3/profile/";

#[derive(Clone, Debug)]
struct FinancialmodelingprepCompanyProfileRequest {
    base: String,
    api_key: Secret<String>,
}

impl FinancialmodelingprepCompanyProfileRequest {
    fn expose_secret(&self) -> String {
        self.base.clone() + self.api_key.expose_secret()
    }
}

impl Display for FinancialmodelingprepCompanyProfileRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.base)?;
        Secret::fmt(&self.api_key, f)
    }
}

#[derive(Clone, Debug)]
pub struct FinancialmodelingprepCompanyProfileColletor {
    pool: PgPool,
    client: Client,
    api_key: Option<Secret<String>>,
}

impl FinancialmodelingprepCompanyProfileColletor {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(pool: PgPool, client: Client, api_key: Option<Secret<String>>) -> Self {
        FinancialmodelingprepCompanyProfileColletor {
            pool,
            client,
            api_key,
        }
    }
}

impl Display for FinancialmodelingprepCompanyProfileColletor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FinancialmodelingprepCompanyProfileColletor struct.")
    }
}

#[async_trait]
impl Runnable for FinancialmodelingprepCompanyProfileColletor {
    #[tracing::instrument(name = "Run FinancialmodelingprepCompanyProfileColletor", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, crate::dag_schedule::task::TaskError> {
        if let Some(key) = &self.api_key {
            load_and_store_missing_data(self.pool.clone(), self.client.clone(), key)
                .map_err(UnexpectedError)
                .await?;
        } else {
            error!("No Api key provided for FinancialmodelingprepCompanyProfileColletor");
            return Err(UnexpectedError(Error::msg(
                "FinancialmodelingprepCompanyProfileColletor key not provided",
            )));
        }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Responses {
    Data(Vec<CompanyProfileElement>),
    KeyExhausted(ErrorStruct),
    NotFound(Vec<Option<serde_json::Value>>),
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorStruct {
    #[serde(rename = "Error Message")]
    error_message: String,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataStruct {
    data_msg: Option<i32>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanyProfile {
    #[serde(rename = "Error Message")]
    error_message: Option<String>,
    company_profile_elements: Option<Vec<CompanyProfileElement>>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompanyError {
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
    company_name: String,
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
    api_key: &Secret<String>,
) -> Result<(), anyhow::Error> {
    load_and_store_missing_data_given_url(connection_pool, client, api_key, URL).await
}

#[tracing::instrument(level = "debug", skip_all)]
async fn load_and_store_missing_data_given_url(
    connection_pool: sqlx::Pool<sqlx::Postgres>,
    client: Client,
    api_key: &Secret<String>,
    url: &str,
) -> Result<(), anyhow::Error> {
    info!("Starting to load Financialmodelingprep Company Profile Colletor.");
    let mut potential_issue_sybmol: Option<String> =
        get_next_issue_symbol(&connection_pool).await?;

    // let mut current_check_date = get_start_date(result);
    let mut successful_request_counter: u16 = 0;
    while let Some(issue_sybmol) = potential_issue_sybmol.as_ref() {
        println!("#########################{}####", issue_sybmol);
        let request = create_polygon_grouped_daily_request(url, issue_sybmol, api_key);
        println!("##########{}", request.expose_secret());
        info!("Financialmodelingprep Company request: {}", request);
        let response = client
            .get(&request.expose_secret())
            .send()
            .await?
            .text()
            .await?;
        println!("Repsonse: {}", response);

        //TODO: Handle error
        let parsed = crate::utils::action_helpers::parse_response::<Responses>(&response)?;

        match parsed {
            Responses::Data(data) => {
                store_data(data, &connection_pool).await?;
                successful_request_counter += 1;
            }
            Responses::KeyExhausted(_) => {
                return handle_exhausted_key(successful_request_counter);
            }
            Responses::NotFound(_) => {
                info!("Key {} not found.", issue_sybmol);
                return Ok(());
            }
        }

        potential_issue_sybmol = get_next_issue_symbol(&connection_pool).await?;
    }
    Ok(())
}

async fn get_next_issue_symbol(connection_pool: &PgPool) -> Result<Option<String>, anyhow::Error> {
    Ok(sqlx::query!(
        "select issue_symbol
        from master_data_eligible mde
        where issue_symbol not in 
          (select distinct(symbol) 
           from financialmodelingprep_company_profile fcp) and issue_symbol not in ('ATCH', 'BFX')
        order by issue_symbol limit 1"
    )
    .fetch_one(connection_pool)
    .await?
    .issue_symbol)
}

fn handle_exhausted_key(successful_request_counter: u16) -> Result<(), anyhow::Error> {
    if successful_request_counter == 0 {
        error!("FinancialmodelingprepCompanyProfileColletor key is already exhausted");
        return Err(Error::msg(
            "FinancialmodelingprepCompanyProfileColletor key is already exhausted",
        ));
    } else {
        info!(
            "FinancialmodelingprepCompanyProfileColletor collected {} entries.",
            successful_request_counter
        );
        return Ok(());
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
   &vec![data[0].company_name.to_string()] as _,
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

///  Example output https://financialmodelingprep.com/api/v3/profile/AAPL?apikey=TOKEN
#[tracing::instrument(level = "debug", skip_all)]
fn create_polygon_grouped_daily_request(
    base_url: &str,
    issue_symbol: &str,
    api_key: &Secret<String>,
) -> FinancialmodelingprepCompanyProfileRequest {
    let base_request_url = base_url.to_string() + issue_symbol.to_string().as_str() + "?apikey=";
    FinancialmodelingprepCompanyProfileRequest {
        base: base_request_url,
        api_key: api_key.clone(),
    }
}

#[cfg(test)]
mod test {
    use chrono::NaiveDate;

    use crate::actions::collect::financialmodelingprep_company_profile::{
        CompanyProfileElement, Responses,
    };

    #[test]
    fn parse_financialmodelingprep_company_profile() {
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
        println!("parsed:{:?}", parsed);

        let instrument = CompanyProfileElement {
            symbol: "A".to_string(),
            price: Some(137.74),
            beta: Some(1.122),
            vol_avg: Some(1591377),
            mkt_cap: Some(40365395700),
            last_div: Some(0.94),
            range: Some("96.8-151.58".to_string()),
            changes: Some(1.37),
            company_name: "Agilent Technologies, Inc.".to_string(),
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
    fn parse_financialmodelingprep_company_profile_with_empty_ipo() {
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

        let instrument = CompanyProfileElement {
            symbol: "A".to_string(),
            price: Some(137.74),
            beta: Some(1.122),
            vol_avg: Some(1591377),
            mkt_cap: Some(40365395700),
            last_div: Some(0.94),
            range: Some("96.8-151.58".to_string()),
            changes: Some(1.37),
            company_name: "Agilent Technologies, Inc.".to_string(),
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
    fn parse_financialmodelingprep_company_profile_with_empty_ipo2() {
        let input_json = r#"{
            "error_msg": "My Error Text"
            }
          "#;
        let parsed = crate::utils::action_helpers::parse_response::<
            Option<Vec<CompanyProfileElement>>,
        >(input_json)
        .unwrap();
        println!("Parsed: {:?}", parsed);
        let instrument = CompanyProfileElement {
            symbol: "A".to_string(),
            price: Some(137.74),
            beta: Some(1.122),
            vol_avg: Some(1591377),
            mkt_cap: Some(40365395700),
            last_div: Some(0.94),
            range: Some("96.8-151.58".to_string()),
            changes: Some(1.37),
            company_name: "Agilent Technologies, Inc.".to_string(),
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
        // assert_eq!(parsed.company_profile_elements.unwrap()[0], instrument);
    }
}

// {
//     "Error Message": "Limit Reach . Please upgrade your plan or visit our documentation for more details at https://site.financialmodelingprep.com/"
//   }
