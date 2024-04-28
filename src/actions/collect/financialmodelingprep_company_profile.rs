use anyhow::Error;
use async_trait::async_trait;
use chrono::{Days, Months, NaiveDate, Utc};
use futures_util::TryFutureExt;
use secrecy::{ExposeSecret, Secret};
use serde_json::from_str;

use serde_with::{serde_as, DisplayFromStr, NoneAsEmptyString};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::Arc;
use std::time;
use tokio::time::sleep;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;
use tracing::{debug, error, info};

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
    #[tracing::instrument(name = "Run FinancialmodelingprepCompanyProfileColletor", skip_all)]
    async fn run(&self) -> Result<Option<StatsMap>, crate::dag_schedule::task::TaskError> {
        if let Some(key) = &self.api_key {
            load_and_store_missing_data(self.pool.clone(), self.client.clone(), key)
                .await
                .map_err(TaskError::UnexpectedError)?;
        } else {
            error!("No Api key provided for FinancialmodelingprepCompanyProfileColletor");
            return Err(TaskError::UnexpectedError(Error::msg(
                "FinancialmodelingprepCompanyProfileColletor key not provided",
            )));
        }
        Ok(None)
    }
}

// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub struct PolygonGroupedDaily {
//     adjusted: Option<bool>,
//     query_count: Option<i64>,
//     results: Option<Vec<DailyValue>>,
//     results_count: Option<i64>,
//     status: String,
// }

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

// #[derive(Default, Deserialize, Debug, Serialize, PartialEq)]
// #[serde(rename_all = "camelCase")]
// struct TransposedPolygonOpenClose {
//     pub close: Vec<f64>,
//     pub business_date: Vec<NaiveDate>,
//     pub high: Vec<f64>,
//     pub low: Vec<f64>,
//     pub open: Vec<f64>,
//     pub symbol: Vec<String>,
//     pub stock_volume: Vec<Option<i64>>,
//     pub traded_volume: Vec<f64>,
//     pub volume_weighted_average_price: Vec<Option<f64>>,
// }

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
    let mut result: Option<String> = sqlx::query!(
        "select issue_symbol
        from master_data_eligible mde
        where issue_symbol not in 
          (select distinct(symbol) 
           from financialmodelingprep_company_profile fcp)
        order by issue_symbol limit 1"
    )
    .fetch_one(&connection_pool)
    .await?
    .issue_symbol;

    // let mut current_check_date = get_start_date(result);
    let mut successful_request_counter: u16 = 0;
    while let Some(issue_sybmol) = result.as_ref() {
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
        let company_profile_response =
            crate::utils::action_helpers::parse_response::<CompanyProfile>(&response)?;

        println!("company_profile: {:?}", company_profile_response);
        if let Some(company_profile) = company_profile_response.company_profile_elements {
            successful_request_counter += 1;
            sqlx::query!(r#"INSERT INTO financialmodelingprep_company_profile (symbol, price, beta, vol_avg, mkt_cap, last_div, "range", changes, company_name, currency, cik, isin, cusip, exchange, exchange_short_name, industry, website, description, ceo, sector, country, full_time_employees, phone, address, city, state, zip, dcf_diff, dcf, image, ipo_date, default_image, is_etf, is_actively_trading, is_adr, is_fund)
                     Select * from UNNEST ($1::text[], $2::float[], $3::float[], $4::integer[], $5::float[], $6::float[], $7::text[], $8::float[], $9::text[], $10::text[], $11::text[], $12::text[], $13::text[], $14::text[], $15::text[], $16::text[], $17::text[], $18::text[], $19::text[], $20::text[], $21::text[], $22::integer[], $23::text[], $24::text[], $25::text[], $26::text[], $27::text[], $28::float[], $29::float[], $30::text[], $31::date[], $32::bool[], $33::bool[], $34::bool[], $35::bool[], $36::bool[]) on conflict do nothing"#,
                    &vec![company_profile[0].symbol.to_string()],
                    &vec![company_profile[0].price] as _,
                    &vec![company_profile[0].beta] as _,
                    &vec![company_profile[0].vol_avg] as _,
                    &vec![company_profile[0].mkt_cap] as _,
                    &vec![company_profile[0].last_div] as _,
                    &vec![company_profile[0].range.clone()] as _,
                    &vec![company_profile[0].changes] as _,
                    &vec![company_profile[0].company_name.to_string()] as _,
                    &vec![company_profile[0].currency.clone()] as _,
                    &vec![company_profile[0].cik.clone()] as _,
                    &vec![company_profile[0].isin.clone()] as _,
                    &vec![company_profile[0].cusip.clone()] as _,
                    &vec![company_profile[0].exchange.clone()] as _,
                    &vec![company_profile[0].exchange_short_name.clone()] as _,
                    &vec![company_profile[0].industry.clone()] as _,
                    &vec![company_profile[0].website.clone()] as _,
                    &vec![company_profile[0].description.clone()] as _,
                    &vec![company_profile[0].ceo.clone()] as _,
                    &vec![company_profile[0].sector.clone()] as _,
                    &vec![company_profile[0].country.clone()] as _,
                    &vec![company_profile[0].full_time_employees.clone()] as _,
                    &vec![company_profile[0].phone.clone()] as _,
                    &vec![company_profile[0].address.clone()] as _,
                    &vec![company_profile[0].city.clone()] as _,
                    &vec![company_profile[0].state.clone()] as _,
                    &vec![company_profile[0].zip.clone()] as _,
                    &vec![company_profile[0].dcf_diff] as _,
                    &vec![company_profile[0].dcf] as _,
                    &vec![company_profile[0].image.clone()] as _,
                    &vec![company_profile[0].ipo_date] as _,
                    &vec![company_profile[0].default_image] as _,
                    &vec![company_profile[0].is_etf] as _,
                    &vec![company_profile[0].is_actively_trading] as _,
                    &vec![company_profile[0].is_adr] as _,
                    &vec![company_profile[0].is_fund] as _
                )
                .execute(&connection_pool).await?;
            //     }
            //     if open_close.status != *"ERROR" {
            //         current_check_date = current_check_date
            //             .checked_add_days(Days::new(1))
            //             .expect("Adding one day must always work, given the operating date context.");
            //         sleep(time::Duration::from_secs(13)).await;
            //     } else {
            //         info!(
            //             "Failed with request {} and got response {}",
            //             request, response
            //         );
            //         sleep(time::Duration::from_secs(13)).await;
        } else {
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
        result = sqlx::query!(
            "select issue_symbol
            from master_data_eligible mde
            where issue_symbol not in 
              (select distinct(symbol) 
               from financialmodelingprep_company_profile fcp)
            order by issue_symbol limit 1"
        )
        .fetch_one(&connection_pool)
        .await?
        .issue_symbol;
    }
    Ok(())
}

// #[tracing::instrument(level = "debug", skip_all)]
// fn get_start_date(result: Option<NaiveDate>) -> NaiveDate {
//     if let Some(date) = result {
//         return date
//             .checked_add_days(Days::new(1))
//             .expect("Adding one day must always work, given the operating date context.");
//     }
//     earliest_date()
// }

// #[tracing::instrument(level = "debug", skip_all)]
// fn transpose_polygon_grouped_daily(
//     instruments: Vec<DailyValue>,
//     business_date: NaiveDate,
// ) -> TransposedPolygonOpenClose {
//     let mut result = TransposedPolygonOpenClose {
//         close: vec![],
//         business_date: vec![],
//         high: vec![],
//         low: vec![],
//         open: vec![],
//         symbol: vec![],
//         traded_volume: vec![],
//         volume_weighted_average_price: vec![],
//         stock_volume: vec![],
//     };

//     for data in instruments {
//         result.close.push(data.close);
//         result.business_date.push(business_date);
//         result.high.push(data.high);
//         result.low.push(data.low);
//         result.open.push(data.open);
//         result.symbol.push(data.symbol);
//         result.traded_volume.push(data.traded_volume);
//         result
//             .volume_weighted_average_price
//             .push(data.volume_weighted_average_price);
//         result.stock_volume.push(data.stock_volume)
//     }
//     result
// }

// #[tracing::instrument(level = "debug", skip_all)]
// fn earliest_date() -> NaiveDate {
//     Utc::now()
//         .date_naive()
//         .checked_sub_months(Months::new(24))
//         .expect("Minus 2 years should never fail")
//         .checked_add_days(Days::new(1))
//         .expect("Adding 1 day should always work")
// }

// // impl Collector for PolygonGroupedDailyCollector {
// //     fn get_sp_fields(&self) -> Vec<sp500_fields::Fields> {
// //         vec![
// //             sp500_fields::Fields::OpenClose,
// //             sp500_fields::Fields::MonthTradingVolume,
// //         ]
// //     }

// //     fn get_source(&self) -> collector_sources::CollectorSource {
// //         collector_sources::CollectorSource::PolygonGroupedDaily
// //     }
// // }

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

    use crate::actions::collect::financialmodelingprep_company_profile::CompanyProfileElement;

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
        let parsed =
            crate::utils::action_helpers::parse_response::<Vec<CompanyProfileElement>>(input_json)
                .unwrap();
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
        assert_eq!(parsed[0], instrument);
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
}
