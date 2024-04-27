use crate::actions::collect::dummy::DummyCollector;
use crate::actions::collect::financialmodelingprep_company_profile::FinancialmodelingprepCompanyProfileColletor;
use crate::actions::collect::nyse_events::NyseEventCollector;
use crate::actions::collect::nyse_instruments::NyseInstrumentCollector;
use crate::actions::collect::sec_companies::SecCompanyCollector;
use crate::actions::stage::nyse_instruments::NyseInstrumentStager;
use crate::actions::stage::sec_companies::SecCompanyStager;

use crate::configuration::SecretKeys;
use crate::dag_schedule::task::Runnable;

use reqwest::Client;
use secrecy::Secret;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;

use super::collect::polygon_grouped_daily::PolygonGroupedDailyCollector;
use super::collect::polygon_open_close::PolygonOpenCloseCollector;

/// Action is a boxed trait object of Runnable.
pub type Action = Arc<dyn Runnable + Send + Sync>;

/// create_action creates a boxed trait object of Action from a ActionType.
pub fn create_action(
    action_type: &ActionType,
    pool: &PgPool,
    client: &Client,
    secrets: &Option<SecretKeys>,
) -> Action {
    match action_type {
        ActionType::NyseEventsCollect => {
            Arc::new(NyseEventCollector::new(pool.clone(), client.clone()))
        }
        ActionType::NyseInstrumentsCollect => {
            Arc::new(NyseInstrumentCollector::new(pool.clone(), client.clone()))
        }
        ActionType::SecCompaniesCollect => {
            Arc::new(SecCompanyCollector::new(pool.clone(), client.clone()))
        }
        ActionType::NyseInstrumentsStage => Arc::new(NyseInstrumentStager::new(pool.clone())),
        ActionType::SecCompaniesStage => Arc::new(SecCompanyStager::new(pool.clone())),
        ActionType::Dummy => Arc::new(DummyCollector::new()),
        ActionType::PolygonGroupedDaily => {
            create_action_polygon_grouped_daily(pool, client, secrets)
        }
        ActionType::PolygonOpenClose => create_action_polygon_open_close(pool, client, secrets),
        ActionType::FinancialmodelingprepCompanyProfileCollet => {
            create_action_financial_modeling_company_profile(pool, client, secrets)
        }
    }
}

fn create_action_financial_modeling_company_profile(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    secrets: &Option<SecretKeys>,
) -> Arc<FinancialmodelingprepCompanyProfileColletor> {
    let mut fin_modeling_prep_key = Option::<Secret<String>>::None;
    if let Some(secret) = secrets {
        fin_modeling_prep_key = secret.financialmodelingprep_company.clone();
    }

    Arc::new(FinancialmodelingprepCompanyProfileColletor::new(
        pool.clone(),
        client.clone(),
        fin_modeling_prep_key,
    ))
}

fn create_action_polygon_grouped_daily(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    secrets: &Option<SecretKeys>,
) -> Arc<PolygonGroupedDailyCollector> {
    let mut polygon_key = Option::<Secret<String>>::None;
    if let Some(secret) = secrets {
        polygon_key = secret.polygon.clone();
    }

    Arc::new(PolygonGroupedDailyCollector::new(
        pool.clone(),
        client.clone(),
        polygon_key,
    ))
}

fn create_action_polygon_open_close(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    secrets: &Option<SecretKeys>,
) -> Arc<PolygonOpenCloseCollector> {
    let mut polygon_key = Option::<Secret<String>>::None;
    if let Some(secret) = secrets {
        polygon_key = secret.polygon.clone();
    }

    Arc::new(PolygonOpenCloseCollector::new(
        pool.clone(),
        client.clone(),
        polygon_key,
    ))
}

/// Possible Actions
#[derive(Clone, Deserialize)]
pub enum ActionType {
    NyseEventsCollect,
    NyseInstrumentsCollect,
    SecCompaniesCollect,
    NyseInstrumentsStage,
    SecCompaniesStage,
    PolygonGroupedDaily,
    PolygonOpenClose,
    FinancialmodelingprepCompanyProfileCollet,
    Dummy,
}

// // todo kept for later as example if actions can be bundled by type from config
// impl CollectAction {
//     fn matching_collectors(
//         setting: &TaskSetting,
//         pool: &PgPool,
//         client: &Client,
//     ) -> Vec<Box<dyn Collector>> {
//         let collectors = Self::get_all_collectors(pool, client);
//         collectors
//             .into_iter()
//             .filter(|collector| Self::is_collector_requested(setting, collector.as_ref()))
//             .collect()
//     }
//
//     fn get_all_collectors(pool: &PgPool, client: &Client) -> Vec<Box<dyn Collector>> {
//         vec![
//             Box::new(NyseEventCollector::new(pool.clone(), client.clone())),
//             Box::new(NyseInstrumentCollector::new(pool.clone(), client.clone())),
//             Box::new(SecCompanyCollector::new(pool.clone(), client.clone())),
//         ]
//     }
//
//     fn is_collector_requested(setting: &TaskSetting, collector: &dyn Collector) -> bool {
//         let converted_settings_sp = setting.sp500_fields.iter().collect::<BTreeSet<_>>();
//         let sp_fields = collector.get_sp_fields();
//         let converted_collector_sp = sp_fields.iter().collect::<BTreeSet<_>>();
//         if converted_settings_sp.is_disjoint(&converted_collector_sp) {
//             return false;
//         }
//
//         if !(setting.include_sources.contains(&collector.get_source())
//             || setting.include_sources.contains(&CollectorSource::All))
//         {
//             return false;
//         }
//
//         if setting.exclude_sources.contains(&collector.get_source()) {
//             return false;
//         }
//
//         true
//     }
// }
