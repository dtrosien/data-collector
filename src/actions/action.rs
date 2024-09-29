use super::collect::financialmodelingprep_company_profile::FinancialmodelingprepCompanyProfileCollector;
use super::collect::financialmodelingprep_market_capitalization::FinancialmodelingprepMarketCapitalizationCollector;
use super::collect::polygon_grouped_daily::PolygonGroupedDailyCollector;
use super::collect::polygon_open_close::PolygonOpenCloseCollector;
use super::stage::financialmodelingprep_company_profile::FinancialmodelingprepCompanyProfileStager;
use super::stage::polygon_grouped_daily::PolygonGroupedDailyStager;
use crate::{actions::collect::dummy::DummyCollector, api_keys::api_key::PolygonKey};

use crate::actions::collect::nyse_events::NyseEventCollector;
use crate::actions::collect::nyse_instruments::NyseInstrumentCollector;
use crate::actions::collect::sec_companies::SecCompanyCollector;
use crate::actions::stage::nyse_instruments::NyseInstrumentStager;
use crate::actions::stage::sec_companies::SecCompanyStager;
use crate::api_keys::api_key::FinancialmodelingprepKey;
use crate::api_keys::key_manager;
use crate::api_keys::key_manager::KeyManager;
use crate::configuration::SecretKeys;
use crate::dag_schedule::task::Runnable;
use reqwest::Client;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Action is a boxed trait object of Runnable.
pub type Action = Arc<dyn Runnable + Send + Sync>;

/// create_action creates a boxed trait object of Action from a ActionType.
pub fn create_action(
    action_type: &ActionType,
    pool: &PgPool,
    client: &Client,
    secrets: &SecretKeys,
) -> Action {
    let key_store = Arc::new(Mutex::new(key_manager::KeyManager::new()));
    fill_key_store(&key_store, secrets.clone());

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
            create_action_polygon_grouped_daily(pool, client, Arc::clone(&key_store))
        }
        ActionType::PolygonGroupedDailyStager => create_action_polygon_grouped_daily_stager(pool),
        ActionType::PolygonOpenClose => {
            create_action_polygon_open_close(pool, client, Arc::clone(&key_store))
        }
        ActionType::FinancialmodelingprepCompanyProfileCollet => {
            create_action_financial_modeling_company_profile(pool, client, Arc::clone(&key_store))
        }
        ActionType::FinmodCompanyProfileStage => {
            Arc::new(FinancialmodelingprepCompanyProfileStager::new(pool.clone()))
        }
        ActionType::FinmodMarketCapCollect => {
            create_action_financial_modeling_market_capitalization(
                pool,
                client,
                Arc::clone(&key_store),
            )
        }
    }
}

fn fill_key_store(key_store: &Arc<Mutex<KeyManager>>, secrets: SecretKeys) {
    let mut k = key_store.lock().unwrap();
    if let Some(finmod_list) = secrets.financialmodelingprep_company {
        finmod_list
            .split(' ')
            .collect::<Vec<&str>>()
            .into_iter()
            .for_each(|x| {
                let key = FinancialmodelingprepKey::new(x.to_string());
                debug!("FinancialmodelingprepKey key added");
                k.add_key_by_platform(Box::new(key));
            });
    }
    if let Some(poly_list) = secrets.polygon {
        let v = poly_list.split(' ').collect::<Vec<&str>>();

        v.into_iter().for_each(|x| {
            let key = PolygonKey::new(x.to_string());
            debug!("Polygon key added");
            k.add_key_by_platform(Box::new(key));
        });
    }
}

fn create_action_financial_modeling_market_capitalization(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Arc<dyn Runnable + Send + Sync> {
    Arc::new(FinancialmodelingprepMarketCapitalizationCollector::new(
        pool.clone(),
        client.clone(),
        key_manager,
    ))
}

fn create_action_financial_modeling_company_profile(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Arc<FinancialmodelingprepCompanyProfileCollector> {
    Arc::new(FinancialmodelingprepCompanyProfileCollector::new(
        pool.clone(),
        client.clone(),
        key_manager,
    ))
}

fn create_action_polygon_grouped_daily(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Arc<PolygonGroupedDailyCollector> {
    Arc::new(PolygonGroupedDailyCollector::new(
        pool.clone(),
        client.clone(),
        key_manager,
    ))
}

fn create_action_polygon_grouped_daily_stager(
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Arc<PolygonGroupedDailyStager> {
    Arc::new(PolygonGroupedDailyStager::new(pool.clone()))
}

fn create_action_polygon_open_close(
    pool: &sqlx::Pool<sqlx::Postgres>,
    client: &Client,
    key_manager: Arc<Mutex<KeyManager>>,
) -> Arc<PolygonOpenCloseCollector> {
    Arc::new(PolygonOpenCloseCollector::new(
        pool.clone(),
        client.clone(),
        key_manager,
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
    PolygonGroupedDailyStager,
    PolygonOpenClose,
    FinancialmodelingprepCompanyProfileCollet,
    FinmodCompanyProfileStage,
    FinmodMarketCapCollect,
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
