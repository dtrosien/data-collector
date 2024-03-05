use crate::collectors::source_apis::dummy::DummyCollector;
use crate::collectors::source_apis::nyse_events::NyseEventCollector;
use crate::collectors::source_apis::nyse_instruments::NyseInstrumentCollector;
use crate::collectors::source_apis::sec_companies::SecCompanyCollector;
use crate::collectors::staging::nyse_instruments_staging::NyseInstrumentStager;
use crate::collectors::staging::sec_companies_staging::SecCompanyStager;
use crate::configuration::TaskSetting;
use crate::dag_scheduler::task::{Runnable, StatsMap, TaskError};
use anyhow::anyhow;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

/// BoxedAction is a boxed trait object of Action.
pub type Action = Arc<dyn Runnable + Send + Sync>;

/// create_action creates a boxed trait object of Action from a ActionType.
pub fn create_action(action_type: &ActionType, pool: &PgPool, client: &Client) -> Action {
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
    }
}

/// Possible Actions
#[derive(Clone, Deserialize)]
pub enum ActionType {
    NyseEventsCollect,
    NyseInstrumentsCollect,
    SecCompaniesCollect,
    NyseInstrumentsStage,
    SecCompaniesStage,
    Dummy,
}

// #[derive(Clone)]
// pub struct ActionDependencies {
//     pub pool: PgPool,
//     pub setting: TaskSetting,
//     pub client: Client,
// }
