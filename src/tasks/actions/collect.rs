use crate::collectors::collector_sources::CollectorSource;
use crate::collectors::source_apis::nyse_events::NyseEventCollector;
use crate::collectors::source_apis::nyse_instruments::NyseInstrumentCollector;
use crate::collectors::source_apis::sec_companies::SecCompanyCollector;
use crate::configuration::TaskSetting;

use crate::utils::futures::join_handle_results;
use async_trait::async_trait;

use crate::collectors::collector::Collector;
use crate::dag_scheduler::task::{StatsMap, TaskError};
use crate::tasks::actions::action::Action;
use crate::tasks::task::ActionDependencies;
use reqwest::Client;
use sqlx::PgPool;
use std::collections::BTreeSet;
use tokio::task::JoinHandle;

/// Collect from sources via Collectors
pub struct CollectAction {}

#[async_trait]
impl Action for CollectAction {
    async fn execute(&self, dependencies: ActionDependencies) -> Result<Option<StatsMap>, TaskError> {
        let collectors = CollectAction::matching_collectors(
            &dependencies.setting,
            &dependencies.pool,
            &dependencies.client,
        );

        let handles: Vec<JoinHandle<Result<Option<StatsMap>, TaskError>>> =
            collectors.into_iter().map(execute_collector).collect();

        join_handle_results(handles).await
    }
}

impl CollectAction {
    fn matching_collectors(
        setting: &TaskSetting,
        pool: &PgPool,
        client: &Client,
    ) -> Vec<Box<dyn Collector>> {
        let collectors = Self::get_all_collectors(pool, client);
        collectors
            .into_iter()
            .filter(|collector| Self::is_collector_requested(setting, collector.as_ref()))
            .collect()
    }

    // todo does this really require Collectors instances? Currently it seems that this can be solved via the collector source enum
    fn get_all_collectors(pool: &PgPool, client: &Client) -> Vec<Box<dyn Collector>> {
        vec![
            Box::new(NyseEventCollector::new(pool.clone(), client.clone())),
            Box::new(NyseInstrumentCollector::new(pool.clone(), client.clone())),
            Box::new(SecCompanyCollector::new(pool.clone(), client.clone())),
        ]
    }

    fn is_collector_requested(setting: &TaskSetting, collector: &dyn Collector) -> bool {
        let converted_settings_sp = setting.sp500_fields.iter().collect::<BTreeSet<_>>();
        let sp_fields = collector.get_sp_fields();
        let converted_collector_sp = sp_fields.iter().collect::<BTreeSet<_>>();
        if converted_settings_sp.is_disjoint(&converted_collector_sp) {
            return false;
        }

        if !(setting.include_sources.contains(&collector.get_source())
            || setting.include_sources.contains(&CollectorSource::All))
        {
            return false;
        }

        if setting.exclude_sources.contains(&collector.get_source()) {
            return false;
        }

        true
    }
}

// cannot be executed via execute_runner, since trait upcasting is currently not allowed in Rust :/
pub fn execute_collector(collector: Box<dyn Collector>) -> JoinHandle<Result<Option<StatsMap>, TaskError>> {
    tokio::spawn(async move { collector.run().await })
}
