use crate::actions::Action;
use crate::collectors::Collector;
use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::future_utils::join_handle_results;
use crate::source_apis::nyse_events::NyseEventCollector;
use crate::task::ActionDependencies;
use futures_util::future::BoxFuture;
use sqlx::PgPool;
use std::collections::BTreeSet;
use tokio::task::JoinHandle;

/// Collect from sources via Collectors
pub struct CollectAction {}

impl Action for CollectAction {
    fn perform<'a>(&self, dependencies: ActionDependencies) -> BoxFuture<'a, Result<()>> {
        let collectors =
            CollectAction::matching_collectors(&dependencies.setting, dependencies.pool.clone());

        let handles: Vec<JoinHandle<Result<()>>> =
            collectors.into_iter().map(execute_collector).collect();

        let joined_result = async move { join_handle_results(handles).await };

        Box::pin(joined_result)
    }
}

impl CollectAction {
    fn matching_collectors(setting: &TaskSetting, pool: PgPool) -> Vec<Box<dyn Collector>> {
        let collectors = Self::get_all_collectors(pool);
        collectors
            .into_iter()
            .filter(|collector| Self::is_collector_requested(setting, collector.as_ref()))
            .collect()
    }

    fn is_collector_requested(setting: &TaskSetting, collector: &dyn Collector) -> bool {
        let converted_settings_sp = setting.sp500_fields.iter().collect::<BTreeSet<_>>();
        let sp_fields = collector.get_sp_fields();
        let converted_collector_sp = sp_fields.iter().collect::<BTreeSet<_>>();
        if converted_settings_sp.is_disjoint(&converted_collector_sp) {
            return false;
        }

        if !setting.include_sources.contains(&collector.get_source()) {
            return false;
        }

        if setting.exclude_sources.contains(&collector.get_source()) {
            return false;
        }

        true
    }

    fn get_all_collectors(pool: PgPool) -> Vec<Box<dyn Collector>> {
        vec![Box::new(NyseEventCollector::new(pool))]
    }
}

// cannot be executed via execute_runner, since trait upcasting is currently not allowed in Rust :/
pub fn execute_collector(collector: Box<dyn Collector>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { collector.run().await })
}
