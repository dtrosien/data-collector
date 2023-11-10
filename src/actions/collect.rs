use crate::actions::Action;
use crate::collectors::Collector;
use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::source_apis::nyse::NyseEventCollector;
use crate::task::ActionDependencies;
use futures_util::future::{try_join_all, BoxFuture};
use sqlx::PgPool;
use std::collections::BTreeSet;
use tokio::task::JoinHandle;

/// Collect from sources via Collectors
pub struct CollectAction {}

impl Action for CollectAction {
    fn perform<'a>(&self, meta: ActionDependencies) -> BoxFuture<'a, Result<()>> {
        let collectors = CollectAction::matching_collectors(&meta.setting, meta.pool.clone());

        let handles: Vec<JoinHandle<Result<()>>> =
            collectors.into_iter().map(execute_collector).collect();

        let joined_result = async move { join_handle_results(handles).await };

        Box::pin(joined_result)
    }
}

impl CollectAction {
    fn matching_collectors<'a>(setting: &TaskSetting, pool: PgPool) -> Vec<Box<dyn Collector>> {
        let mut result = vec![];
        let collectors = Self::get_all_collectors(pool);
        let f: Vec<_> = collectors
            .into_iter()
            .filter(|collector| Self::is_collector_requested(setting, collector))
            .collect();
        result
    }

    fn is_collector_requested(setting: &TaskSetting, collector: &Box<dyn Collector>) -> bool {
        let converted_settings_sp = setting.sp500_fields.iter().collect::<BTreeSet<_>>();
        let sp_fields = collector.get_sp_fields();
        let converted_collector_sp = sp_fields.iter().collect::<BTreeSet<_>>();
        if converted_settings_sp.is_disjoint(&converted_collector_sp) {
            return false;
        }

        if !setting.include_sources.contains(&&collector.get_source()) {
            return false;
        }

        if setting.exclude_sources.contains(&&collector.get_source()) {
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

/// Joins all results from handles into one,
/// if any future returns an error then all other handles will
/// be canceled and an error will be returned immediately
pub async fn join_handle_results_strict(handles: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    try_join_all(handles)
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

/// Joins all results from handles into one,
/// if any future returns an error all other handles will still be processed
pub async fn join_handle_results(handles: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    let mut errors = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    errors.push(e);
                }
            }
            Err(e) => errors.push(e.into()),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err("One or more tasks failed".into())
    }
}
