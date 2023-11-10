use crate::actions::Action;
use crate::collectors::Collector;
use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::source_apis::nyse::NyseEventCollector;
use crate::task::{execute_runnable, Runnable, Task, TaskMeta};
use futures_util::future::BoxFuture;
use sqlx::PgPool;
use std::collections::BTreeSet;
use tokio::task::JoinHandle;

/// Collect from sources via Collectors
pub struct CollectAction {}

impl Action for CollectAction {
    fn perform<'a>(&self, meta: TaskMeta) -> BoxFuture<'a, Result<()>> {
        let mut collectors = CollectAction::matching_collectors(&meta.setting, meta.pool.clone());
        let mut handles: Vec<JoinHandle<Result<()>>> = vec![];
        while !collectors.is_empty() {
            // https://stackoverflow.com/questions/28632968/why-doesnt-rust-support-trait-object-upcasting
            handles.push(execute_runnable(collectors.pop().unwrap()))
        }
        let joined_result = async move {
            let mut errors = Vec::new();

            for handle in handles {
                if let Err(err) = handle.await {
                    errors.push(err);
                }
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err("".into())
            }
        };
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
