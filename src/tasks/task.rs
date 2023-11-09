use crate::{configuration::TaskSetting, source_apis::nyse::NyseEventCollector};

use sqlx::PgPool;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::collector::Collector;

pub struct Task {
    id: Uuid,
    _pool: PgPool,
    collectors: Vec<Box<dyn Collector>>,
}

impl Task {
    pub fn new(setting: &TaskSetting, db: &PgPool) -> Self {
        Task {
            id: Uuid::new_v4(),
            _pool: db.clone(),
            collectors: Self::matching_collectors(setting),
        }
    }

    fn matching_collectors<'a>(setting: &TaskSetting) -> Vec<Box<dyn Collector>> {
        let mut result = vec![];
        let collectors = Self::get_all_collectors();
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

    fn get_all_collectors() -> Vec<Box<dyn Collector>> {
        vec![Box::new(NyseEventCollector {})]
    }

    #[tracing::instrument(
    name = "Running task",
    skip(self),
    fields(
    task_id = %self.id,
    // collector = %self.collectors
    )
    )]
    pub async fn run(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // let _response = client::query_api(&self.url).await;
        //
        // db::insert_into(&self.pool)
        //     .await
        //     .expect("TODO: panic message");
        Ok(())
    }
}

pub async fn execute_task(task: Task) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
    tokio::spawn(async move { task.run().await })
}
