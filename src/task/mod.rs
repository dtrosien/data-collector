use crate::actions::BoxedAction;
use crate::collectors::Collector;
use crate::error::Result;
use crate::{configuration::TaskSetting, source_apis::nyse::NyseEventCollector};
use futures_util::future::BoxFuture;
use sqlx::PgPool;
use std::{
    collections::{BTreeSet, HashSet},
    error::Error,
};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub trait Runnable: Send + Sync {
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>>;
}

#[derive(Clone)]
pub struct TaskMeta {}

pub struct Task {
    id: Uuid,
    priority: f32,
    actions: Vec<BoxedAction>,
    meta: TaskMeta, //collectors: Vec<Box<dyn Collector>>,
}

impl Task {
    pub fn new(setting: &TaskSetting, db: &PgPool) -> Self {
        Task {
            id: Uuid::new_v4(),
            priority: 1.0,
            actions: vec![], //todo build actions
            meta: TaskMeta {},
            //collectors: Self::matching_collectors(setting, db.clone()),
        }
    }

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

impl Runnable for Task {
    #[tracing::instrument(
    name = "Running task",
    skip(self),
    fields(
    task_id = %self.id,
    // collectors = %self.collectors
    )
    )]
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>> {
        let action_futures = self
            .actions
            .iter()
            .map(|x| x.perform(self.meta.clone()))
            .collect::<Vec<BoxFuture<'a, Result<()>>>>();

        let joined_result = async move {
            let mut errors = Vec::new();

            for action_future in action_futures {
                if let Err(err) = action_future.await {
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

pub fn execute_runnable(runnable: Box<dyn Runnable>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { runnable.run().await })
}
