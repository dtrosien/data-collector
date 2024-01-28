use crate::collectors::collector_sources::CollectorSource;
use crate::collectors::stagers::Stager;
use crate::collectors::staging::nyse_instruments_staging::NyseInstrumentStager;
use crate::collectors::staging::sec_companies_staging::SecCompanyStager;
use crate::configuration::TaskSetting;
use crate::tasks::actions::action::Action;
use crate::tasks::task::ActionDependencies;
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use async_trait::async_trait;
use sqlx::PgPool;
use std::collections::BTreeSet;
use tokio::task::JoinHandle;

/// Stages Data for DB
pub struct StageAction {}

#[async_trait]
impl Action for StageAction {
    async fn execute(&self, dependencies: ActionDependencies) -> Result<()> {
        let stagers =
            StageAction::matching_collectors(&dependencies.setting, dependencies.pool.clone());

        let handles: Vec<JoinHandle<Result<()>>> =
            stagers.into_iter().map(execute_collector).collect();

        join_handle_results(handles).await
    }
}

impl StageAction {
    fn matching_collectors(setting: &TaskSetting, pool: PgPool) -> Vec<Box<dyn Stager>> {
        let collectors = Self::get_all_stagers(pool);
        collectors
            .into_iter()
            .filter(|collector| Self::is_stager_requested(setting, collector.as_ref()))
            .collect()
    }

    // todo does this really require Stagers instances? Currently it seems that this can be solved via the stager source enum
    fn get_all_stagers(pool: PgPool) -> Vec<Box<dyn Stager>> {
        vec![
            Box::new(SecCompanyStager::new(pool.clone())),
            Box::new(NyseInstrumentStager::new(pool.clone())),
        ]
    }

    fn is_stager_requested(setting: &TaskSetting, stager: &dyn Stager) -> bool {
        let converted_settings_sp = setting.sp500_fields.iter().collect::<BTreeSet<_>>();
        let sp_fields = stager.get_sp_fields();
        let converted_collector_sp = sp_fields.iter().collect::<BTreeSet<_>>();
        if converted_settings_sp.is_disjoint(&converted_collector_sp) {
            return false;
        }

        if !(setting.include_sources.contains(&stager.get_source())
            || setting.include_sources.contains(&CollectorSource::All))
        {
            return false;
        }

        if setting.exclude_sources.contains(&stager.get_source()) {
            return false;
        }

        true
    }
}

// cannot be executed via execute_runner, since trait upcasting is currently not allowed in Rust :/
pub fn execute_collector(collector: Box<dyn Stager>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { collector.run().await })
}
