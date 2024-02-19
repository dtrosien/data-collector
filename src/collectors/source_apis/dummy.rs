use crate::collectors::collector::Collector;
use crate::collectors::collector_sources::CollectorSource;
use crate::collectors::sp500_fields::Fields;
use crate::dag_scheduler::task::{Runnable, StatsMap, TaskError};

use async_trait::async_trait;
use core::fmt::{Display, Formatter};

pub struct DummyCollector {}

#[async_trait]
impl Runnable for DummyCollector {
    #[tracing::instrument(name = "Start running dummy collector", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        Ok(None)
    }
}

impl Display for DummyCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "DummyCollector")
    }
}

impl Collector for DummyCollector {
    fn get_sp_fields(&self) -> Vec<Fields> {
        vec![Fields::Nyse]
    }

    fn get_source(&self) -> CollectorSource {
        CollectorSource::Dummy
    }
}
