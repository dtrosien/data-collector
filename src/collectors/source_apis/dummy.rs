use crate::collectors::collector_sources::CollectorSource;
use crate::collectors::sp500_fields::Fields;
use crate::collectors::Collector;
use crate::tasks::runnable::Runnable;
use crate::utils::errors::Result;
use async_trait::async_trait;
use core::fmt::{Display, Formatter};

pub struct DummyCollector {}

#[async_trait]
impl Runnable for DummyCollector {
    #[tracing::instrument(name = "Start running dummy collector", skip(self))]
    async fn run(&self) -> Result<()> {
        Ok(())
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
        CollectorSource::All
    }
}
