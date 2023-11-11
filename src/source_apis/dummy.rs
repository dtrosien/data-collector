use crate::collectors::collector_sources::CollectorSource;
use crate::collectors::sp500_fields::Fields;
use crate::collectors::Collector;
use crate::runner::Runnable;
use core::fmt::{Display, Formatter};
use futures_util::future::BoxFuture;

pub struct DummyCollector {}

impl Runnable for DummyCollector {
    #[tracing::instrument(name = "Start running dummy collector", skip(self))]
    fn run<'a>(&self) -> BoxFuture<'a, crate::error::Result<()>> {
        let f = async move { Ok(()) };
        Box::pin(f)
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
