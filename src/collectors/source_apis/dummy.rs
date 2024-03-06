use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

use async_trait::async_trait;
use core::fmt::{Display, Formatter};

pub struct DummyCollector {}

impl DummyCollector {
    pub fn new() -> Self {
        DummyCollector {}
    }
}

impl Default for DummyCollector {
    fn default() -> Self {
        DummyCollector::new()
    }
}

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
