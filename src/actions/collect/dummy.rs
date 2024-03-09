use crate::dag_schedule::task::{Runnable, StatsMap, TaskError};

use async_trait::async_trait;
use core::fmt::{Display, Formatter};
use tracing::debug;
#[derive(Debug)]
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
    #[tracing::instrument(name = "Run dummy collector", skip(self))]
    async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
        dummy_function(8).await;
        Ok(None)
    }
}

#[tracing::instrument]
async fn dummy_function(some_input: u8) {
    debug!("do stuff: {}", some_input);
}

impl Display for DummyCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "DummyCollector")
    }
}
