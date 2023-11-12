use crate::utils::error::Result;
use async_trait::async_trait;

/// used when spawning tokio tasks, needs adjustment when async traits are allowed
#[async_trait]
pub trait Runnable: Send + Sync {
    async fn run(&self) -> Result<()>;
}

// currently not useful since rust does not (yet) support trait upcasting
// pub fn execute_runnable(runnable: Box<dyn Runnable>) -> JoinHandle<Result<()>> {
//     tokio::spawn(async move { runnable.run().await })
// }
