use crate::tasks::actions::Action;
use crate::tasks::ActionDependencies;
use crate::utils::error::Result;
use async_trait::async_trait;

/// Stages Data for DB
pub struct StageAction {}

#[async_trait]
impl Action for StageAction {
    async fn execute(&self, _dependencies: ActionDependencies) -> Result<()> {
        todo!()
    }
}
