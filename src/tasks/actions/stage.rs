use crate::tasks::actions::action::Action;
use crate::tasks::task::ActionDependencies;
use crate::utils::errors::Result;
use async_trait::async_trait;

/// Stages Data for DB
pub struct StageAction {}

#[async_trait]
impl Action for StageAction {
    async fn execute(&self, _dependencies: ActionDependencies) -> Result<()> {
        todo!()
    }
}
