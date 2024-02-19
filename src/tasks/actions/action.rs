use crate::dag_scheduler::task::{StatsMap, TaskError};
use anyhow::anyhow;
use async_trait::async_trait;

use crate::tasks::actions::collect::CollectAction;
use crate::tasks::actions::stage::StageAction;
use crate::tasks::task::ActionDependencies;

/// Action is a trait that defines the interface for all actions.
#[async_trait]
pub trait Action: Send + Sync {
    /// transform will take an input Resource and perform an actions on it and returns another Resource.
    async fn execute(&self, dependencies: ActionDependencies) -> Result<Option<StatsMap>, TaskError>;
}

/// BoxedAction is a boxed trait object of Action.
pub type BoxedAction = Box<dyn Action + Send + Sync>;

/// create_action creates a boxed trait object of Action from a ActionType.
pub fn create_action(action_type: &ActionType) -> Result<BoxedAction, TaskError> {
    match action_type {
        ActionType::Collect => Ok(Box::new(CollectAction {})),
        ActionType::Stage => Ok(Box::new(StageAction {})),
        ActionType::Unknown => Err(TaskError::UnexpectedError(anyhow!("Action unknown"))),
    }
}

/// Possible Actions
#[derive(Debug, Clone, serde::Deserialize)]
pub enum ActionType {
    Collect,
    Stage,
    Unknown,
}
