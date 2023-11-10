pub mod collect;
pub mod stage;

use crate::actions::collect::CollectAction;
use crate::actions::stage::StageAction;
use crate::error::{MatchError, Result};
use crate::task::{ActionDependencies, Task};
use futures_util::future::BoxFuture;

/// Action is a trait that defines the interface for all actions.
pub trait Action: Send + Sync {
    /// transform will take an input Resource and perform an actions on it and returns another Resource.
    fn perform<'a>(&self, dependencies: ActionDependencies) -> BoxFuture<'a, Result<()>>;
}

/// BoxedAction is a boxed trait object of Action.
pub type BoxedAction = Box<dyn Action + Send + Sync>;

/// create_action creates a boxed trait object of Action from a ActionType.
pub fn create_action(action_type: &ActionType) -> Result<BoxedAction> {
    match action_type {
        ActionType::Collect => Ok(Box::new(CollectAction {})),
        ActionType::Stage => Ok(Box::new(StageAction {})),
        ActionType::Unknown => Err(Box::new(MatchError {})),
    }
}

/// Possible Actions
#[derive(Debug, Clone, serde::Deserialize)]
pub enum ActionType {
    Collect,
    Stage,
    Unknown,
}
