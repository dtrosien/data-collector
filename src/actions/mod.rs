use crate::error::{MatchError, Result};
use futures_util::future::BoxFuture;

/// Action is a trait that defines the interface for all actions.
pub trait Action: Send + Sync {
    /// transform will take an input Resource and perform an actions on it and returns another Resource.
    fn perform<'a>(&self) -> BoxFuture<'a, Result<()>>;
}

/// BoxedAction is a boxed trait object of Action.
pub type BoxedAction = Box<dyn Action>;

/// create_action creates a boxed trait object of Action from a ActionType.
pub fn create_action(action_type: &ActionType) -> Result<BoxedAction> {
    match action_type {
        ActionType::Collect => Ok(Box::new(CollectAction {})),
        ActionType::Stage => Ok(Box::new(StageAction {})),
        ActionType::Unknown => Err(Box::new(MatchError {})),
    }
}

/// Resource is the type on which Actions are performed
pub struct Resource {}

#[derive(Debug, Clone)]
pub enum ActionType {
    Collect,
    Stage,
    Unknown,
}

/// Reads in Table of the Resource
pub struct CollectAction {}

impl Action for CollectAction {
    fn perform<'a>(&self) -> BoxFuture<'a, Result<()>> {
        todo!()
    }
}

pub struct StageAction {}

impl Action for StageAction {
    fn perform<'a>(&self) -> BoxFuture<'a, Result<()>> {
        todo!()
    }
}
