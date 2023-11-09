use crate::error::{MatchError, Result};

/// Action is a trait that defines the interface for all actions.
pub trait Action: Send {
    /// transform will take an input Resource and perform an actions on it and returns another Resource.
    fn perform(&self, input: Resource) -> Result<Resource>;
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
    fn perform(&self, input: Resource) -> Result<Resource> {
        todo!()
    }
}

pub struct StageAction {}

impl Action for StageAction {
    fn perform(&self, input: Resource) -> Result<Resource> {
        todo!()
    }
}
