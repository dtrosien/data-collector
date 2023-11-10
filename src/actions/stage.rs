use crate::actions::Action;
use crate::error::Result;
use crate::task::{ActionDependencies, Task};
use futures_util::future::BoxFuture;

/// Stages Data for DB
pub struct StageAction {}

impl Action for StageAction {
    fn perform<'a>(&self, meta: ActionDependencies) -> BoxFuture<'a, Result<()>> {
        todo!()
    }
}
