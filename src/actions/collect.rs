use crate::actions::Action;
use crate::error::Result;
use crate::task::{Task, TaskMeta};
use futures_util::future::BoxFuture;

/// Collect from sources via Collectors
pub struct CollectAction {}

impl Action for CollectAction {
    fn perform<'a>(&self, meta: TaskMeta) -> BoxFuture<'a, Result<()>> {
        todo!()
    }
}
