use crate::error::Result;
use crate::task::Runnable;
use core::fmt::{self, Display};
use futures_util::future::BoxFuture;
use sqlx::PgPool;

pub mod collector_sources;
pub mod sp500_fields;

pub trait Collector: Display + Send + Sync + Runnable {
    fn run<'a>(&self, connection_pool: &'a PgPool) -> BoxFuture<'a, Result<()>>;
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields>;
    fn get_source(&self) -> collector_sources::CollectorSource;
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Collector of source: {}", Collector::get_source(self))
    }
}
