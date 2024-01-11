use crate::collectors::{collector_sources, sp500_fields};
use crate::tasks::runnable::Runnable;
use core::fmt;
use core::fmt::Display;

pub trait Collector: Runnable + Display + Send + Sync {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields>;
    fn get_source(&self) -> collector_sources::CollectorSource;
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Collector of source: {}", Collector::get_source(self))
    }
}
