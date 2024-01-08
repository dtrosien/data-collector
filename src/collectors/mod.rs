use crate::tasks::runnable::Runnable;
use core::fmt::{self, Display};

pub mod collector_sources;
pub mod source_apis;
pub mod sp500_fields;
pub mod stagers;
pub mod utils;

pub trait Collector: Runnable + Display + Send + Sync {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields>;
    fn get_source(&self) -> collector_sources::CollectorSource;
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Collector of source: {}", Collector::get_source(self))
    }
}

pub trait Stager: Runnable + Display + Send + Sync {
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields>;
    fn get_source(&self) -> collector_sources::CollectorSource;
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Stager of source: {}", Stager::get_source(self))
    }
}
