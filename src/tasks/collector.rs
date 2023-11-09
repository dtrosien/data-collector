use core::fmt::{self, Display};
use std::{error::Error, pin::Pin, process::Output};

use futures::{future::BoxFuture, Future};
use sqlx::Postgres;

use crate::tasks::{collector_sources, sp500_fields};

pub trait Collector: Display + Send + Sync {
    fn run(
        &self,
        connection_pool: &sqlx::Pool<Postgres>,
    ) -> Box<dyn Future<Output = Result<(), Box<dyn Error>>>>;
    // Result<(), Box<dyn Error + Send + Sync>>
    fn get_sp_fields(&self) -> Vec<sp500_fields::Fields>;
    fn get_source(&self) -> collector_sources::CollectorSource;
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Collector of source: {}",
            self::Collector::get_source(self)
        )
    }
}
