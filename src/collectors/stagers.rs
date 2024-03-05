// pub trait Stager: Runnable + Display + Send + Sync {
//     fn get_sp_fields(&self) -> Vec<sp500_fields::Fields>;
//     fn get_source(&self) -> collector_sources::CollectorSource;
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "Stager of source: {}", Stager::get_source(self))
//     }
// }
