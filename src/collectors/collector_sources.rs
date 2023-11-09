use core::fmt;

use serde::Deserialize;

#[derive(Deserialize, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all(deserialize = "SCREAMING_SNAKE_CASE"))]
pub enum CollectorSource {
    NyseEvents,
    NyseCompanies,
    All,
}

impl fmt::Display for CollectorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
