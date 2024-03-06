use core::fmt;

use serde::Deserialize;

// todo delete or keep for later when/if field based selection is back in schedule?

#[derive(Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
#[serde(rename_all(deserialize = "SCREAMING_SNAKE_CASE"))]
pub enum CollectorSource {
    All,
    Dummy,
    NyseEvents,
    NyseInstruments,
    SecCompanies,
    Unknown,
}

impl fmt::Display for CollectorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
