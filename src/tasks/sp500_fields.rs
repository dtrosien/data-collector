use serde::Deserialize;

#[derive(Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all(deserialize = "SCREAMING_SNAKE_CASE"))]
pub enum Fields {
    Nyse,
    Location,
    Nasdaq,
    MarketCap,
    MonthTradingVolume,
}
