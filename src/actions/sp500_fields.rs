use serde::Deserialize;

// todo delete or keep for later when/if field based selection is back in schedule?

#[derive(Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Clone)]
#[serde(rename_all(deserialize = "SCREAMING_SNAKE_CASE"))]
pub enum Fields {
    Nyse,
    Location,
    Nasdaq,
    MarketCap,
    MonthTradingVolume,
}
