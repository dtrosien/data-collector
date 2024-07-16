use std::{collections::BinaryHeap, sync::Arc};

use chrono::{DateTime, Utc};
use config::Map;
use priority_queue::PriorityQueue;
// use vtable::VBox;

use super::api_key::{self, ApiKey, ApiKeyPlatform, FinancialmodelingprepKey};

pub struct KeyManager {
    // fin_mod_keys: PriorityQueue<api_key::FinancialmodelingprepKey, DateTime<Utc>>,
    // polygon_keys: PriorityQueue<api_key::PolygonKey, DateTime<Utc>>,
    keys: Map<ApiKeyPlatform, PriorityQueue<Arc<dyn ApiKey>, DateTime<Utc>>>,
}

// impl KeyManager<FinancialmodelingprepKey> {}

#[cfg(test)]
mod test {
    use super::KeyManager;

    #[test]
    fn create_struct_key_manager() {}
}
