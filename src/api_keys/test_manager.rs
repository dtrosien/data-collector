use super::test::KeyChain;
use super::test::TestEnum;
use chrono::{DateTime, Utc};
use config::Map;
use priority_queue::PriorityQueue;

pub struct KeyManager {
    keys: Map<TestEnum, PriorityQueue<KeyChain, DateTime<Utc>>>,
}
