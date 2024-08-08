use std::cmp::Reverse;

use chrono::{DateTime, Utc};
use config::Map;
use priority_queue::PriorityQueue;
// use vtable::VBox;

use super::api_key::{ApiKey, ApiKeyPlatform, Status};

type KeyOrTimeoutResult = Result<(Option<Box<dyn ApiKey>>, Option<DateTime<Utc>>), KeyErrors>;

#[derive(Debug)]
pub struct KeyManager {
    keys: Map<ApiKeyPlatform, PriorityQueue<Box<dyn ApiKey>, Reverse<DateTime<Utc>>>>,
}

impl KeyManager {
    pub fn new() -> Self {
        KeyManager { keys: Map::new() }
    }

    pub fn add_key_by_platform(&mut self, key: Box<dyn ApiKey>) {
        let platform = key.get_platform();
        let key_value_pair = self.keys.get_mut(&platform);
        let next_update = key.next_refresh_possible();
        println!("Next update possible: {}", next_update);
        if let Some(queue) = key_value_pair {
            queue.push(key, Reverse(next_update));
        } else {
            let mut queue: PriorityQueue<Box<dyn ApiKey>, Reverse<DateTime<Utc>>> =
                PriorityQueue::new();
            queue.push(key, Reverse(next_update));
            self.keys.insert(platform, queue);
        }
    }

    pub fn get_key_and_timeout(&mut self, platform: ApiKeyPlatform) -> KeyOrTimeoutResult {
        if let Some(pq) = self.keys.get_mut(&platform) {
            println!("#####queue size: {}", pq.len());
            println!("#####queue: {:?}", pq);
            {
                if let Some(pair) = pq.peek() {
                    println!("peek time: {:?}", pair);
                    println!("now: {:?}", Utc::now());
                    if pair.1 .0 > Utc::now() {
                        println!("returning waiting time");
                        return Ok((None, Some(pair.1 .0)));
                    }
                } else {
                    return Ok((None, None)); //No key available in queue, at all
                }
            }

            if let Some(mut pair) = pq.pop() {
                pair.0.set_status(Status::Ready);
                return Ok((Some(pair.0), None)); //return key
            }
            Err(KeyErrors::KeyNeverProvided(anyhow::Error::msg(
                "Code should not be reachable",
            ))) // should not be reachable
        } else {
            Err(KeyErrors::KeyNeverProvided(anyhow::Error::msg(
                "FinancialmodelingprepCompanyProfileColletor key not provided",
            ))) // No key was ever added
        }
    }
}

impl Default for KeyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum KeyErrors {
    #[error("Key of this type was never provided")]
    KeyNeverProvided(#[source] anyhow::Error),
}

#[cfg(test)]
mod test {
    use crate::api_keys::api_key::FinancialmodelingprepKey;

    use super::KeyManager;

    #[test]
    fn create_struct_key_manager() {
        let key = FinancialmodelingprepKey::new("key".to_string());
        let mut km = KeyManager::new();
        km.add_key_by_platform(Box::new(key));
    }
}
