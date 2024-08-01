use chrono::{DateTime, Utc};
use config::Map;
use priority_queue::PriorityQueue;
// use vtable::VBox;

use super::api_key::{ApiKey, ApiKeyPlatform};

#[derive(Debug)]
pub struct KeyManager {
    // fin_mod_keys: PriorityQueue<api_key::FinancialmodelingprepKey, DateTime<Utc>>,
    // polygon_keys: PriorityQueue<api_key::PolygonKey, DateTime<Utc>>,
    keys: Map<ApiKeyPlatform, PriorityQueue<Box<dyn ApiKey>, DateTime<Utc>>>,
}

impl KeyManager {
    pub fn new() -> Self {
        KeyManager { keys: Map::new() }
    }

    pub fn add_key_by_platform(&mut self, key: Box<dyn ApiKey>) {
        let platform = key.get_platform();
        let key_value_pair = self.keys.get_mut(&platform);
        let next_update = key.next_refresh_possible().clone();
        if let Some(queue) = key_value_pair {
            queue.push((key), next_update);
        } else {
            let mut queue: PriorityQueue<Box<dyn ApiKey>, DateTime<Utc>> = PriorityQueue::new();
            queue.push((key), next_update);
            self.keys.insert(platform, queue);
            // println!("no")
        }
        println!("adding key test")
    }

    pub fn get_key_and_timeout(
        &mut self,
        platform: ApiKeyPlatform,
    ) -> Result<(Option<Box<dyn ApiKey>>, Option<DateTime<Utc>>), KeyErrors> {
        // let mut result: Result<(Option<Box<dyn ApiKey>>, Option<DateTime<Utc>>), KeyErrors> =
        //     Ok((None, None));
        if let Some(pq) = self.keys.get_mut(&platform) {
            println!("pq: size: {}", pq.len());
            {
                if let Some(pair) = pq.peek_mut() {
                    if pair.1 > &Utc::now() {
                        // return time at which the key refreshes
                        // self.keys.insert(platform, pq);
                        return Ok((None, Some(pair.1.clone())));
                    }
                } else {
                    // self.keys.insert(platform, pq);
                    return Ok((None, None)); //No key available in queue, at all
                }
            }

            if let Some(pair) = pq.pop() {
                // self.keys.insert(platform, pq);
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

#[derive(thiserror::Error, Debug)]
pub enum KeyErrors {
    #[error("Key of this type was never provided")]
    KeyNeverProvided(#[source] anyhow::Error),
}

#[cfg(test)]
mod test {
    use crate::api_keys::api_key::{ApiKeyPlatform, FinancialmodelingprepKey};

    use super::KeyManager;

    #[test]
    fn create_struct_key_manager() {
        let key = FinancialmodelingprepKey::new("key".to_string());
        let mut km = KeyManager::new();
        km.add_key_by_platform(Box::new(key));
    }
}
