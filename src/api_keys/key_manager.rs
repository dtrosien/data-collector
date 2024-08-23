use std::{
    cmp::Reverse,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration, Utc};
use config::Map;
use priority_queue::PriorityQueue;

use super::api_key::{ApiKey, ApiKeyPlatform, Status};

type KeyOrTimeoutResult = Result<(Option<Box<dyn ApiKey>>, Option<DateTime<Utc>>), KeyErrors>;
type KeyStore = Map<ApiKeyPlatform, PriorityQueue<Box<dyn ApiKey>, Reverse<DateTime<Utc>>>>;

#[derive(Debug)]
pub struct KeyManager {
    keys: KeyStore,
}

impl KeyManager {
    pub fn new() -> Self {
        KeyManager { keys: Map::new() }
    }

    pub async fn exchange_apikey_or_wait_if_non_ready(
        key_manager: Arc<Mutex<KeyManager>>,
        wait: bool,
        api_key: Box<dyn ApiKey>,
        platform: &ApiKeyPlatform,
    ) -> Option<Box<dyn ApiKey>> {
        if api_key.get_status() == Status::Ready {
            Some(api_key)
        } else {
            KeyManager::exchange_apikey_or_wait(key_manager.clone(), wait, api_key, platform).await
        }
    }

    pub async fn exchange_apikey_or_wait(
        key_manager: Arc<Mutex<KeyManager>>,
        wait: bool,
        api_key: Box<dyn ApiKey>,
        platform: &ApiKeyPlatform,
    ) -> Option<Box<dyn ApiKey>> {
        {
            let mut d = key_manager.lock().expect("msg");
            d.add_key_by_platform(api_key);
        }
        KeyManager::get_new_apikey_or_wait(key_manager, wait, platform).await
    }

    pub async fn get_new_apikey_or_wait(
        key_manager: Arc<Mutex<KeyManager>>,
        wait: bool,
        platform: &ApiKeyPlatform,
    ) -> Option<Box<dyn ApiKey>> {
        let mut g = {
            let mut d = key_manager.lock().expect("msg");
            d.get_key_and_timeout(platform)
        };
        while let Ok(f) = g {
            match f {
                (Some(_), Some(_)) => return None, // Cannot occur
                // Queue is empty
                (None, None) => {
                    if wait {
                        tokio::time::sleep(Duration::minutes(1).to_std().unwrap()).await;
                    } else {
                        return None;
                    }
                }
                (None, Some(refresh_time)) => {
                    if wait {
                        let time_difference = refresh_time - Utc::now();
                        if let Ok(sleep_duration) = time_difference.to_std() {
                            tokio::time::sleep(sleep_duration).await;
                        }
                    } else {
                        return None;
                    }
                }
                (Some(key), None) => return Some(key),
            }
            g = {
                let mut d = key_manager.lock().expect("msg");
                d.get_key_and_timeout(platform)
            };
        }
        None // Key never added to queue
    }

    pub fn add_key_by_platform(&mut self, key: Box<dyn ApiKey>) {
        let platform = key.get_platform();
        let key_value_pair = self.keys.get_mut(&platform);
        let next_update = key.next_ready_time();
        if let Some(queue) = key_value_pair {
            queue.push(key, Reverse(next_update));
        } else {
            let mut queue: PriorityQueue<Box<dyn ApiKey>, Reverse<DateTime<Utc>>> =
                PriorityQueue::new();
            queue.push(key, Reverse(next_update));
            self.keys.insert(platform, queue);
        }
    }

    pub fn get_key_and_timeout(&mut self, platform: &ApiKeyPlatform) -> KeyOrTimeoutResult {
        if let Some(pq) = self.keys.get_mut(platform) {
            {
                if let Some(pair) = pq.peek() {
                    if pair.1 .0 > Utc::now() {
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
