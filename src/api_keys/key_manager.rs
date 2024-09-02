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

    #[tracing::instrument(level = "debug", skip_all)]
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
    use chrono::{Duration, Utc};

    use crate::api_keys::api_key::{
        ApiKey, ApiKeyPlatform, FinancialmodelingprepKey, PolygonKey, Status,
    };

    use super::KeyManager;

    // Tested
    // Create object and add key throws no errors
    // Adding key will be in correct queue
    // Adding 3 keys with different refresh times, will order them correctly when retrieving
    // Adding 4 keys for two queues each, will put them in two queues
    // Adding two times the same key will result in one key in the queue
    // Queue with exhausted non refresh-able key will return waiting time
    // Queue with exhausted refresh-able key will return key
    // Queue with exhausted refresh-able key will return key and key is ready
    // Queue with non refresh-able, ready key will return ready key and same counter
    // Missing tests
    // Queue with refresh-able, ready key will return ready key and counter at zero // Ready again must be faked. Maybe use  tokio::time::pause, and change all Utc::now() calls to Instant.now()
    // Get key from empty stores does not cause problems
    // Get key from Platform which was never initialized returns correctly
    // Get key from initialized Platform returns correctly

    #[test]
    fn create_struct_key_manager() {
        let key = FinancialmodelingprepKey::new("key".to_string());
        let mut km = KeyManager::new();
        km.add_key_by_platform(Box::new(key));
    }

    #[test]
    fn add_key_gives_correct_queue_size() {
        let key = FinancialmodelingprepKey::new("key".to_string());
        let mut km = KeyManager::new();
        km.add_key_by_platform(Box::new(key));
        let queue_size = km
            .keys
            .get(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .len();
        assert_eq!(queue_size, 1);
    }

    #[test]
    fn add_three_keys_with_differing_refresh_time_then_returned_in_correct_order() {
        //Setup exhausted keys
        let now = Utc::now();
        let mut key_1 =
            FinancialmodelingprepKey::new_with_time("key_1".to_string(), now - Duration::days(3));
        key_1.set_status(Status::Exhausted);
        let mut key_2 =
            FinancialmodelingprepKey::new_with_time("key_2".to_string(), now - Duration::days(1));
        key_2.set_status(Status::Exhausted);
        let mut key_3 =
            FinancialmodelingprepKey::new_with_time("key_3".to_string(), now - Duration::days(2));
        key_3.set_status(Status::Exhausted);
        let mut km = KeyManager::new();
        km.add_key_by_platform(Box::new(key_1));
        km.add_key_by_platform(Box::new(key_2));
        km.add_key_by_platform(Box::new(key_3));

        let (res_1, _) = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap();
        let (res_2, _) = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap();
        let (res_3, _) = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap();

        assert_eq!(res_1.unwrap().expose_secret_for_data_structure(), "key_1");
        assert_eq!(res_2.unwrap().expose_secret_for_data_structure(), "key_3");
        assert_eq!(res_3.unwrap().expose_secret_for_data_structure(), "key_2");
    }

    #[test]
    fn adding_4_keys_of_2_types_then_queue_size_is_correct() {
        let mut km = KeyManager::new();

        let key_1 = FinancialmodelingprepKey::new("key1".to_string());
        let key_2 = FinancialmodelingprepKey::new("key2".to_string());
        let key_3 = PolygonKey::new("key3".to_string());
        let key_4 = PolygonKey::new("key4".to_string());
        km.add_key_by_platform(Box::new(key_1));
        km.add_key_by_platform(Box::new(key_2));
        km.add_key_by_platform(Box::new(key_3));
        km.add_key_by_platform(Box::new(key_4));
        let queue_size_fin_mod = km
            .keys
            .get(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .len();
        assert_eq!(queue_size_fin_mod, 2);
        let queue_size_poly = km.keys.get(&ApiKeyPlatform::Polygon).unwrap().len();
        assert_eq!(queue_size_poly, 2);
    }

    #[test]
    fn adding_2_times_the_same_api_key_then_queue_size_is_one() {
        let mut km = KeyManager::new();

        let key_1 = FinancialmodelingprepKey::new("key1".to_string());
        let key_2 = FinancialmodelingprepKey::new("key1".to_string());
        km.add_key_by_platform(Box::new(key_1));
        km.add_key_by_platform(Box::new(key_2));
        let queue_size_fin_mod = km
            .keys
            .get(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .len();
        assert_eq!(queue_size_fin_mod, 1);
    }

    #[test]
    fn queue_with_exhausted_key_and_not_refresh_able_then_time_is_returned() {
        let mut km = KeyManager::new();
        let mut key = FinancialmodelingprepKey::new_with_time("key1".to_string(), Utc::now());
        key.set_status(Status::Exhausted);
        km.add_key_by_platform(Box::new(key));

        let time = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .1;
        assert_eq!(time.is_some(), true);
    }

    #[test]
    fn queue_with_exhausted_key_and_refresh_able_then_key_is_returned() {
        let mut km = KeyManager::new();
        let mut key = FinancialmodelingprepKey::new("key1".to_string());
        key.set_status(Status::Exhausted);
        km.add_key_by_platform(Box::new(key));

        let time = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .0;
        assert_eq!(time.is_some(), true);
    }

    #[test]
    fn queue_with_exhausted_key_and_refresh_able_then_key_is_returned_and_ready() {
        let mut km = KeyManager::new();
        let mut key = FinancialmodelingprepKey::new("key1".to_string());
        key.set_status(Status::Exhausted);
        km.add_key_by_platform(Box::new(key));

        let time = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .0
            .unwrap();

        assert_eq!(time.get_status(), Status::Ready);
    }

    #[test]
    fn queue_with_ready_key_and_non_refresh_able_then_key_is_returned_and_counter_stays_the_same() {
        let mut km = KeyManager::new();
        let mut key = FinancialmodelingprepKey::new("key1".to_string());
        key.get_secret();
        assert_eq!(key.get_usage_counter(), 1);

        km.add_key_by_platform(Box::new(key));
        let key1 = km
            .get_key_and_timeout(&ApiKeyPlatform::Financialmodelingprep)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(key1.get_usage_counter(), 1);
    }
}
