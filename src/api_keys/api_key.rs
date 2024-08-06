use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use secrecy::{ExposeSecret, Secret};
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;

// pub trait MaxRequests {
//     const MAX_REQUESTS: u32;
// }
pub trait ApiKey: Sync + Send {
    // fn new(key: String) -> Self;
    fn expose_secret(&self) -> &String;
    fn refresh_if_possible(&mut self) -> bool;
    fn next_refresh_possible(&self) -> chrono::DateTime<Utc>;
    fn get_status(&self) -> Status;
    fn get_platform(&self) -> ApiKeyPlatform;
    fn get_secret(&mut self) -> &Secret<String>;
    fn set_status(&mut self, new_status: Status);
}

impl PartialEq for dyn ApiKey + 'static {
    fn eq(&self, _other: &Self) -> bool {
        if self.get_platform() == (_other.get_platform())
            && self.expose_secret() == _other.expose_secret()
        {
            return true;
        }
        false
    }
}
impl Eq for dyn ApiKey + 'static {}

impl Hash for dyn ApiKey + 'static {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        self.expose_secret().hash(_state);
        self.get_platform().hash(_state)
    }
}

impl Debug for dyn ApiKey + 'static {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.get_platform())
        // todo!
    }
}

#[derive(Debug, Clone)]
pub struct FinancialmodelingprepKey {
    api_key: Secret<String>,
    platform: ApiKeyPlatform,
    status: Status,
    last_use: DateTime<Utc>,
    counter: u32,
}

// impl MaxRequests for FinancialmodelingprepKey {
//     const MAX_REQUESTS: u32 = 250;
// }

impl FinancialmodelingprepKey {
    pub fn new(key: String) -> Self {
        FinancialmodelingprepKey {
            api_key: Secret::new(key),
            platform: ApiKeyPlatform::Financialmodelingprep,
            status: Status::Ready,
            last_use: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            counter: 0,
        }
    }

    fn compute_next_refresh_time(&self) -> chrono::DateTime<Utc> {
        //Key refreshes at 19 o'clock UTC
        if self.last_use.hour() < 19 {
            return Utc
                .with_ymd_and_hms(
                    self.last_use.year(),
                    self.last_use.month(),
                    self.last_use.day(),
                    19,
                    0,
                    0,
                )
                .single()
                .expect("Unwrapping of date today should always work");
        }
        Utc.with_ymd_and_hms(
            self.last_use.year(),
            self.last_use.month(),
            self.last_use.day() + 1,
            19,
            0,
            0,
        )
        .single()
        .expect("Unwrapping of date today should always work")
    }
}

impl ApiKey for FinancialmodelingprepKey {
    fn expose_secret(&self) -> &String {
        self.api_key.expose_secret()
    }

    fn refresh_if_possible(&mut self) -> bool {
        todo!()
    }

    fn next_refresh_possible(&self) -> chrono::DateTime<Utc> {
        match self.get_status() {
            Status::Ready => Utc::now(),
            Status::Exhausted => self.compute_next_refresh_time(),
        }
    }

    fn get_status(&self) -> Status {
        self.status.clone()
    }

    fn get_platform(&self) -> ApiKeyPlatform {
        self.platform.clone()
    }

    fn get_secret(&mut self) -> &Secret<String> {
        self.last_use = Utc::now();
        self.counter += 1;
        println!("Counter at: {}", &self.counter);
        if self.counter == 250 {
            self.status = Status::Exhausted;
        }
        &self.api_key
    }

    fn set_status(&mut self, new_status: Status) {
        println!("new status: {}", new_status);
        self.status = new_status;
    }
}

struct PolygonKey {
    api_key: Secret<String>,
    platform: ApiKeyPlatform,
    status: Status,
    last_use: DateTime<Utc>,
    counter: u8,
}

// impl MaxRequests for PolygonKey {
//     const MAX_REQUESTS: u32 = 5;
// }

impl PolygonKey {
    pub fn new(key: String) -> Self {
        PolygonKey {
            api_key: Secret::new(key),
            platform: ApiKeyPlatform::Polygon,
            status: Status::Ready,
            last_use: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            counter: 0,
        }
    }
}

impl ApiKey for PolygonKey {
    fn expose_secret(&self) -> &String {
        // self.last_use = Utc::now();
        self.api_key.expose_secret()
    }

    fn refresh_if_possible(&mut self) -> bool {
        todo!()
    }

    fn next_refresh_possible(&self) -> chrono::DateTime<Utc> {
        match self.get_status() {
            Status::Ready => Utc::now(),
            Status::Exhausted => self.last_use + Duration::minutes(1),
        }
    }

    fn get_status(&self) -> Status {
        self.status.clone()
    }

    fn get_platform(&self) -> ApiKeyPlatform {
        self.platform.clone()
    }
    fn get_secret(&mut self) -> &Secret<String> {
        &self.api_key
    }

    fn set_status(&mut self, new_status: Status) {
        self.status = new_status;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ApiKeyPlatform {
    Financialmodelingprep,
    Polygon,
}

impl fmt::Display for ApiKeyPlatform {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Status {
    Ready,
    Exhausted,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod test {
    use secrecy::Secret;

    use crate::api_keys::api_key::{ApiKey, ApiKeyPlatform, PolygonKey, Status};

    use super::FinancialmodelingprepKey;

    #[test]
    fn create_financialmodelingprep_key() {
        let fin_key = FinancialmodelingprepKey::new("key".to_string());
        assert_eq!(fin_key.counter, 0);
        assert_eq!(fin_key.get_status(), Status::Ready);
        assert_eq!(
            fin_key.get_platform(),
            ApiKeyPlatform::Financialmodelingprep
        );
        assert_eq!(fin_key.expose_secret(), &"key".to_string());
    }

    #[test]
    fn create_polygon_key() {
        let fin_key = PolygonKey::new("key".to_string());
        assert_eq!(fin_key.counter, 0);
        assert_eq!(fin_key.get_status(), Status::Ready);
        assert_eq!(fin_key.get_platform(), ApiKeyPlatform::Polygon);
        assert_eq!(fin_key.expose_secret(), &"key".to_string());
    }
}
