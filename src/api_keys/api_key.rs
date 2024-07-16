use chrono::prelude::*;
use chrono::DateTime;
use chrono::Utc;
use secrecy::{ExposeSecret, Secret};
use std::hash::Hash;
use std::sync::Arc;

// pub trait MaxRequests {
//     const MAX_REQUESTS: u32;
// }
pub trait ApiKey {
    // fn new(key: String) -> Self;
    fn expose_secret(&mut self) -> &String;
    fn refresh_if_possible(&mut self) -> bool;
    fn next_refresh_possible(&self) -> chrono::DateTime<Utc>;
    fn get_status(&self) -> Status;
    fn get_platform(&self) -> ApiKeyPlatform;
    fn get_secret(&mut self) -> &Secret<String>;
}

impl PartialEq for dyn ApiKey + 'static {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}
impl Eq for dyn ApiKey + 'static {}

impl Hash for dyn ApiKey + 'static {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        todo!()
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
}

impl ApiKey for FinancialmodelingprepKey {
    fn expose_secret(&mut self) -> &String {
        self.last_use = Utc::now();
        self.api_key.expose_secret()
    }

    fn refresh_if_possible(&mut self) -> bool {
        todo!()
    }

    fn next_refresh_possible(&self) -> chrono::DateTime<Utc> {
        todo!()
    }

    fn get_status(&self) -> Status {
        self.status.clone()
    }

    fn get_platform(&self) -> ApiKeyPlatform {
        self.platform.clone()
    }

    fn get_secret(&mut self) -> &Secret<String> {
        self.counter += 1;
        println!("Counter at: {}", &self.counter);
        if self.counter == 250 {
            self.status = Status::Exhausted;
        }
        &self.api_key
    }
}

struct PolygonKey {
    api_key: Secret<String>,
    platform: ApiKeyPlatform,
    status: Status,
    last_use: DateTime<Utc>,
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
        }
    }
}

impl ApiKey for PolygonKey {
    fn expose_secret(&mut self) -> &String {
        self.last_use = Utc::now();
        self.api_key.expose_secret()
    }

    fn refresh_if_possible(&mut self) -> bool {
        todo!()
    }

    fn next_refresh_possible(&self) -> chrono::DateTime<Utc> {
        todo!()
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiKeyPlatform {
    Financialmodelingprep,
    Polygon,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Ready,
    Exhausted,
}
