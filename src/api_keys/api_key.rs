use std::clone;

use chrono::prelude::*;
use chrono::DateTime;
use chrono::Utc;
use secrecy::{ExposeSecret, Secret};

trait api_key {
    fn new(key: String) -> Self;
    fn expose_secret(&mut self) -> &String;
    fn refresh_if_possible(&mut self) -> bool;
    fn next_refresh_possible(&self) -> chrono::DateTime<Utc>;
    fn get_status(&self) -> Status;
    fn get_platform(&self) -> ApiKeyPlatform;
}

struct FinancialmodelingprepKey {
    api_key: Secret<String>,
    platform: ApiKeyPlatform,
    status: Status,
    last_use: DateTime<Utc>,
}

impl api_key for FinancialmodelingprepKey {
    fn new(key: String) -> Self {
        FinancialmodelingprepKey {
            api_key: Secret::new(key),
            platform: ApiKeyPlatform::Financialmodelingprep,
            status: Status::Ready,
            last_use: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
        }
    }

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
}

struct PolygonKey {
    api_key: Secret<String>,
    platform: ApiKeyPlatform,
    status: Status,
    last_use: DateTime<Utc>,
}

impl api_key for PolygonKey {
    fn new(key: String) -> Self {
        PolygonKey {
            api_key: Secret::new(key),
            platform: ApiKeyPlatform::Polygon,
            status: Status::Ready,
            last_use: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
        }
    }

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
}

#[derive(Clone)]
enum ApiKeyPlatform {
    Financialmodelingprep,
    Polygon,
}

#[derive(Clone)]
enum Status {
    Ready,
    Exhausted,
}
