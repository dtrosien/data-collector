use chrono::prelude::*;
use chrono::DateTime;
use chrono::Days;
use chrono::Duration;
use chrono::Utc;
use secrecy::{ExposeSecret, Secret};
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use tracing::debug;

pub trait ApiKey: Sync + Send {
    fn expose_secret_for_data_structure(&self) -> &String;
    fn refresh_if_possible(&mut self) -> bool;
    fn next_ready_time(&self) -> chrono::DateTime<Utc>;
    fn get_status(&self) -> Status;
    fn get_platform(&self) -> ApiKeyPlatform;
    fn get_secret(&mut self) -> &Secret<String>;
    fn set_status(&mut self, new_status: Status);
}

impl PartialEq for dyn ApiKey + 'static {
    fn eq(&self, _other: &Self) -> bool {
        if self.get_platform() == (_other.get_platform())
            && self.expose_secret_for_data_structure() == _other.expose_secret_for_data_structure()
        {
            return true;
        }
        false
    }
}
impl Eq for dyn ApiKey + 'static {}

impl Hash for dyn ApiKey + 'static {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        self.expose_secret_for_data_structure().hash(_state);
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

    fn compute_next_full_refresh_time(&self) -> chrono::DateTime<Utc> {
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
    fn expose_secret_for_data_structure(&self) -> &String {
        self.api_key.expose_secret()
    }

    fn refresh_if_possible(&mut self) -> bool {
        let mut last_possible_refresh = Utc::now();
        //Set minutes and seconds to 0
        last_possible_refresh = last_possible_refresh
            .with_minute(0)
            .expect("Setting minutes to zero should never fail");
        last_possible_refresh = last_possible_refresh
            .with_second(0)
            .expect("Setting seconds to zero should never fail");
        //Check if current time is past todays refresh time
        if last_possible_refresh.hour() >= 19 {
            last_possible_refresh = last_possible_refresh
                .with_hour(19)
                .expect("Setting hours to 19 should never fail");
        } else {
            // Last refresh possible was yesterday
            last_possible_refresh = last_possible_refresh
                .with_hour(19)
                .expect("Setting hours to 19 should never fail");
            last_possible_refresh = last_possible_refresh
                .checked_sub_days(Days::new(1))
                .expect("Setting hours to 19 should never fail");
        }
        if self.last_use < last_possible_refresh {
            self.counter = 0;
            self.set_status(Status::Ready);
            return true;
        }
        return false;
    }

    fn next_ready_time(&self) -> chrono::DateTime<Utc> {
        match self.get_status() {
            Status::Ready => Utc::now(),
            Status::Exhausted => self.compute_next_full_refresh_time(),
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
        debug!("Counter at: {}", &self.counter);
        if self.counter == 250 {
            self.status = Status::Exhausted;
        }
        &self.api_key
    }

    fn set_status(&mut self, new_status: Status) {
        if new_status == Status::Exhausted {
            self.counter = 0;
        }
        self.status = new_status;
    }
}

pub struct PolygonKey {
    api_key: Secret<String>,
    platform: ApiKeyPlatform,
    status: Status,
    last_use: DateTime<Utc>,
    counter: u8,
}

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
    fn expose_secret_for_data_structure(&self) -> &String {
        self.api_key.expose_secret()
    }

    fn refresh_if_possible(&mut self) -> bool {
        if self.last_use < Utc::now() + Duration::minutes(1) {
            self.set_status(Status::Ready);
            self.counter = 0;
            return true;
        }
        return false;
    }

    fn next_ready_time(&self) -> chrono::DateTime<Utc> {
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
        self.last_use = Utc::now();
        self.counter += 1;
        debug!("Counter at: {}", &self.counter);
        if self.counter == 5 {
            self.set_status(Status::Exhausted);
        }
        &self.api_key
    }

    fn set_status(&mut self, new_status: Status) {
        if new_status == Status::Exhausted {
            self.counter = 0;
        }
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
    // #### Implemented tests ###
    // Keys can be created
    // Exposing secret updates counter
    // Exposing secret updates last use
    // Setting status to expired, sets counter to 0
    // Counter reaching limit sets the status to expired
    // Calling the secret for the data structure does not increase the counter
    // next_refresh_possible is computed correctly
    // Next refresh of Ready key is now
    // Refreshing the key is possible, refreshing sets the counter to 0
    // #### Missing tests ###
    // refresh_if_possible calculates date correctly

    use chrono::{Datelike, Duration, TimeDelta, TimeZone, Utc};

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
        assert_eq!(
            fin_key.expose_secret_for_data_structure(),
            &"key".to_string()
        );
    }

    #[test]
    fn create_polygon_key() {
        let fin_key = PolygonKey::new("key".to_string());
        assert_eq!(fin_key.counter, 0);
        assert_eq!(fin_key.get_status(), Status::Ready);
        assert_eq!(fin_key.get_platform(), ApiKeyPlatform::Polygon);
        assert_eq!(
            fin_key.expose_secret_for_data_structure(),
            &"key".to_string()
        );
    }

    #[test]
    fn poly_expose_secret_counts_counter() {
        let mut fin_key = PolygonKey::new("key".to_string());
        assert_eq!(fin_key.counter, 0);
        fin_key.get_secret();
        assert_eq!(fin_key.counter, 1);
    }

    #[test]
    fn finrep_expose_secret_counts_counter() {
        let mut finrep_key = FinancialmodelingprepKey::new("key".to_string());
        assert_eq!(finrep_key.counter, 0);
        finrep_key.get_secret();
        assert_eq!(finrep_key.counter, 1);
    }

    #[test]
    fn poly_exposing_updates_last_use() {
        let mut fin_key = PolygonKey::new("key".to_string());
        let now = Utc::now();
        assert_ne!(fin_key.last_use.year(), Utc::now().year());
        fin_key.get_secret();
        let time_diff = fin_key.last_use - now;
        assert!(time_diff < TimeDelta::minutes(1));
    }

    #[test]
    fn finrep_exposing_updates_last_use() {
        let mut fin_key = FinancialmodelingprepKey::new("key".to_string());
        let now = Utc::now();
        assert_ne!(fin_key.last_use.year(), Utc::now().year());
        fin_key.get_secret();
        let time_diff = fin_key.last_use - now;
        assert!(time_diff < TimeDelta::minutes(1));
    }

    #[test]
    fn poly_status_to_exhausted_zeroes_counter() {
        let mut fin_key = PolygonKey::new("key".to_string());
        fin_key.get_secret();
        assert_eq!(fin_key.counter, 1);
        fin_key.set_status(Status::Exhausted);
        assert_eq!(fin_key.counter, 0);
    }

    #[test]
    fn finrep_status_to_exhausted_zeroes_counter() {
        let mut fin_key = FinancialmodelingprepKey::new("key".to_string());
        fin_key.get_secret();
        assert_eq!(fin_key.counter, 1);
        fin_key.set_status(Status::Exhausted);
        assert_eq!(fin_key.counter, 0);
    }

    #[test]
    fn poly_exhausting_tries_set_status_to_exhausted() {
        let mut fin_key = PolygonKey::new("key".to_string());
        for _ in 0..5 {
            assert_eq!(fin_key.status, Status::Ready);
            fin_key.get_secret();
        }
        assert_eq!(fin_key.status, Status::Exhausted);
    }

    #[test]
    fn finrep_exhausting_tries_set_status_to_exhausted() {
        let mut fin_key = FinancialmodelingprepKey::new("key".to_string());
        for _ in 0..250 {
            assert_eq!(fin_key.status, Status::Ready);
            fin_key.get_secret();
        }
        assert_eq!(fin_key.status, Status::Exhausted);
    }

    #[test]
    fn poly_secret_of_data_structure_preserves_count() {
        let fin_key = PolygonKey::new("key".to_string());
        fin_key.expose_secret_for_data_structure();
        assert_eq!(fin_key.counter, 0);
    }

    #[test]
    fn finrep_secret_of_data_structure_preserves_count() {
        let fin_key = FinancialmodelingprepKey::new("key".to_string());
        fin_key.expose_secret_for_data_structure();
        assert_eq!(fin_key.counter, 0);
    }

    #[test]
    fn poly_refreshes_within_a_minute() {
        let mut fin_key = PolygonKey::new("key".to_string());
        fin_key.set_status(Status::Exhausted);
        let one_minute = Duration::minutes(1);
        assert_eq!(fin_key.last_use + one_minute, fin_key.next_ready_time());
    }

    #[test]
    fn finrep_refreshes_at_seven_same_day() {
        let mut fin_key = FinancialmodelingprepKey::new("key".to_string());
        fin_key.set_status(Status::Exhausted);
        fin_key.last_use = Utc.with_ymd_and_hms(2000, 1, 1, 13, 1, 1).unwrap();
        assert_eq!(
            Utc.with_ymd_and_hms(2000, 1, 1, 19, 0, 0).unwrap(),
            fin_key.next_ready_time()
        );
    }

    #[test]
    fn finrep_refreshes_at_seven_next_day() {
        let mut fin_key = FinancialmodelingprepKey::new("key".to_string());
        fin_key.set_status(Status::Exhausted);
        fin_key.last_use = Utc.with_ymd_and_hms(2000, 1, 1, 20, 1, 1).unwrap();
        assert_eq!(
            Utc.with_ymd_and_hms(2000, 1, 2, 19, 0, 0).unwrap(),
            fin_key.next_ready_time()
        );
    }

    #[test]
    fn poly_ready_key_is_ready_now() {
        let fin_key = PolygonKey::new("key".to_string());
        let one_minute = Duration::minutes(1);
        let now = Utc::now();
        assert!(now < fin_key.next_ready_time());
        assert!(now + one_minute > fin_key.next_ready_time());
    }

    #[test]
    fn finrep_ready_key_is_ready_now() {
        let fin_key = FinancialmodelingprepKey::new("key".to_string());
        let one_minute = Duration::minutes(1);
        let now = Utc::now();
        assert!(now < fin_key.next_ready_time());
        assert!(now + one_minute > fin_key.next_ready_time());
    }

    // Refreshing the key is possible, refreshing sets the counter to 0

    #[test]
    fn poly_refreshing_ready_key_sets_counter_to_zero() {
        let mut fin_key = PolygonKey::new("key".to_string());
        fin_key.get_secret();
        assert_eq!(fin_key.counter, 1);
        fin_key.last_use = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(fin_key.refresh_if_possible(), true);
        assert_eq!(fin_key.counter, 0);
        assert_eq!(fin_key.get_status(), Status::Ready);
    }

    #[test]
    fn finrep_refreshing_ready_key_sets_counter_to_zero() {
        let mut fin_key = FinancialmodelingprepKey::new("key".to_string());
        fin_key.get_secret();
        assert_eq!(fin_key.counter, 1);
        fin_key.last_use = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(fin_key.refresh_if_possible(), true);
        assert_eq!(fin_key.counter, 0);
        assert_eq!(fin_key.get_status(), Status::Ready);
    }
}
