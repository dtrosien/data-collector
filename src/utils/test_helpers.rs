use crate::utils::telemetry::{get_subscriber, init_subscriber};
use reqwest::Client;
use std::sync::OnceLock;

pub fn get_test_client() -> Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(Client::new).clone()
}

// Ensure that the `tracing` stack is only initialised once using `once_cell`
// to enable logs in tests start test with "TEST_LOG=true cargo test"
pub fn init_tracing() {
    static TRACING: OnceLock<()> = OnceLock::new();
    TRACING.get_or_init(|| {
        let default_filter_level = "info".to_string();
        let subscriber_name = "test".to_string();

        if std::env::var("TEST_LOG").is_ok_and(|x| x.contains("true")) {
            let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::stdout);
            init_subscriber(subscriber);
        } else {
            let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::sink);
            init_subscriber(subscriber);
        }
    });
}
