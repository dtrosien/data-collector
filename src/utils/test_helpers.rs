use reqwest::Client;
use std::sync::OnceLock;

pub fn get_test_client() -> Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(Client::new).clone()
}
