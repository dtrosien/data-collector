use reqwest::Response;

pub async fn query_api(url: &str) -> Response {
    // todo this is just a dummy impl, maybe cache client etc
    let client = reqwest::Client::new();

    client
        .get(url)
        .send()
        .await
        .expect("Failed to execute request.")
}
