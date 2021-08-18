//! Test suite for calling the (locally running) server from a client
#![cfg(all(
    not(target_arch = "wasm32"),
    feature = "assemblage-broadcast-integration-tests"
))]

use std::{thread::sleep, time::Duration};

use serde::Deserialize;

const BASE_URL: &str = if cfg!(feature = "workers-localhost") {
    "http://localhost:8787"
} else if cfg!(feature = "workers-env-prod") {
    "https://assemblage-broadcast.assemblage.workers.dev"
} else {
    "https://assemblage-broadcast-dev.assemblage.workers.dev"
};

#[test]
fn get_invalid_route() -> Result<(), reqwest::Error> {
    let resp = reqwest::blocking::get(format!("{}/invalid/id", BASE_URL))?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(resp.text()?, "");
    Ok(())
}

#[test]
fn get_broadcast_not_found() -> Result<(), reqwest::Error> {
    let broadcast_id = 1234;
    let resp = reqwest::blocking::get(format!("{}/broadcast/{}", BASE_URL, broadcast_id))?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(resp.text()?, "");
    Ok(())
}

#[test]
fn get_episode_not_found() -> Result<(), reqwest::Error> {
    let broadcast_id = 1234;
    let episode_id = 1;
    let resp = reqwest::blocking::get(format!(
        "{}/broadcast/{}/{}",
        BASE_URL, broadcast_id, episode_id
    ))?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(resp.text()?, "");
    Ok(())
}

#[test]
fn post_and_get_broadcast() -> Result<(), reqwest::Error> {
    let episode1_id = "1";
    let bytes_ep1 = vec![1, 2, 3, 4, 5];
    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{}/broadcast?episode={}", BASE_URL, episode1_id))
        .body(bytes_ep1.clone())
        .send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let BroadcastResponse {
        broadcast_id,
        token,
    } = resp.json()?;
    assert_ne!(broadcast_id, "");
    assert_ne!(token, "");

    let resp = reqwest::blocking::get(format!(
        "{}/broadcast/{}/{}",
        BASE_URL, broadcast_id, episode1_id
    ))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.bytes()?, bytes_ep1);

    sleep(Duration::from_secs(2));

    let episode2_id = "2";
    let bytes_ep2 = vec![6, 7, 8, 9];
    let client = reqwest::blocking::Client::new();
    let resp = client
        .put(format!(
            "{}/broadcast/{}/{}",
            BASE_URL, broadcast_id, episode2_id
        ))
        .bearer_auth(token.clone())
        .body(bytes_ep2.clone())
        .send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(resp.text()?, "");

    sleep(Duration::from_secs(2));

    let client = reqwest::blocking::Client::new();
    let resp = client
        .put(format!(
            "{}/broadcast/{}/{}",
            BASE_URL, broadcast_id, episode2_id
        ))
        .bearer_auth(token.clone())
        .body(bytes_ep2.clone())
        .send()?;
    // the status should normally be 200 OK, but can be 201 CREATED if the
    // previous PUT was not yet propagated to the edge location where the second
    // PUT is processed
    assert!(
        (resp.status() == reqwest::StatusCode::OK)
            || (resp.status() == reqwest::StatusCode::CREATED)
    );
    assert_eq!(resp.text()?, "");

    // ensure propagation to all edge locations
    sleep(Duration::from_secs(60));

    let resp = reqwest::blocking::get(format!("{}/broadcast/{}", BASE_URL, broadcast_id))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.text()?,
        format!("[\"{}\",\"{}\"]", episode1_id, episode2_id)
    );

    let resp = reqwest::blocking::get(format!(
        "{}/broadcast/{}/{}",
        BASE_URL, broadcast_id, episode2_id
    ))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.bytes()?, bytes_ep2);

    let client = reqwest::blocking::Client::new();
    let resp = client
        .delete(format!("{}/broadcast/{}", BASE_URL, broadcast_id))
        .bearer_auth(token)
        .send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text()?, "");

    // ensure propagation to all edge locations
    sleep(Duration::from_secs(60));

    let resp = reqwest::blocking::get(format!("{}/broadcast/{}", BASE_URL, broadcast_id))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text()?, "[]");
    Ok(())
}

#[test]
fn post_episode_unauthorized() -> Result<(), reqwest::Error> {
    let client = reqwest::blocking::Client::new();
    let resp = client.post(format!("{}/broadcast", BASE_URL)).send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let broadcast_id = resp.json::<BroadcastResponse>()?.broadcast_id;
    let token = 12345;

    sleep(Duration::from_secs(2));

    let bytes = vec![1, 2, 3, 4, 5];
    let client = reqwest::blocking::Client::new();
    let resp = client
        .put(format!("{}/broadcast/{}/1", BASE_URL, broadcast_id))
        .bearer_auth(token)
        .body(bytes)
        .send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(resp.text()?, "");

    let resp = reqwest::blocking::get(format!("{}/broadcast/{}", BASE_URL, broadcast_id))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text()?, "[]");
    Ok(())
}

#[test]
fn delete_episode_unauthorized() -> Result<(), reqwest::Error> {
    let client = reqwest::blocking::Client::new();
    let resp = client.post(format!("{}/broadcast", BASE_URL)).send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let BroadcastResponse {
        broadcast_id,
        token,
    } = resp.json()?;
    assert_ne!(broadcast_id, "");
    assert_ne!(token, "");

    let episode_id = "1";
    let bytes = vec![1, 2, 3, 4, 5];
    let client = reqwest::blocking::Client::new();
    let resp = client
        .put(format!(
            "{}/broadcast/{}/{}",
            BASE_URL, broadcast_id, episode_id
        ))
        .bearer_auth(token)
        .body(bytes.clone())
        .send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    assert_eq!(resp.text()?, "");

    // ensure propagation to all edge locations
    sleep(Duration::from_secs(60));

    let resp = reqwest::blocking::get(format!("{}/broadcast/{}", BASE_URL, broadcast_id))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text()?, format!("[\"{}\"]", episode_id));

    let resp = reqwest::blocking::get(format!(
        "{}/broadcast/{}/{}",
        BASE_URL, broadcast_id, episode_id
    ))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.bytes()?, bytes);

    let client = reqwest::blocking::Client::new();
    let resp = client
        .delete(format!("{}/broadcast/{}", BASE_URL, broadcast_id))
        .send()?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(resp.text()?, "");

    let resp = reqwest::blocking::get(format!("{}/broadcast/{}", BASE_URL, broadcast_id))?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text()?, format!("[\"{}\"]", episode_id));
    Ok(())
}

#[derive(Deserialize)]
struct BroadcastResponse {
    broadcast_id: String,
    token: String,
}
