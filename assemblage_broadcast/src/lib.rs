extern crate cfg_if;
extern crate wasm_bindgen;

use std::collections::HashMap;

use cfg_if::cfg_if;
use js_sys::Date;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;
use worker::*;
use worker_kv::KvStore;

const KV_BINDING: &str = "BROADCAST";

const STATUS_CREATED: u16 = 201;
const STATUS_BAD_REQUEST: u16 = 400;
const STATUS_UNAUTHORIZED: u16 = 401;
const STATUS_NOT_FOUND: u16 = 404;

cfg_if! {
    if #[cfg(feature = "wee_alloc")] {
        extern crate wee_alloc;
        #[global_allocator]
        static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;
    }
}

cfg_if! {
    if #[cfg(feature = "console_error_panic_hook")] {
        extern crate console_error_panic_hook;
        pub use self::console_error_panic_hook::set_once as set_panic_hook;
    } else {
        #[inline]
        pub fn set_panic_hook() {}
    }
}

fn log_request(req: &Request) {
    console_log!(
        "{} - [{}], located at: {:?}, within: {}",
        Date::now().to_string(),
        req.path(),
        req.cf().coordinates().unwrap_or_default(),
        req.cf().region().unwrap_or("unknown region".into())
    );
}

#[event(fetch)]
pub async fn main(req: Request, env: Env) -> Result<Response> {
    log_request(&req);
    set_panic_hook();

    let url = req.url()?.clone();
    let headers = req.headers().clone();
    let mut resp = Router::new(())
        .get_async("/broadcast/:broadcast_id", |_req, ctx| async move {
            if let Some(broadcast_id) = ctx.param("broadcast_id") {
                let kv = ctx.kv(KV_BINDING)?;
                get_broadcast(&kv, broadcast_id).await
            } else {
                Response::error("Bad Request", STATUS_BAD_REQUEST)
            }
        })
        .get_async(
            "/broadcast/:broadcast_id/:episode_id",
            |_req, ctx| async move {
                if let (Some(broadcast_id), Some(episode_id)) =
                    (ctx.param("broadcast_id"), ctx.param("episode_id"))
                {
                    let kv = ctx.kv(KV_BINDING)?;
                    get_episode(&kv, broadcast_id, episode_id).await
                } else {
                    Response::error("Bad Request", STATUS_BAD_REQUEST)
                }
            },
        )
        .post_async("/broadcast", |mut req, ctx| async move {
            let kv = ctx.kv(KV_BINDING)?;
            let body = req.bytes().await?;
            let url = req.url()?;
            let query_params: HashMap<_, _> = url.query_pairs().into_owned().collect();
            let episode_id = query_params.get("episode");
            post_broadcast(&kv, body, episode_id).await
        })
        .put_async(
            "/broadcast/:broadcast_id/:episode_id",
            |mut req, ctx| async move {
                if let (Some(broadcast_id), Some(episode_id)) =
                    (ctx.param("broadcast_id"), ctx.param("episode_id"))
                {
                    let kv = ctx.kv(KV_BINDING)?;
                    let body = req.bytes().await?;
                    let auth = req.headers().get("Authorization")?;
                    put_episode(&kv, broadcast_id, episode_id, auth, body).await
                } else {
                    Response::error("Bad Request", STATUS_BAD_REQUEST)
                }
            },
        )
        .delete_async("/broadcast/:broadcast_id", |req, ctx| async move {
            if let Some(broadcast_id) = ctx.param("broadcast_id") {
                let kv = ctx.kv(KV_BINDING)?;
                let auth = req.headers().get("Authorization")?;
                delete_episodes(&kv, broadcast_id, auth).await
            } else {
                Response::error("Bad Request", STATUS_BAD_REQUEST)
            }
        })
        .options_async("/*whatever", |req, _ctx| async move {
            if req.method() == Method::Options {
                Response::empty()
            } else {
                Response::error("Not Found", STATUS_NOT_FOUND)
            }
        })
        .run(req, env)
        .await?;
    if let Some(origin) = headers.get("Origin")? {
        if origin == url.origin().ascii_serialization()
            || origin.starts_with("http://localhost:")
            || origin.starts_with("http://127.0.0.1:")
        {
            resp.headers_mut()
                .set("Access-Control-Allow-Origin", &origin)?;
            resp.headers_mut().set(
                "Access-Control-Allow-Methods",
                "GET,PUT,POST,DELETE,OPTIONS",
            )?;
            resp.headers_mut()
                .set("Access-Control-Allow-Headers", "*")?;
            resp.headers_mut().set("Access-Control-Max-Age", "3000")?;
        }
    }
    Ok(resp)
}

#[derive(Deserialize)]
struct Broadcast {
    token: String,
    expiration: u64,
    episodes: Vec<String>,
}

async fn get_broadcast(kv: &KvStore, broadcast_id: &str) -> Result<Response> {
    let key = format!("broadcast:{}", broadcast_id);
    match kv.get(&key).await? {
        Some(broadcast) => {
            let broadcast: Broadcast = broadcast.as_json()?;
            Response::from_json(&broadcast.episodes)
        }
        None => Response::error("Not Found", STATUS_NOT_FOUND),
    }
}

async fn get_episode(kv: &KvStore, broadcast_id: &str, episode_id: &str) -> Result<Response> {
    let key = &format!("broadcast:{}:{}", broadcast_id, episode_id);
    let bytes = kv.get(key).await?;
    if let Some(bytes) = bytes {
        Response::from_bytes(bytes.as_bytes().to_vec())
    } else {
        Response::error("Not Found", STATUS_NOT_FOUND)
    }
}

async fn post_broadcast(kv: &KvStore, body: Vec<u8>, ep_id: Option<&String>) -> Result<Response> {
    let broadcast_id = Uuid::new_v4();
    let seconds_now = Date::now() as u64 / 1000;
    let expiration = seconds_now + (60 * 60 * 24);
    let ep_json = match ep_id {
        None => "".to_string(),
        Some(ep) => match ep.parse::<u64>() {
            Ok(ep) => {
                let key = &format!("broadcast:{}:{}", broadcast_id, ep);
                let seconds_keep_alive = 60 * 60 * 12;
                kv.put(key, body)?
                    .expiration(expiration + seconds_keep_alive)
                    .execute()
                    .await?;
                format!("\"{}\"", ep)
            }
            Err(_) => return Response::error("Bad Request", STATUS_BAD_REQUEST),
        },
    };

    let token = Uuid::new_v4();
    let key = &format!("broadcast:{}", broadcast_id);
    let value = format!(
        "{{\"token\":\"{}\",\"expiration\":{},\"episodes\":[{}]}}",
        token, expiration, ep_json
    );
    kv.put(key, &value)?
        .expiration(expiration)
        .execute()
        .await?;
    Response::from_json(
        &json!({ "broadcast_id": broadcast_id, "token": token, "expiration": expiration }),
    )
    .map(|resp| resp.with_status(STATUS_CREATED))
}

async fn put_episode(
    kv: &KvStore,
    broadcast_id: &str,
    episode_id: &str,
    auth: Option<String>,
    body: Vec<u8>,
) -> Result<Response> {
    let broadcast = kv.get(&format!("broadcast:{}", broadcast_id)).await?;
    let auth = auth.unwrap_or_default();
    if let Some(broadcast) = broadcast {
        let Broadcast {
            token,
            expiration,
            mut episodes,
        } = broadcast.as_json()?;
        if let Ok(token) = Uuid::parse_str(&token) {
            if auth.starts_with("Bearer ") && auth == format!("Bearer {}", token) {
                if let Ok(episode_id) = episode_id.parse::<u64>() {
                    let key = &format!("broadcast:{}:{}", broadcast_id, episode_id);
                    let seconds_keep_alive = 60 * 60 * 12;
                    kv.put_bytes(key, &body)?
                        .expiration(expiration + seconds_keep_alive)
                        .execute()
                        .await?;
                    if episodes.contains(&format!("{}", episode_id)) {
                        Response::empty()
                    } else {
                        episodes.push(format!("{}", episode_id));
                        episodes.sort_unstable();
                        let key = format!("broadcast:{}", broadcast_id);
                        let episodes_json_array = episodes
                            .into_iter()
                            .map(|ep| format!("\"{}\"", ep))
                            .collect::<Vec<String>>()
                            .join(",");
                        let value = format!(
                            "{{\"token\":\"{}\",\"expiration\":{},\"episodes\":[{}]}}",
                            token, expiration, episodes_json_array
                        );
                        kv.put(&key, &value)?
                            .expiration(expiration)
                            .execute()
                            .await?;
                        Response::empty().map(|resp| resp.with_status(STATUS_CREATED))
                    }
                } else {
                    Response::error("Bad Request", STATUS_BAD_REQUEST)
                }
            } else {
                Response::error("Unauthorized", STATUS_UNAUTHORIZED)
            }
        } else {
            Response::error("Bad Request", STATUS_BAD_REQUEST)
        }
    } else {
        Response::error("Not Found", STATUS_NOT_FOUND)
    }
}

async fn delete_episodes(
    kv: &KvStore,
    broadcast_id: &str,
    auth: Option<String>,
) -> Result<Response> {
    let key = format!("broadcast:{}", broadcast_id);
    let auth = auth.unwrap_or_default();
    match kv.get(&key).await? {
        Some(broadcast) => {
            let Broadcast {
                token, expiration, ..
            } = broadcast.as_json()?;
            if let Ok(token) = Uuid::parse_str(&token) {
                if auth.starts_with("Bearer ") && auth == format!("Bearer {}", token) {
                    let value = format!(
                        "{{\"token\":\"{}\",\"expiration\":{},\"episodes\":[]}}",
                        token, expiration
                    );
                    kv.put(&key, &value)?
                        .expiration(expiration)
                        .execute()
                        .await?;
                    Response::empty()
                } else {
                    Response::error("Unauthorized", STATUS_UNAUTHORIZED)
                }
            } else {
                Response::error("Bad Request", STATUS_BAD_REQUEST)
            }
        }
        None => Response::error("Not Found", STATUS_NOT_FOUND),
    }
}
