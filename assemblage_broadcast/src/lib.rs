extern crate cfg_if;
extern crate wasm_bindgen;

use cfg_if::cfg_if;
use js_sys::{try_iter, Array, ArrayBuffer, Date, Object, Promise, Reflect, Uint8Array, JSON};
use uuid::Uuid;
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, Response, ResponseInit, Url, UrlSearchParams};

const STATUS_OK: u16 = 200;
const STATUS_CREATED: u16 = 201;
const STATUS_BAD_REQUEST: u16 = 400;
const STATUS_UNAUTHORIZED: u16 = 401;
const STATUS_NOT_FOUND: u16 = 404;
const STATUS_METHOD_NOT_ALLOWED: u16 = 405;

type JsResult<T> = Result<T, JsValue>;

cfg_if! {
    // When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
    // allocator.
    if #[cfg(feature = "wee_alloc")] {
        extern crate wee_alloc;
        #[global_allocator]
        static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;
    }
}

#[wasm_bindgen]
pub async fn handle(kv: WorkersKvJs, req: JsValue) -> JsResult<Response> {
    let req: Request = req.dyn_into()?;
    let url = Url::new(&req.url())?;
    let pathname = url.pathname();
    let query_params = url.search_params();
    let path: Vec<&str> = pathname.split('/').skip(1).collect();
    let broadcast = path.get(1);
    let ep = path.get(2);
    let auth = req.headers().get("Authorization")?;
    let body = req.array_buffer()?;
    let kv = WorkersKv { kv };
    if let Some(&"broadcast") = path.first() {
        let resp = match (req.method().as_str(), path.len()) {
            ("GET", 2) => get_broadcast(&kv, broadcast.unwrap()).await,
            ("GET", 3) => get_episode(&kv, broadcast.unwrap(), ep.unwrap()).await,
            ("GET", _) => empty_response(STATUS_BAD_REQUEST),
            ("POST", 1) => post_broadcast(&kv, body, query_params).await,
            ("POST", _) => empty_response(STATUS_BAD_REQUEST),
            ("PUT", 3) => put_episode(&kv, broadcast.unwrap(), ep.unwrap(), auth, body).await,
            ("PUT", _) => empty_response(STATUS_BAD_REQUEST),
            ("DELETE", 2) => delete_episodes(&kv, broadcast.unwrap(), auth).await,
            ("DELETE", _) => empty_response(STATUS_BAD_REQUEST),
            ("OPTIONS", _) => empty_response(STATUS_OK),
            (_, _) => empty_response(STATUS_METHOD_NOT_ALLOWED),
        }?;
        if let Some(origin) = req.headers().get("Origin")? {
            if origin == url.origin()
                || origin.starts_with("http://localhost:")
                || origin.starts_with("http://127.0.0.1:")
            {
                resp.headers().set("Access-Control-Allow-Origin", &origin)?;
                resp.headers().set(
                    "Access-Control-Allow-Methods",
                    "GET,PUT,POST,DELETE,OPTIONS",
                )?;
                resp.headers().set("Access-Control-Allow-Headers", "*")?;
                resp.headers().set("Access-Control-Max-Age", "3000")?;
            }
        }
        Ok(resp)
    } else {
        empty_response(STATUS_BAD_REQUEST)
    }
}

async fn get_broadcast(kv: &WorkersKv, broadcast_id: &str) -> JsResult<Response> {
    let key = format!("broadcast:{}", broadcast_id);
    match kv.get_text(&key).await? {
        Some(broadcast) => {
            let broadcast = JSON::parse(&broadcast)?;
            let episodes = Reflect::get(&broadcast, &"episodes".into())?;
            let mut init = ResponseInit::new();
            init.status(STATUS_OK);
            let json = JSON::stringify(&episodes)?.as_string().unwrap();
            Response::new_with_opt_str_and_init(Some(json.as_str()), &init)
        }
        None => empty_response(STATUS_NOT_FOUND),
    }
}

async fn get_episode(kv: &WorkersKv, broadcast_id: &str, episode_id: &str) -> JsResult<Response> {
    let key = &format!("broadcast:{}:{}", broadcast_id, episode_id);
    let bytes = kv.get_vec(key).await?;
    if let Some(mut bytes) = bytes {
        let mut init = ResponseInit::new();
        init.status(STATUS_OK);
        Response::new_with_opt_u8_array_and_init(Some(bytes.as_mut_slice()), &init)
    } else {
        empty_response(STATUS_NOT_FOUND)
    }
}

async fn post_broadcast(
    kv: &WorkersKv,
    body: Promise,
    query: UrlSearchParams,
) -> JsResult<Response> {
    let broadcast_id = Uuid::new_v4();
    let seconds_now = Date::now() as u64 / 1000;
    let expiration = seconds_now + (60 * 60 * 24);
    let ep_json = match query.get("episode") {
        None => "".to_string(),
        Some(ep) => match ep.parse::<u64>() {
            Ok(ep) => {
                store_episode(kv, &broadcast_id.to_string(), expiration, ep, body).await?;
                format!("\"{}\"", ep)
            }
            Err(_) => return empty_response(STATUS_BAD_REQUEST),
        },
    };

    let token = Uuid::new_v4();
    let key = &format!("broadcast:{}", broadcast_id);
    let value = format!(
        "{{\"token\":\"{}\",\"expiration\":{},\"episodes\":[{}]}}",
        token, expiration, ep_json
    );
    kv.put_text(key, &value, expiration).await?;
    let json = format!(
        "{{\"broadcast_id\":\"{}\",\"token\":\"{}\",\"expiration\":{}}}",
        broadcast_id, token, expiration
    );
    let mut init = ResponseInit::new();
    init.status(STATUS_CREATED);
    Response::new_with_opt_str_and_init(Some(json.as_str()), &init)
}

async fn put_episode(
    kv: &WorkersKv,
    broadcast_id: &str,
    episode_id: &str,
    auth: Option<String>,
    body: Promise,
) -> JsResult<Response> {
    let broadcast = kv.get_text(&format!("broadcast:{}", broadcast_id)).await?;
    let auth = auth.unwrap_or_default();
    if let Some(broadcast) = broadcast {
        let (token, expiration, mut episodes) = parse_broadcast(&broadcast)?;
        if let Ok(token) = Uuid::parse_str(&token) {
            if auth.starts_with("Bearer ") && auth == format!("Bearer {}", token) {
                if let Ok(episode_id) = episode_id.parse::<u64>() {
                    store_episode(kv, broadcast_id, expiration, episode_id, body).await?;
                    if episodes.contains(&episode_id) {
                        empty_response(STATUS_OK)
                    } else {
                        episodes.push(episode_id);
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
                        kv.put_text(&key, &value, expiration).await?;
                        empty_response(STATUS_CREATED)
                    }
                } else {
                    empty_response(STATUS_BAD_REQUEST)
                }
            } else {
                empty_response(STATUS_UNAUTHORIZED)
            }
        } else {
            empty_response(STATUS_BAD_REQUEST)
        }
    } else {
        empty_response(STATUS_NOT_FOUND)
    }
}

async fn store_episode(
    kv: &WorkersKv,
    broadcast_id: &str,
    expiration: u64,
    episode_id: u64,
    body: Promise,
) -> JsResult<()> {
    let key = &format!("broadcast:{}:{}", broadcast_id, episode_id);
    let buffer = JsFuture::from(body).await?;
    let typed_array = Uint8Array::new_with_byte_offset(&buffer, 0);
    let mut v = vec![0; typed_array.length() as usize];
    typed_array.copy_to(v.as_mut_slice());
    let seconds_keep_alive = 60 * 60 * 12;
    kv.put_vec(key, &v, expiration + seconds_keep_alive).await?;
    Ok(())
}

async fn delete_episodes(
    kv: &WorkersKv,
    broadcast_id: &str,
    auth: Option<String>,
) -> JsResult<Response> {
    let key = format!("broadcast:{}", broadcast_id);
    let auth = auth.unwrap_or_default();
    match kv.get_text(&key).await? {
        Some(broadcast) => {
            let (token, expiration, _episodes) = parse_broadcast(&broadcast)?;
            if let Ok(token) = Uuid::parse_str(&token) {
                if auth.starts_with("Bearer ") && auth == format!("Bearer {}", token) {
                    let broadcast = JSON::parse(&broadcast)?;
                    Reflect::set(&broadcast, &"episodes".into(), &Array::new().into())?;
                    let json = JSON::stringify(&broadcast)?.as_string().unwrap();
                    kv.put_text(&key, &json, expiration).await?;
                    empty_response(STATUS_OK)
                } else {
                    empty_response(STATUS_UNAUTHORIZED)
                }
            } else {
                empty_response(STATUS_BAD_REQUEST)
            }
        }
        None => empty_response(STATUS_NOT_FOUND),
    }
}

fn parse_broadcast(broadcast: &str) -> JsResult<(String, u64, Vec<u64>)> {
    let broadcast = JSON::parse(broadcast)?;
    let token = Reflect::get(&broadcast, &"token".into())?
        .as_string()
        .unwrap_or_default();
    let expiration = Reflect::get(&broadcast, &"expiration".into())?
        .as_f64()
        .unwrap_or_default() as u64;
    let episodes_array = Reflect::get(&broadcast, &"episodes".into())?;
    let mut episodes = Vec::new();
    if let Some(episodes_array) = try_iter(&episodes_array)? {
        for ep in episodes_array {
            let ep = ep?.as_string().unwrap();
            episodes.push(ep.parse::<u64>().unwrap());
        }
    }
    Ok((token, expiration, episodes))
}

fn empty_response(statuscode: u16) -> JsResult<Response> {
    let mut init = ResponseInit::new();
    init.status(statuscode);
    Response::new_with_opt_str_and_init(None, &init)
}

struct WorkersKv {
    kv: WorkersKvJs,
}

impl WorkersKv {
    async fn put_text(&self, key: &str, value: &str, expiration: u64) -> JsResult<()> {
        let options = Object::new();
        Reflect::set(&options, &"expiration".into(), &(expiration as f64).into())?;
        self.kv
            .put(JsValue::from_str(key), value.into(), options.into())
            .await?;
        Ok(())
    }

    async fn put_vec(&self, key: &str, value: &[u8], expiration: u64) -> JsResult<()> {
        let options = Object::new();
        Reflect::set(&options, &"expiration".into(), &(expiration as f64).into())?;
        let typed_array = Uint8Array::new_with_length(value.len() as u32);
        typed_array.copy_from(value);
        self.kv
            .put(
                JsValue::from_str(key),
                typed_array.buffer().into(),
                options.into(),
            )
            .await?;
        Ok(())
    }

    async fn get_text(&self, key: &str) -> JsResult<Option<String>> {
        let options = Object::new();
        Reflect::set(&options, &"type".into(), &"text".into())?;
        Ok(self
            .kv
            .get(JsValue::from_str(key), options.into())
            .await?
            .as_string())
    }

    async fn get_vec(&self, key: &str) -> JsResult<Option<Vec<u8>>> {
        let options = Object::new();
        Reflect::set(&options, &"type".into(), &"arrayBuffer".into())?;
        let value = self.kv.get(JsValue::from_str(key), options.into()).await?;
        if value.is_null() {
            Ok(None)
        } else {
            let buffer = ArrayBuffer::from(value);
            let typed_array = Uint8Array::new_with_byte_offset(&buffer, 0);
            let mut v = vec![0; typed_array.length() as usize];
            typed_array.copy_to(v.as_mut_slice());
            Ok(Some(v))
        }
    }
}

#[wasm_bindgen]
extern "C" {
    pub type WorkersKvJs;

    #[wasm_bindgen(structural, method, catch)]
    pub async fn put(
        this: &WorkersKvJs,
        k: JsValue,
        v: JsValue,
        options: JsValue,
    ) -> JsResult<JsValue>;

    #[wasm_bindgen(structural, method, catch)]
    pub async fn get(this: &WorkersKvJs, key: JsValue, options: JsValue) -> JsResult<JsValue>;

    #[wasm_bindgen(structural, method, catch)]
    pub async fn delete(this: &WorkersKvJs, key: JsValue) -> JsResult<JsValue>;

    #[wasm_bindgen(structural, method, catch)]
    pub async fn list(this: &WorkersKvJs, options: JsValue) -> JsResult<JsValue>;
}

#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}
