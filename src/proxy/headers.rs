use hyper::{
    HeaderMap,
    header::{
        CONNECTION, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING,
        UPGRADE,
    },
};

const KEEP_ALIVE: &str = "keep-alive";

pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let connection_tokens: Vec<String> = headers
        .get(CONNECTION)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .map(|v| v.trim().to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default();
    headers.remove(CONNECTION);
    headers.remove(KEEP_ALIVE);
    headers.remove(PROXY_AUTHENTICATE);
    headers.remove(PROXY_AUTHORIZATION);
    headers.remove(TE);
    headers.remove(TRAILER);
    headers.remove(TRANSFER_ENCODING);
    headers.remove(UPGRADE);
    headers.remove(HOST);
    for token in connection_tokens {
        headers.remove(token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;
    #[test]
    fn removes_declared_connection_headers() {
        let mut h = HeaderMap::new();
        h.insert(
            CONNECTION,
            HeaderValue::from_static("keep-alive, x-private"),
        );
        h.insert("x-private", HeaderValue::from_static("secret"));
        strip_hop_by_hop(&mut h);
        assert!(!h.contains_key("x-private"));
        assert!(!h.contains_key(CONNECTION));
    }
}
