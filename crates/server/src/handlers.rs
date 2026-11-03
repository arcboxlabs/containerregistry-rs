//! Request routing and the blob/upload endpoints.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use containerregistry_image::Digest;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::error::RegistryError;
use crate::manifests::{self, Repo};
use crate::storage::{BlobData, BlobStore, Upload, Uploads, random_id, random_u64};

/// Shared registry state.
pub(crate) struct Inner {
    pub(crate) blobs: BlobStore,
    pub(crate) uploads: Uploads,
    /// Blobs pushed or mounted into each repository (used for mounts).
    pub(crate) links: Mutex<HashMap<String, HashSet<String>>>,
    pub(crate) repos: RwLock<BTreeMap<String, Repo>>,
    pub(crate) referrers: bool,
    pub(crate) warnings: Vec<(f64, String)>,
    pub(crate) log_requests: bool,
}

enum Route {
    Base,
    Catalog,
    Blob { name: String, digest: String },
    UploadStart { name: String },
    Upload { name: String, id: String },
    Manifest { name: String, reference: String },
    Tags { name: String },
    Referrers { name: String, digest: String },
}

/// Parses `/v2/...` paths. Repository names may contain slashes, so the
/// endpoint is recognised from the trailing path segments.
fn parse_route(path: &str) -> Option<Route> {
    if path == "/v2" || path == "/v2/" {
        return Some(Route::Base);
    }
    let rest = path.strip_prefix("/v2/")?;
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    if rest == "_catalog" {
        return Some(Route::Catalog);
    }
    let segs: Vec<&str> = rest.split('/').collect();
    let n = segs.len();
    if n < 3 {
        return None;
    }
    let name = |k: usize| segs[..k].join("/");
    let last = segs[n - 1].to_string();
    Some(match (segs[n - 2], segs[n - 1]) {
        ("blobs", "uploads") => Route::UploadStart { name: name(n - 2) },
        ("uploads", _) if n >= 4 && segs[n - 3] == "blobs" => Route::Upload {
            name: name(n - 3),
            id: last,
        },
        ("blobs", _) => Route::Blob {
            name: name(n - 2),
            digest: last,
        },
        ("manifests", _) => Route::Manifest {
            name: name(n - 2),
            reference: last,
        },
        ("tags", "list") => Route::Tags { name: name(n - 2) },
        ("referrers", _) => Route::Referrers {
            name: name(n - 2),
            digest: last,
        },
        _ => return None,
    })
}

/// Checks a repository name against the distribution-spec grammar:
/// `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*`.
fn valid_name(name: &str) -> bool {
    fn component(c: &str) -> bool {
        let b = c.as_bytes();
        let alnum = |x: &u8| x.is_ascii_lowercase() || x.is_ascii_digit();
        if b.is_empty() || !alnum(&b[0]) || !alnum(&b[b.len() - 1]) {
            return false;
        }
        let mut i = 0;
        while i < b.len() {
            if alnum(&b[i]) {
                i += 1;
                continue;
            }
            let start = i;
            while i < b.len() && !alnum(&b[i]) {
                i += 1;
            }
            let sep = &c[start..i];
            let ok = sep == "." || sep == "_" || sep == "__" || sep.bytes().all(|x| x == b'-');
            if !ok {
                return false;
            }
        }
        true
    }
    name.len() <= 255 && name.split('/').all(component)
}

fn check_name(name: &str) -> Result<(), RegistryError> {
    if valid_name(name) {
        Ok(())
    } else {
        Err(RegistryError::new(
            StatusCode::BAD_REQUEST,
            "NAME_INVALID",
            format!("invalid repository name {name:?}"),
        ))
    }
}

/// The single entry point for all requests.
pub(crate) async fn handle(
    State(state): State<Arc<Inner>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let mut resp = match route(&state, &method, &uri, &headers, body).await {
        Ok(resp) => {
            if state.log_requests {
                tracing::info!(%method, %uri, status = resp.status().as_u16(), "registry request");
            }
            resp
        }
        Err(err) => {
            if state.log_requests {
                tracing::warn!(%method, %uri, status = err.status.as_u16(), code = err.code, message = %err.message, "registry request failed");
            }
            err.into_response()
        }
    };
    let h = resp.headers_mut();
    h.insert(
        "Docker-Distribution-API-Version",
        HeaderValue::from_static("registry/2.0"),
    );
    for (prob, msg) in &state.warnings {
        if *prob > random_f64()
            && let Ok(v) = HeaderValue::from_str(&format!("299 - \"{msg}\""))
        {
            h.append(header::WARNING, v);
        }
    }
    resp
}

fn random_f64() -> f64 {
    (random_u64() >> 11) as f64 / (1u64 << 53) as f64
}

async fn route(
    state: &Inner,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, RegistryError> {
    let query: HashMap<String, String> = Query::try_from_uri(uri)
        .map(|Query(q)| q)
        .unwrap_or_default();
    let route = match parse_route(uri.path()) {
        Some(Route::Referrers { .. }) if !state.referrers => None,
        r => r,
    };
    let Some(route) = route else {
        return Err(RegistryError::method_unknown());
    };
    let m = method.clone();
    match route {
        Route::Base => json_response(&serde_json::json!({}), None, &[]),
        Route::Catalog if m == Method::GET => manifests::catalog(state, &query),
        Route::Blob { name, digest } => {
            check_name(&name)?;
            match m {
                Method::GET => blob_get(state, &digest, headers, false).await,
                Method::HEAD => blob_get(state, &digest, headers, true).await,
                Method::DELETE => blob_delete(state, &digest).await,
                _ => Err(unsupported(method)),
            }
        }
        Route::UploadStart { name } if m == Method::POST => {
            check_name(&name)?;
            upload_start(state, &name, &query, headers, body).await
        }
        Route::Upload { name, id } => {
            check_name(&name)?;
            match m {
                Method::GET => upload_status(state, &name, &id).await,
                Method::PATCH => upload_patch(state, &name, &id, headers, body).await,
                Method::PUT => upload_put(state, &name, &id, &query, headers, body).await,
                Method::DELETE => upload_cancel(state, &name, &id).await,
                _ => Err(unsupported(method)),
            }
        }
        Route::Manifest { name, reference } => {
            check_name(&name)?;
            match m {
                Method::GET => manifests::get(state, &name, &reference, false),
                Method::HEAD => manifests::get(state, &name, &reference, true),
                Method::PUT => manifests::put(state, &name, &reference, headers, body).await,
                Method::DELETE => manifests::delete(state, &name, &reference),
                _ => Err(unsupported(method)),
            }
        }
        Route::Tags { name } if m == Method::GET => {
            check_name(&name)?;
            manifests::tags(state, &name, &query)
        }
        Route::Referrers { name, digest } if m == Method::GET => {
            check_name(&name)?;
            manifests::referrers(state, &name, &digest, &query)
        }
        _ => Err(unsupported(method)),
    }
}

fn unsupported(method: &Method) -> RegistryError {
    RegistryError::unsupported(format!("{method} is not supported on this endpoint"))
}

/// Builds a JSON response with an optional `Link` header; `extra` headers
/// replace defaults (e.g. a different `Content-Type`).
pub(crate) fn json_response(
    value: &serde_json::Value,
    link: Option<String>,
    extra: &[(&str, &str)],
) -> Result<Response, RegistryError> {
    let body = serde_json::to_vec(value).map_err(RegistryError::internal)?;
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, body.len());
    if let Some(link) = link {
        builder = builder.header(header::LINK, link);
    }
    let mut resp = builder
        .body(Body::from(body))
        .map_err(RegistryError::internal)?;
    for (k, v) in extra {
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(RegistryError::internal)?;
        let value = HeaderValue::from_str(v).map_err(RegistryError::internal)?;
        resp.headers_mut().insert(name, value);
    }
    Ok(resp)
}

/// Applies `last` and `n` to a sorted list, returning the page and a
/// `Link: <...>; rel="next"` value when more entries remain.
pub(crate) fn paginate(
    mut items: Vec<String>,
    query: &HashMap<String, String>,
    path: &str,
) -> Result<(Vec<String>, Option<String>), RegistryError> {
    items.sort();
    if let Some(last) = query.get("last").filter(|l| !l.is_empty()) {
        items.retain(|i| i.as_str() > last.as_str());
    }
    let mut link = None;
    if let Some(n) = query.get("n").filter(|n| !n.is_empty()) {
        let n: usize = n.parse().map_err(|_| {
            RegistryError::new(
                StatusCode::BAD_REQUEST,
                "PAGINATION_NUMBER_INVALID",
                format!("invalid n {n:?}"),
            )
        })?;
        if items.len() > n {
            items.truncate(n);
            if let Some(last) = items.last() {
                link = Some(format!("<{path}?n={n}&last={last}>; rel=\"next\""));
            }
        }
    }
    Ok((items, link))
}

fn parse_digest(s: &str) -> Result<Digest, RegistryError> {
    s.parse()
        .map_err(|e| RegistryError::digest_invalid(format!("invalid digest {s:?}: {e}")))
}

enum ByteRange {
    /// Serve the whole blob.
    Full,
    /// Half-open range.
    Part(u64, u64),
    Unsatisfiable,
}

/// Parses a single `Range: bytes=...` value. Unparseable values and
/// multi-range requests are ignored (served in full), as RFC 9110 allows.
fn parse_range(value: &str, size: u64) -> ByteRange {
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return ByteRange::Full;
    };
    if spec.contains(',') {
        return ByteRange::Full;
    }
    let Some((a, b)) = spec.split_once('-') else {
        return ByteRange::Full;
    };
    let (a, b) = (a.trim(), b.trim());
    if a.is_empty() {
        // Suffix range: the last `b` bytes.
        return match b.parse::<u64>() {
            Ok(0) => ByteRange::Unsatisfiable,
            Ok(_) if size == 0 => ByteRange::Unsatisfiable,
            Ok(n) => ByteRange::Part(size - n.min(size), size),
            Err(_) => ByteRange::Full,
        };
    }
    let Ok(start) = a.parse::<u64>() else {
        return ByteRange::Full;
    };
    let end = if b.is_empty() {
        None
    } else {
        match b.parse::<u64>() {
            Ok(e) => Some(e),
            Err(_) => return ByteRange::Full,
        }
    };
    if end.is_some_and(|e| e < start) || start >= size {
        return ByteRange::Unsatisfiable;
    }
    ByteRange::Part(start, end.map_or(size, |e| (e + 1).min(size)))
}

async fn blob_get(
    state: &Inner,
    digest: &str,
    headers: &HeaderMap,
    head: bool,
) -> Result<Response, RegistryError> {
    let d = parse_digest(digest)?;
    let Some((data, size)) = state.blobs.open(&d).await? else {
        return Err(RegistryError::blob_unknown(digest));
    };
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map_or(ByteRange::Full, |r| parse_range(r, size));
    let (status, start, end) = match range {
        ByteRange::Full => (StatusCode::OK, 0, size),
        ByteRange::Part(s, e) => (StatusCode::PARTIAL_CONTENT, s, e),
        ByteRange::Unsatisfiable => {
            let mut resp = RegistryError::new(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "BLOB_UNKNOWN",
                format!("requested range not satisfiable for {size} byte blob"),
            )
            .into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("bytes */{size}")) {
                resp.headers_mut().insert(header::CONTENT_RANGE, v);
            }
            return Ok(resp);
        }
    };
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, end - start)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT_RANGES, "bytes")
        .header("Docker-Content-Digest", d.to_string());
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end - 1, size),
        );
    }
    let body = if head {
        Body::empty()
    } else {
        match data {
            BlobData::Memory(bytes) => Body::from(bytes.slice(start as usize..end as usize)),
            BlobData::File(mut file) => {
                if start > 0 {
                    file.seek(std::io::SeekFrom::Start(start)).await?;
                }
                Body::from_stream(ReaderStream::with_capacity(
                    file.take(end - start),
                    256 * 1024,
                ))
            }
        }
    };
    builder.body(body).map_err(RegistryError::internal)
}

async fn blob_delete(state: &Inner, digest: &str) -> Result<Response, RegistryError> {
    let d = parse_digest(digest)?;
    if !state.blobs.delete(&d).await? {
        return Err(RegistryError::blob_unknown(digest));
    }
    let key = d.to_string();
    for set in state.links.lock().expect("links lock").values_mut() {
        set.remove(&key);
    }
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(header::CONTENT_LENGTH, 0)
        .body(Body::empty())
        .map_err(RegistryError::internal)
}

fn link(state: &Inner, name: &str, digest: &Digest) {
    state
        .links
        .lock()
        .expect("links lock")
        .entry(name.to_string())
        .or_default()
        .insert(digest.to_string());
}

fn blob_created(name: &str, digest: &Digest) -> Result<Response, RegistryError> {
    Response::builder()
        .status(StatusCode::CREATED)
        .header(header::LOCATION, format!("/v2/{name}/blobs/{digest}"))
        .header("Docker-Content-Digest", digest.to_string())
        .header(header::CONTENT_LENGTH, 0)
        .body(Body::empty())
        .map_err(RegistryError::internal)
}

/// Adds the headers describing an upload session's state.
fn upload_headers(headers: &mut HeaderMap, name: &str, id: &str, size: u64) {
    let pairs = [
        (header::LOCATION, format!("/v2/{name}/blobs/uploads/{id}")),
        (header::RANGE, format!("0-{}", size.saturating_sub(1))),
        (
            HeaderName::from_static("docker-upload-uuid"),
            id.to_string(),
        ),
    ];
    for (k, v) in pairs {
        if let Ok(v) = HeaderValue::from_str(&v) {
            headers.insert(k, v);
        }
    }
}

fn upload_response(
    status: StatusCode,
    name: &str,
    id: &str,
    size: u64,
) -> Result<Response, RegistryError> {
    let mut resp = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, 0)
        .body(Body::empty())
        .map_err(RegistryError::internal)?;
    upload_headers(resp.headers_mut(), name, id, size);
    Ok(resp)
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok())
}

async fn upload_start(
    state: &Inner,
    name: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, RegistryError> {
    if let Some(mount) = query.get("mount") {
        // Cross-repository mount; falls back to a new session when the
        // source does not have the blob.
        if let Ok(d) = mount.parse::<Digest>() {
            let in_source = match query.get("from").filter(|f| !f.is_empty()) {
                Some(from) => state
                    .links
                    .lock()
                    .expect("links lock")
                    .get(from)
                    .is_some_and(|set| set.contains(&d.to_string())),
                None => true,
            };
            if in_source && state.blobs.stat(&d).await?.is_some() {
                link(state, name, &d);
                return blob_created(name, &d);
            }
        }
    } else if let Some(digest) = query.get("digest") {
        // Monolithic upload in the POST body.
        let d = parse_digest(digest)?;
        let mut upload = Upload::new(&state.blobs, name, &random_id()).await?;
        let written = upload.append(body).await?;
        if let Some(len) = content_length(headers)
            && len != written
        {
            return Err(size_invalid(len, written));
        }
        upload.commit(&state.blobs, &d).await?;
        link(state, name, &d);
        return blob_created(name, &d);
    }
    let id = random_id();
    let upload = Upload::new(&state.blobs, name, &id).await?;
    state.uploads.insert(id.clone(), upload);
    upload_response(StatusCode::ACCEPTED, name, &id, 0)
}

fn size_invalid(expected: u64, got: u64) -> RegistryError {
    RegistryError::new(
        StatusCode::BAD_REQUEST,
        "SIZE_INVALID",
        format!("Content-Length {expected} does not match {got} bytes received"),
    )
}

/// Parses `Content-Range: <start>-<end>` (also accepting a `bytes ` prefix
/// and `/total` suffix).
fn parse_content_range(value: &str) -> Option<(u64, u64)> {
    let v = value.trim();
    let v = v
        .strip_prefix("bytes=")
        .or_else(|| v.strip_prefix("bytes "))
        .unwrap_or(v);
    let v = v.split('/').next()?;
    let (a, b) = v.split_once('-')?;
    let (a, b) = (a.trim().parse().ok()?, b.trim().parse().ok()?);
    (a <= b).then_some((a, b))
}

/// Checks a chunk's `Content-Range` against the session offset.
fn check_chunk(headers: &HeaderMap, name: &str, id: &str, size: u64) -> Result<(), Box<Response>> {
    let Some(value) = headers.get(header::CONTENT_RANGE) else {
        return Ok(());
    };
    let range = value.to_str().ok().and_then(parse_content_range);
    let message = match range {
        None => "invalid Content-Range".to_string(),
        Some((start, _)) if start != size => {
            format!("chunk starts at {start}, but the upload has {size} bytes")
        }
        Some((start, end)) if content_length(headers).is_some_and(|l| l != end - start + 1) => {
            "Content-Length does not match Content-Range".to_string()
        }
        Some(_) => return Ok(()),
    };
    let mut resp = RegistryError::range_invalid(message).into_response();
    upload_headers(resp.headers_mut(), name, id, size);
    Err(Box::new(resp))
}

async fn upload_patch(
    state: &Inner,
    name: &str,
    id: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, RegistryError> {
    let session = state
        .uploads
        .get(id)
        .ok_or_else(RegistryError::upload_unknown)?;
    let mut guard = session.lock().await;
    let upload = guard
        .as_mut()
        .filter(|u| u.repo == name)
        .ok_or_else(RegistryError::upload_unknown)?;
    if let Err(resp) = check_chunk(headers, name, id, upload.size) {
        return Ok(*resp);
    }
    // Without Content-Range this is a streamed chunk appended at the
    // current offset (which also lets clients resume a stream).
    upload.append(body).await?;
    upload_response(StatusCode::ACCEPTED, name, id, upload.size)
}

async fn upload_status(state: &Inner, name: &str, id: &str) -> Result<Response, RegistryError> {
    let session = state
        .uploads
        .get(id)
        .ok_or_else(RegistryError::upload_unknown)?;
    let guard = session.lock().await;
    let upload = guard
        .as_ref()
        .filter(|u| u.repo == name)
        .ok_or_else(RegistryError::upload_unknown)?;
    upload_response(StatusCode::NO_CONTENT, name, id, upload.size)
}

async fn upload_put(
    state: &Inner,
    name: &str,
    id: &str,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, RegistryError> {
    let digest = query
        .get("digest")
        .filter(|d| !d.is_empty())
        .ok_or_else(|| RegistryError::digest_invalid("digest not specified"))?;
    let d = parse_digest(digest)?;
    let session = state
        .uploads
        .get(id)
        .ok_or_else(RegistryError::upload_unknown)?;
    let mut guard = session.lock().await;
    let upload = guard
        .as_mut()
        .filter(|u| u.repo == name)
        .ok_or_else(RegistryError::upload_unknown)?;
    if let Err(resp) = check_chunk(headers, name, id, upload.size) {
        return Ok(*resp);
    }
    upload.append(body).await?;
    let upload = guard.take().expect("checked above");
    state.uploads.remove(id);
    drop(guard);
    upload.commit(&state.blobs, &d).await?;
    link(state, name, &d);
    blob_created(name, &d)
}

async fn upload_cancel(state: &Inner, name: &str, id: &str) -> Result<Response, RegistryError> {
    let session = state
        .uploads
        .get(id)
        .ok_or_else(RegistryError::upload_unknown)?;
    let mut guard = session.lock().await;
    if !guard.as_ref().is_some_and(|u| u.repo == name) {
        return Err(RegistryError::upload_unknown());
    }
    guard.take();
    state.uploads.remove(id);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(RegistryError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_nested_names() {
        assert!(matches!(
            parse_route("/v2/a/b/c/blobs/uploads/"),
            Some(Route::UploadStart { name }) if name == "a/b/c"
        ));
        assert!(matches!(
            parse_route("/v2/a/b/blobs/uploads/xyz"),
            Some(Route::Upload { name, id }) if name == "a/b" && id == "xyz"
        ));
        assert!(matches!(
            parse_route("/v2/a/b/manifests/latest"),
            Some(Route::Manifest { name, reference }) if name == "a/b" && reference == "latest"
        ));
        assert!(matches!(
            parse_route("/v2/x/tags/list"),
            Some(Route::Tags { name }) if name == "x"
        ));
        assert!(matches!(parse_route("/v2/_catalog"), Some(Route::Catalog)));
        assert!(matches!(parse_route("/v2/"), Some(Route::Base)));
        assert!(parse_route("/v2/foo").is_none());
        assert!(parse_route("/v3/").is_none());
    }

    #[test]
    fn name_grammar() {
        for ok in [
            "a",
            "a/b",
            "foo.bar/b__c",
            "a-b--c/d_e",
            "conformance/repo1",
        ] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in ["", "A", "a/", "-a", "a..b", "a___b", "a/.b", "a_-b"] {
            assert!(!valid_name(bad), "{bad}");
        }
    }

    #[test]
    fn byte_ranges() {
        let r = |v| match parse_range(v, 100) {
            ByteRange::Full => "full".to_string(),
            ByteRange::Part(a, b) => format!("{a}..{b}"),
            ByteRange::Unsatisfiable => "416".to_string(),
        };
        assert_eq!(r("bytes=0-9"), "0..10");
        assert_eq!(r("bytes=90-"), "90..100");
        assert_eq!(r("bytes=-10"), "90..100");
        assert_eq!(r("bytes=50-500"), "50..100");
        assert_eq!(r("bytes=50-10"), "416");
        assert_eq!(r("bytes=100-200"), "416");
        assert_eq!(r("bytes=0-1,5-6"), "full");
        assert_eq!(r("items=1-2"), "full");
    }

    #[test]
    fn content_ranges() {
        assert_eq!(parse_content_range("0-9"), Some((0, 9)));
        assert_eq!(parse_content_range("bytes 10-19/100"), Some((10, 19)));
        assert_eq!(parse_content_range("9-0"), None);
        assert_eq!(parse_content_range("x"), None);
    }
}
