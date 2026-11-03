//! Additional client tests for edge cases and protocol compliance.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use containerregistry_image::{Digest, ImageIndex, Manifest};
use containerregistry_registry::{Client, ClientConfig, Error, Reference};

fn serve_responses(responses: Vec<String>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let addr = listener.local_addr().expect("server addr");
    let handle = thread::spawn(move || {
        for response in responses {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(response.as_bytes());
            }
        }
    });
    (format!("{}:{}", addr.ip(), addr.port()), handle)
}

fn http_response(status: &str, headers: Vec<(&str, String)>, body: &str) -> String {
    let mut resp = format!("HTTP/1.1 {}\r\n", status);
    resp.push_str("Connection: close\r\n");
    for (k, v) in headers {
        resp.push_str(&format!("{}: {}\r\n", k, v));
    }
    resp.push_str("\r\n");
    resp.push_str(body);
    resp
}

fn minimal_oci_manifest_json() -> &'static str {
    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},"layers":[]}"#
}

fn minimal_oci_index_json() -> &'static str {
    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#
}

fn minimal_manifest() -> Manifest {
    Manifest::from_bytes(minimal_oci_manifest_json().as_bytes()).expect("manifest parse")
}

fn minimal_index() -> ImageIndex {
    ImageIndex::from_bytes(minimal_oci_index_json().as_bytes()).expect("index parse")
}

// This test ensures Content-Type parameters (e.g., charset) are ignored when
// determining the manifest media type.
#[tokio::test]
async fn test_head_manifest_content_type_with_parameters() {
    let digest: Digest = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        .parse()
        .unwrap();
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json; charset=utf-8".into(),
            ),
            ("Docker-Content-Digest", digest.to_string()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let (media_type, parsed_digest, _size) = client.head_manifest(&reference).await.unwrap();
    assert_eq!(
        media_type.as_str(),
        "application/vnd.oci.image.manifest.v1+json"
    );
    assert_eq!(parsed_digest, digest);

    let _ = handle.join();
}

// This test ensures registries returning tags=null (empty repository) are handled
// gracefully by returning an empty list instead of failing to parse JSON.
#[tokio::test]
async fn test_list_tags_allows_null() {
    let body = r#"{"name":"repo","tags":null}"#;
    let response = http_response(
        "200 OK",
        vec![
            ("Content-Type", "application/json".into()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let tags = client.list_tags(&reference).await.unwrap();
    assert!(tags.is_empty());

    let _ = handle.join();
}

// This test ensures list_tags follows Link pagination and accumulates tags.
#[tokio::test]
async fn test_list_tags_pagination() {
    let body1 = r#"{"name":"repo","tags":["v1","v2"]}"#;
    let body2 = r#"{"name":"repo","tags":["v3"]}"#;
    let response1 = http_response(
        "200 OK",
        vec![
            ("Content-Type", "application/json".into()),
            ("Content-Length", body1.len().to_string()),
            (
                "Link",
                "</v2/repo/tags/list?n=2&last=v2>; rel=\"next\"".into(),
            ),
            ("Connection", "close".into()),
        ],
        body1,
    );
    let response2 = http_response(
        "200 OK",
        vec![
            ("Content-Type", "application/json".into()),
            ("Content-Length", body2.len().to_string()),
            ("Connection", "close".into()),
        ],
        body2,
    );

    let (addr, handle) = serve_responses(vec![response1, response2]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let tags = client.list_tags(&reference).await.unwrap();
    assert_eq!(
        tags,
        vec!["v1".to_string(), "v2".to_string(), "v3".to_string()]
    );

    let _ = handle.join();
}

// This test ensures 429 responses are mapped to RateLimited with Retry-After.
#[tokio::test]
async fn test_get_manifest_rate_limited_retry_after() {
    let response = http_response(
        "429 Too Many Requests",
        vec![
            ("Retry-After", "5".into()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client =
        Client::with_config(ClientConfig::new().with_https(false).with_retries(0)).unwrap();

    let err = client.get_manifest(&reference).await.unwrap_err();
    match err {
        Error::RateLimited { retry_after, .. } => {
            assert_eq!(retry_after, Some(Duration::from_secs(5)));
        }
        other => panic!("expected RateLimited, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures PUT manifest maps 429 responses to RateLimited.
#[tokio::test]
async fn test_put_manifest_rate_limited_retry_after() {
    let response = http_response(
        "429 Too Many Requests",
        vec![
            ("Retry-After", "2".into()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    // PUTs are retried on 429 like other requests; disable retries to
    // observe the mapping of the final response.
    let client =
        Client::with_config(ClientConfig::new().with_https(false).with_retries(0)).unwrap();
    let manifest = minimal_manifest();

    let err = client
        .put_manifest(&reference, &manifest)
        .await
        .unwrap_err();
    match err {
        Error::RateLimited { retry_after, .. } => {
            assert_eq!(retry_after, Some(Duration::from_secs(2)));
        }
        other => panic!("expected RateLimited, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures digest-based references are verified against the returned
// manifest digest and mismatches are rejected.
#[tokio::test]
async fn test_get_manifest_digest_reference_mismatch() {
    let body = minimal_oci_manifest_json();
    let actual_digest = Digest::sha256(body.as_bytes());
    let expected_digest: Digest =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
            .parse()
            .unwrap();

    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", actual_digest.to_string()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo@{}", addr, expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client.get_manifest(&reference).await.unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures malformed docker-content-digest headers are rejected for GET.
#[tokio::test]
async fn test_get_manifest_rejects_malformed_digest_header() {
    let body = minimal_oci_manifest_json();
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", "notadigest".into()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client.get_manifest(&reference).await.unwrap_err();
    match err {
        Error::UnexpectedStatus { .. } => {}
        other => panic!("expected UnexpectedStatus, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures malformed docker-content-digest headers are rejected for HEAD.
#[tokio::test]
async fn test_head_manifest_rejects_malformed_digest_header() {
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", "sha256:invalid".into()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client.head_manifest(&reference).await.unwrap_err();
    match err {
        Error::UnexpectedStatus { .. } => {}
        other => panic!("expected UnexpectedStatus, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures digest headers are parsed leniently (case-insensitive).
#[tokio::test]
async fn test_get_manifest_accepts_uppercase_digest_header() {
    let body = minimal_oci_manifest_json();
    let digest = Digest::sha256(body.as_bytes());
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", digest.to_string().to_uppercase()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let (_manifest_or_index, parsed_digest) = client.get_manifest(&reference).await.unwrap();
    assert_eq!(parsed_digest, digest);

    let _ = handle.join();
}

// This test ensures digest-form references using sha384 are accepted even when
// the registry returns a sha256 docker-content-digest header.
#[tokio::test]
async fn test_get_manifest_accepts_sha384_digest_reference() {
    let body = minimal_oci_manifest_json();
    let header_digest = Digest::sha256(body.as_bytes());
    let expected_digest = Digest::sha384(body.as_bytes());
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", header_digest.to_string()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo@{}", addr, expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let (_manifest_or_index, parsed_digest) = client.get_manifest(&reference).await.unwrap();
    assert_eq!(parsed_digest, header_digest);

    let _ = handle.join();
}

// This test ensures digest-form references using sha512 are accepted even when
// the registry returns a sha256 docker-content-digest header.
#[tokio::test]
async fn test_get_manifest_accepts_sha512_digest_reference() {
    let body = minimal_oci_manifest_json();
    let header_digest = Digest::sha256(body.as_bytes());
    let expected_digest = Digest::sha512(body.as_bytes());
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", header_digest.to_string()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo@{}", addr, expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let (_manifest_or_index, parsed_digest) = client.get_manifest(&reference).await.unwrap();
    assert_eq!(parsed_digest, header_digest);

    let _ = handle.join();
}

// This test ensures digest header mismatches are rejected for GET.
#[tokio::test]
async fn test_get_manifest_rejects_mismatched_digest_header() {
    let body = minimal_oci_manifest_json();
    let bad_digest = Digest::sha256(b"not-the-body");
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", bad_digest.to_string()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client.get_manifest(&reference).await.unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures get_blob accepts sha384 digests when the payload matches.
#[tokio::test]
async fn test_get_blob_sha384_digest_ok() {
    let body = b"sha384-blob";
    let digest = Digest::sha384(body);
    let response = http_response(
        "200 OK",
        vec![
            ("Content-Type", "application/octet-stream".into()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        std::str::from_utf8(body).unwrap(),
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let data = client.get_blob(&reference, &digest).await.unwrap();
    assert_eq!(data, body);

    let _ = handle.join();
}

// This test ensures get_blob accepts sha512 digests when the payload matches.
#[tokio::test]
async fn test_get_blob_sha512_digest_ok() {
    let body = b"sha512-blob";
    let digest = Digest::sha512(body);
    let response = http_response(
        "200 OK",
        vec![
            ("Content-Type", "application/octet-stream".into()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        std::str::from_utf8(body).unwrap(),
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let data = client.get_blob(&reference, &digest).await.unwrap();
    assert_eq!(data, body);

    let _ = handle.join();
}

// This test ensures get_blob rejects sha512 digests when the payload mismatches.
#[tokio::test]
async fn test_get_blob_sha512_digest_mismatch() {
    let body = b"sha512-blob";
    let digest = Digest::sha512(b"wrong");
    let response = http_response(
        "200 OK",
        vec![
            ("Content-Type", "application/octet-stream".into()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        std::str::from_utf8(body).unwrap(),
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client.get_blob(&reference, &digest).await.unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures PUT manifest rejects digest header mismatches.
#[tokio::test]
async fn test_put_manifest_rejects_mismatched_digest_header() {
    let bad_digest = Digest::sha256(b"not-the-manifest");
    let response = http_response(
        "201 Created",
        vec![
            ("Docker-Content-Digest", bad_digest.to_string()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();
    let manifest = minimal_manifest();

    let err = client
        .put_manifest(&reference, &manifest)
        .await
        .unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures PUT index rejects digest header mismatches.
#[tokio::test]
async fn test_put_index_rejects_mismatched_digest_header() {
    let bad_digest = Digest::sha256(b"not-the-index");
    let response = http_response(
        "201 Created",
        vec![
            ("Docker-Content-Digest", bad_digest.to_string()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();
    let index = minimal_index();

    let err = client.put_index(&reference, &index).await.unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }

    let _ = handle.join();
}

// This test ensures PUT manifest accepts sha384 digest references that match the body.
#[tokio::test]
async fn test_put_manifest_accepts_sha384_digest_reference() {
    let manifest = minimal_manifest();
    let manifest_bytes = manifest.to_bytes().unwrap();
    let header_digest = Digest::sha256(&manifest_bytes);
    let expected_digest = Digest::sha384(&manifest_bytes);

    let response = http_response(
        "201 Created",
        vec![
            ("Docker-Content-Digest", header_digest.to_string()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo@{}", addr, expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let returned = client.put_manifest(&reference, &manifest).await.unwrap();
    assert_eq!(returned, header_digest);

    let _ = handle.join();
}

// This test ensures PUT index accepts sha512 digest references that match the body.
#[tokio::test]
async fn test_put_index_accepts_sha512_digest_reference() {
    let index = minimal_index();
    let index_bytes = index.to_bytes().unwrap();
    let header_digest = Digest::sha256(&index_bytes);
    let expected_digest = Digest::sha512(&index_bytes);

    let response = http_response(
        "201 Created",
        vec![
            ("Docker-Content-Digest", header_digest.to_string()),
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo@{}", addr, expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let returned = client.put_index(&reference, &index).await.unwrap();
    assert_eq!(returned, header_digest);

    let _ = handle.join();
}

// This test ensures digest reference validation happens before any network call.
#[tokio::test]
async fn test_put_manifest_rejects_sha384_digest_reference_mismatch() {
    let manifest = minimal_manifest();
    let expected_digest = Digest::sha384(b"wrong-body");
    let reference: Reference = format!("127.0.0.1:1/repo@{}", expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client
        .put_manifest(&reference, &manifest)
        .await
        .unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }
}

// This test ensures digest reference validation happens before any network call.
#[tokio::test]
async fn test_put_index_rejects_sha512_digest_reference_mismatch() {
    let index = minimal_index();
    let expected_digest = Digest::sha512(b"wrong-body");
    let reference: Reference = format!("127.0.0.1:1/repo@{}", expected_digest)
        .parse()
        .unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let err = client.put_index(&reference, &index).await.unwrap_err();
    match err {
        Error::DigestMismatch { .. } => {}
        other => panic!("expected DigestMismatch, got {:?}", other),
    }
}

// This test ensures multi-arch tags that return an index are parsed as ImageIndex
// instead of failing when the registry responds with an OCI index payload.
#[tokio::test]
async fn test_get_manifest_returns_index() {
    let body = minimal_oci_index_json();
    let digest = Digest::sha256(body.as_bytes());
    let response = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.index.v1+json".into(),
            ),
            ("Docker-Content-Digest", digest.to_string()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![response]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();

    let (manifest_or_index, parsed_digest) = client.get_manifest(&reference).await.unwrap();
    assert!(manifest_or_index.is_index());
    assert_eq!(parsed_digest, digest);

    let _ = handle.join();
}

// This test ensures transient 5xx responses are retried and succeed when the
// subsequent attempt returns a valid manifest.
#[tokio::test]
async fn test_retry_on_server_error() {
    let body = minimal_oci_manifest_json();
    let digest = Digest::sha256(body.as_bytes());

    let first = http_response(
        "500 Internal Server Error",
        vec![
            ("Content-Length", "0".into()),
            ("Connection", "close".into()),
        ],
        "",
    );
    let second = http_response(
        "200 OK",
        vec![
            (
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".into(),
            ),
            ("Docker-Content-Digest", digest.to_string()),
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".into()),
        ],
        body,
    );

    let (addr, handle) = serve_responses(vec![first, second]);
    let reference: Reference = format!("{}/repo:tag", addr).parse().unwrap();
    let client =
        Client::with_config(ClientConfig::new().with_https(false).with_retries(1)).unwrap();

    let (manifest_or_index, parsed_digest) = client.get_manifest(&reference).await.unwrap();
    assert!(manifest_or_index.is_manifest());
    assert_eq!(parsed_digest, digest);

    let _ = handle.join();
}
