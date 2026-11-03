//! Drives the registry with the workspace's own client
//! (`containerregistry_registry::Client`) and with raw HTTP for exact
//! status codes and headers.

use std::net::SocketAddr;

use bytes::Bytes;
use containerregistry_image::{Digest, MediaType};
use containerregistry_registry::{
    Client, ClientConfig, DownloadOptions, Error, ListOptions, MountResult, Reference,
    UploadOptions, UploadStatus,
};
use containerregistry_server::{Registry, TestRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";

fn client() -> Client {
    Client::with_config(
        ClientConfig::new()
            .with_https(false)
            .with_retries(0)
            .with_retry_backoff(std::time::Duration::from_millis(1)),
    )
    .unwrap()
}

/// Deterministic pseudo-random bytes.
fn data(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// An in-memory and a disk-backed registry (with its directory).
async fn registries() -> Vec<(TestRegistry, Option<tempfile::TempDir>)> {
    let dir = tempfile::tempdir().unwrap();
    let disk = Registry::builder()
        .disk(dir.path().join("blobs"))
        .build()
        .unwrap();
    vec![
        (TestRegistry::spawn().await.unwrap(), None),
        (TestRegistry::spawn_with(disk).await.unwrap(), Some(dir)),
    ]
}

fn tag_ref(reg: &TestRegistry, repo: &str, tag: &str) -> Reference {
    Reference::with_tag(reg.host(), repo, tag)
}

fn digest_ref(reg: &TestRegistry, repo: &str, digest: &Digest) -> Reference {
    Reference::with_digest(reg.host(), repo, digest.clone())
}

struct RawResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RawResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The first OCI error code in the body.
    fn code(&self) -> String {
        let v: serde_json::Value = serde_json::from_slice(&self.body).unwrap_or_default();
        v["errors"][0]["code"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }
}

/// A minimal HTTP/1.1 request (the server always sends Content-Length).
async fn raw(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> RawResponse {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8(buf[..split].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    RawResponse {
        status,
        headers,
        body: buf[split + 4..].to_vec(),
    }
}

/// Pushes an image manifest referencing an empty config; returns its bytes
/// and digest.
async fn push_manifest(
    client: &Client,
    reference: &Reference,
    extra: serde_json::Value,
) -> (Vec<u8>, Digest) {
    let config = client.put_blob(reference, b"{}").await.unwrap();
    let mut manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST,
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config, "size": 2},
        "layers": [],
    });
    for (k, v) in extra.as_object().unwrap() {
        manifest[k] = v.clone();
    }
    let bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    let resp = client
        .put_manifest_raw(reference, &bytes, &OCI_MANIFEST.parse().unwrap())
        .await
        .unwrap();
    (bytes, resp.digest)
}

#[tokio::test]
async fn ping_and_api_version_header() {
    let reg = TestRegistry::spawn().await.unwrap();
    client().ping(&reg.host()).await.unwrap();
    let resp = raw(reg.addr(), "GET", "/v2/", &[], b"").await;
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.header("Docker-Distribution-API-Version"),
        Some("registry/2.0")
    );
    let resp = raw(reg.addr(), "GET", "/v2/nope", &[], b"").await;
    assert_eq!((resp.status, resp.code().as_str()), (404, "METHOD_UNKNOWN"));
}

#[tokio::test]
async fn blob_roundtrip_and_ranges() {
    let client = client();
    for (reg, _dir) in registries().await {
        let r = tag_ref(&reg, "a/b/c", "latest");
        let blob = data(100_000, 1);
        let digest = client.put_blob(&r, &blob).await.unwrap();
        assert_eq!(client.head_blob(&r, &digest).await.unwrap(), 100_000);
        assert_eq!(client.get_blob(&r, &digest).await.unwrap(), blob);

        let mut part = Vec::new();
        client
            .get_blob_range(&r, &digest, 10..20)
            .await
            .unwrap()
            .read_to_end(&mut part)
            .await
            .unwrap();
        assert_eq!(part, &blob[10..20]);
        part.clear();
        client
            .get_blob_range(&r, &digest, 99_990..)
            .await
            .unwrap()
            .read_to_end(&mut part)
            .await
            .unwrap();
        assert_eq!(part, &blob[99_990..]);

        let path = format!("/v2/a/b/c/blobs/{digest}");
        let head = raw(reg.addr(), "HEAD", &path, &[], b"").await;
        assert_eq!(head.status, 200);
        assert_eq!(head.header("Content-Length"), Some("100000"));
        assert_eq!(head.header("Accept-Ranges"), Some("bytes"));
        assert_eq!(
            head.header("Content-Type"),
            Some("application/octet-stream")
        );
        assert_eq!(
            head.header("Docker-Content-Digest"),
            Some(digest.to_string().as_str())
        );
        assert!(head.body.is_empty());

        let ranged = raw(reg.addr(), "GET", &path, &[("Range", "bytes=5-9")], b"").await;
        assert_eq!(ranged.status, 206);
        assert_eq!(ranged.header("Content-Range"), Some("bytes 5-9/100000"));
        assert_eq!(ranged.body, &blob[5..10]);
        let open = raw(reg.addr(), "GET", &path, &[("Range", "bytes=99000-")], b"").await;
        assert_eq!(open.status, 206);
        assert_eq!(open.body, &blob[99_000..]);
        let suffix = raw(reg.addr(), "GET", &path, &[("Range", "bytes=-3")], b"").await;
        assert_eq!(suffix.body, &blob[99_997..]);
        for bad in ["bytes=100000-", "bytes=9-5"] {
            let resp = raw(reg.addr(), "GET", &path, &[("Range", bad)], b"").await;
            assert_eq!(resp.status, 416, "{bad}");
            assert_eq!(resp.header("Content-Range"), Some("bytes */100000"));
        }

        client.delete_blob(&r, &digest).await.unwrap();
        assert!(matches!(
            client.head_blob(&r, &digest).await,
            Err(Error::NotFound(_))
        ));
        let missing = raw(reg.addr(), "GET", &path, &[], b"").await;
        assert_eq!(
            (missing.status, missing.code().as_str()),
            (404, "BLOB_UNKNOWN")
        );
        let missing = raw(reg.addr(), "DELETE", &path, &[], b"").await;
        assert_eq!(
            (missing.status, missing.code().as_str()),
            (404, "BLOB_UNKNOWN")
        );
    }
}

#[tokio::test]
async fn chunked_and_streamed_uploads() {
    let client = client();
    for (reg, dir) in registries().await {
        let r = tag_ref(&reg, "uploads", "latest");
        let blob = data(3 * 1024 * 1024 + 17, 2);
        let digest = Digest::sha256(&blob);

        // Chunked PATCHes with Content-Range.
        let pushed = client
            .push_blob_stream(
                &r,
                std::io::Cursor::new(blob.clone()),
                UploadOptions::default().with_chunk_size(1024 * 1024),
            )
            .await
            .unwrap();
        assert_eq!(pushed.digest, digest);
        assert_eq!(pushed.status, UploadStatus::Uploaded);
        assert_eq!(client.get_blob(&r, &digest).await.unwrap(), blob);
        client.delete_blob(&r, &digest).await.unwrap();

        // One streamed PATCH without Content-Range.
        let pushed = client
            .push_blob_stream(
                &r,
                std::io::Cursor::new(blob.clone()),
                UploadOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            (pushed.digest.clone(), pushed.size),
            (digest.clone(), blob.len() as u64)
        );
        let mut streamed = Vec::new();
        client
            .get_blob_stream(&r, &digest)
            .await
            .unwrap()
            .read_to_end(&mut streamed)
            .await
            .unwrap();
        assert_eq!(streamed, blob);

        // Already present: nothing uploaded.
        let again = client
            .push_blob_stream(
                &r,
                std::io::Cursor::new(blob.clone()),
                UploadOptions::default().with_digest(digest.clone()),
            )
            .await
            .unwrap();
        assert_eq!(again.status, UploadStatus::AlreadyExists);

        // sha512 content.
        let sha512 = Digest::sha512(&blob[..1000]);
        let upload = client.start_blob_upload(&r).await.unwrap();
        upload
            .finish_with(&sha512, Bytes::copy_from_slice(&blob[..1000]))
            .await
            .unwrap();
        assert_eq!(client.get_blob(&r, &sha512).await.unwrap(), &blob[..1000]);

        if let Some(dir) = dir {
            let blobs = dir.path().join("blobs");
            assert!(blobs.join("sha256").join(digest.hex()).is_file());
            assert!(blobs.join("sha512").join(sha512.hex()).is_file());
            let leftovers: Vec<_> = std::fs::read_dir(&blobs)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("upload-"))
                .collect();
            assert!(leftovers.is_empty(), "temp files left: {leftovers:?}");
        }
    }
}

#[tokio::test]
async fn upload_session_status_resume_and_cancel() {
    let client = client();
    for (reg, dir) in registries().await {
        let r = tag_ref(&reg, "resume/me", "latest");
        let blob = data(2_500_000, 3);
        let digest = Digest::sha256(&blob);

        let mut upload = client.start_blob_upload(&r).await.unwrap();
        assert_eq!(upload.status().await.unwrap(), 0);
        upload
            .write_chunk(Bytes::copy_from_slice(&blob[..1_000_000]))
            .await
            .unwrap();
        assert_eq!(upload.offset(), 1_000_000);
        let location = upload.location().to_string();

        // Status from a fresh session handle.
        let mut again = client.resume_blob_upload(&r, &location).await.unwrap();
        assert_eq!(again.offset(), 1_000_000);
        assert_eq!(again.status().await.unwrap(), 1_000_000);

        // A "new process" resumes from the saved location, both chunked and
        // streamed.
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), &blob).unwrap();
        let done = client
            .push_blob_file(
                &r,
                file.path(),
                UploadOptions::default()
                    .with_resume_location(location)
                    .with_chunk_size(700_000),
            )
            .await
            .unwrap();
        assert_eq!(done.digest, digest);
        assert_eq!(client.get_blob(&r, &digest).await.unwrap(), blob);
        client.delete_blob(&r, &digest).await.unwrap();

        let mut upload = client.start_blob_upload(&r).await.unwrap();
        upload
            .write_chunk(Bytes::copy_from_slice(&blob[..123_456]))
            .await
            .unwrap();
        let done = client
            .push_blob_file(
                &r,
                file.path(),
                UploadOptions::default().with_resume_location(upload.location()),
            )
            .await
            .unwrap();
        assert_eq!(done.digest, digest);
        assert_eq!(client.get_blob(&r, &digest).await.unwrap(), blob);

        // Cancel removes the session (and its temp file).
        let mut upload = client.start_blob_upload(&r).await.unwrap();
        upload
            .write_chunk(Bytes::from_static(b"abc"))
            .await
            .unwrap();
        let location = upload.location().to_string();
        upload.cancel().await.unwrap();
        assert!(matches!(
            client.resume_blob_upload(&r, &location).await,
            Err(Error::NotFound(_))
        ));
        if let Some(dir) = dir {
            let leftovers = std::fs::read_dir(dir.path().join("blobs"))
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("upload-"))
                .count();
            assert_eq!(leftovers, 0);
        }
    }
}

#[tokio::test]
async fn upload_protocol_errors() {
    let client = client();
    let reg = TestRegistry::spawn().await.unwrap();
    let r = tag_ref(&reg, "errs", "latest");
    let addr = reg.addr();

    let post = raw(addr, "POST", "/v2/errs/blobs/uploads/", &[], b"").await;
    assert_eq!(post.status, 202);
    assert_eq!(post.header("Range"), Some("0-0"));
    assert!(post.header("Docker-Upload-UUID").is_some());
    let loc = post.header("Location").unwrap().to_string();

    let first = raw(addr, "PATCH", &loc, &[("Content-Range", "0-4")], b"hello").await;
    assert_eq!(first.status, 202);
    assert_eq!(first.header("Range"), Some("0-4"));

    // Out of order chunks are rejected and report the committed range.
    let bad = raw(addr, "PATCH", &loc, &[("Content-Range", "10-14")], b"world").await;
    assert_eq!(bad.status, 416);
    assert_eq!(bad.header("Range"), Some("0-4"));
    let status = raw(addr, "GET", &loc, &[], b"").await;
    assert_eq!(status.status, 204);
    assert_eq!(status.header("Range"), Some("0-4"));
    assert_eq!(status.header("Location"), Some(loc.as_str()));

    // PUT without/with a wrong digest.
    let put = raw(addr, "PUT", &loc, &[], b"").await;
    assert_eq!((put.status, put.code().as_str()), (400, "DIGEST_INVALID"));
    let wrong = Digest::sha256(b"something else");
    let put = raw(
        addr,
        "PUT",
        &format!("{loc}?digest={wrong}"),
        &[],
        b" world",
    )
    .await;
    assert_eq!((put.status, put.code().as_str()), (400, "DIGEST_INVALID"));

    // Unknown sessions.
    let gone = raw(addr, "PATCH", &loc, &[], b"x").await;
    assert_eq!(
        (gone.status, gone.code().as_str()),
        (404, "BLOB_UPLOAD_UNKNOWN")
    );
    let gone = raw(addr, "DELETE", "/v2/errs/blobs/uploads/nope", &[], b"").await;
    assert_eq!(
        (gone.status, gone.code().as_str()),
        (404, "BLOB_UPLOAD_UNKNOWN")
    );

    // A session is bound to its repository.
    let post = raw(addr, "POST", "/v2/errs/blobs/uploads/", &[], b"").await;
    let loc = post.header("Location").unwrap();
    let other = loc.replace("/v2/errs/", "/v2/other/");
    let resp = raw(addr, "GET", &other, &[], b"").await;
    assert_eq!(resp.code(), "BLOB_UPLOAD_UNKNOWN");

    // Monolithic POST with a body, good and bad.
    let body = b"monolithic";
    let good = Digest::sha256(body);
    let resp = raw(
        addr,
        "POST",
        &format!("/v2/errs/blobs/uploads/?digest={good}"),
        &[],
        body,
    )
    .await;
    assert_eq!(resp.status, 201);
    assert_eq!(
        resp.header("Location"),
        Some(format!("/v2/errs/blobs/{good}").as_str())
    );
    let resp = raw(
        addr,
        "POST",
        &format!("/v2/errs/blobs/uploads/?digest={wrong}"),
        &[],
        body,
    )
    .await;
    assert_eq!((resp.status, resp.code().as_str()), (400, "DIGEST_INVALID"));

    // Client-level digest mismatch on finish.
    let upload = client.start_blob_upload(&r).await.unwrap();
    let err = upload
        .finish_with(&wrong, Bytes::from_static(b"data"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("DIGEST_INVALID"), "{err}");

    // Invalid names and digests, unsupported methods.
    let resp = raw(addr, "GET", "/v2/UPPER/tags/list", &[], b"").await;
    assert_eq!((resp.status, resp.code().as_str()), (400, "NAME_INVALID"));
    let resp = raw(addr, "GET", "/v2/errs/blobs/sha256:nothex", &[], b"").await;
    assert_eq!((resp.status, resp.code().as_str()), (400, "DIGEST_INVALID"));
    let resp = raw(addr, "DELETE", "/v2/_catalog", &[], b"").await;
    assert_eq!((resp.status, resp.code().as_str()), (405, "UNSUPPORTED"));
}

#[tokio::test]
async fn parallel_download_and_concurrent_pushes() {
    let client = client();
    for (reg, _dir) in registries().await {
        let r = tag_ref(&reg, "big", "latest");
        let blob = data(9 * 1024 * 1024 + 3, 4);
        let pushed = client
            .push_blob_stream(
                &r,
                std::io::Cursor::new(blob.clone()),
                UploadOptions::default(),
            )
            .await
            .unwrap();
        let out = tempfile::tempdir().unwrap();
        let dest = out.path().join("blob");
        let n = client
            .download_blob(
                &r,
                &pushed.digest,
                &dest,
                DownloadOptions::default()
                    .with_parallelism(4)
                    .with_part_size(1024 * 1024),
            )
            .await
            .unwrap();
        assert_eq!(n, blob.len() as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), blob);

        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..16u64 {
            let client = client.clone();
            let r = tag_ref(&reg, &format!("concurrent/{}", i % 4), "latest");
            tasks.spawn(async move {
                let blob = data(200_000 + i as usize, 100 + i);
                let opts = if i % 2 == 0 {
                    UploadOptions::default().with_chunk_size(64 * 1024)
                } else {
                    UploadOptions::default()
                };
                let pushed = client
                    .push_blob_stream(&r, std::io::Cursor::new(blob.clone()), opts)
                    .await
                    .unwrap();
                assert_eq!(client.get_blob(&r, &pushed.digest).await.unwrap(), blob);
            });
        }
        while let Some(res) = tasks.join_next().await {
            res.unwrap();
        }
    }
}

#[tokio::test]
async fn cross_repository_mount() {
    let client = client();
    for (reg, _dir) in registries().await {
        let src = tag_ref(&reg, "mount/src", "latest");
        let dst = tag_ref(&reg, "mount/dst", "latest");
        let blob = data(10_000, 5);
        let digest = client.put_blob(&src, &blob).await.unwrap();

        assert!(client.mount_blob(&dst, "mount/src", &digest).await.unwrap());
        assert_eq!(client.get_blob(&dst, &digest).await.unwrap(), blob);

        // The source doesn't have it: the registry opens an upload instead.
        let missing = Digest::sha256(b"never pushed");
        match client
            .mount_or_start_upload(&dst, "mount/src", &missing)
            .await
            .unwrap()
        {
            MountResult::Upload(upload) => upload
                .finish_with(&missing, Bytes::from_static(b"never pushed"))
                .await
                .map(|_| ())
                .unwrap(),
            MountResult::Mounted => panic!("mounted a missing blob"),
        }
        // Present in the registry but not in the named source repository.
        assert!(
            !client
                .mount_blob(&dst, "mount/other", &digest)
                .await
                .unwrap()
        );

        let pushed = client
            .push_blob_stream(
                &tag_ref(&reg, "mount/third", "latest"),
                std::io::Cursor::new(blob.clone()),
                UploadOptions::default()
                    .with_digest(digest.clone())
                    .with_mount_from("mount/dst"),
            )
            .await
            .unwrap();
        // The blob is shared across repositories, so the existence check
        // short-circuits before the mount, as with go-containerregistry.
        assert!(matches!(
            pushed.status,
            UploadStatus::AlreadyExists | UploadStatus::Mounted
        ));
    }
}

#[tokio::test]
async fn manifests_by_tag_and_digest() {
    let client = client();
    for (reg, _dir) in registries().await {
        let r = tag_ref(&reg, "app/web", "v1");
        let (bytes, digest) = push_manifest(&client, &r, serde_json::json!({})).await;

        let got = client.get_manifest_raw(&r).await.unwrap();
        assert_eq!(got.data, bytes);
        assert_eq!(got.media_type, MediaType::OciManifest);
        assert_eq!(got.digest, digest);
        let by_digest = digest_ref(&reg, "app/web", &digest);
        assert_eq!(
            client.get_manifest_raw(&by_digest).await.unwrap().data,
            bytes
        );
        let (mt, d, size) = client.head_manifest(&r).await.unwrap();
        assert_eq!(
            (mt, d, size),
            (MediaType::OciManifest, digest.clone(), bytes.len() as u64)
        );

        let path = format!("/v2/app/web/manifests/{digest}");
        let get = raw(reg.addr(), "GET", &path, &[], b"").await;
        assert_eq!(get.header("Content-Type"), Some(OCI_MANIFEST));
        assert_eq!(
            get.header("Docker-Content-Digest"),
            Some(digest.to_string().as_str())
        );

        // Pushing by a digest that does not match the body.
        let other = Digest::sha256(b"x");
        let put = raw(
            reg.addr(),
            "PUT",
            &format!("/v2/app/web/manifests/{other}"),
            &[("Content-Type", OCI_MANIFEST)],
            &bytes,
        )
        .await;
        assert_eq!((put.status, put.code().as_str()), (400, "DIGEST_INVALID"));

        // An index whose child manifest is missing is rejected (like go).
        let index = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": [{"mediaType": OCI_MANIFEST, "digest": other, "size": 1}],
        });
        let err = client
            .put_manifest_raw(
                &tag_ref(&reg, "app/web", "idx"),
                &serde_json::to_vec(&index).unwrap(),
                &MediaType::OciIndex,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
        let index = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": [{"mediaType": OCI_MANIFEST, "digest": digest, "size": bytes.len()}],
        });
        client
            .put_manifest_raw(
                &tag_ref(&reg, "app/web", "idx"),
                &serde_json::to_vec(&index).unwrap(),
                &MediaType::OciIndex,
            )
            .await
            .unwrap();

        // Errors for unknown manifests and repositories.
        let resp = raw(reg.addr(), "GET", "/v2/app/web/manifests/nope", &[], b"").await;
        assert_eq!(
            (resp.status, resp.code().as_str()),
            (404, "MANIFEST_UNKNOWN")
        );
        let resp = raw(reg.addr(), "GET", "/v2/app/none/manifests/v1", &[], b"").await;
        assert_eq!((resp.status, resp.code().as_str()), (404, "NAME_UNKNOWN"));

        // Delete the tag, then the manifest by digest (and its other tags).
        client.tag(&r, "v2").await.unwrap();
        client.delete_manifest(&r).await.unwrap();
        assert!(matches!(
            client.head_manifest(&r).await,
            Err(Error::NotFound(_))
        ));
        client
            .head_manifest(&tag_ref(&reg, "app/web", "v2"))
            .await
            .unwrap();
        client.delete_manifest(&by_digest).await.unwrap();
        assert!(matches!(
            client.get_manifest_raw(&by_digest).await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(client.list_tags(&r).await.unwrap(), vec!["idx".to_string()]);
    }
}

#[tokio::test]
async fn tag_and_catalog_pagination() {
    let client = client();
    let reg = TestRegistry::spawn().await.unwrap();
    let mut expected_tags = Vec::new();
    for i in 0..7 {
        let tag = format!("t{i}");
        push_manifest(
            &client,
            &tag_ref(&reg, "paged", &tag),
            serde_json::json!({"annotations": {"i": i.to_string()}}),
        )
        .await;
        expected_tags.push(tag);
    }
    let r = tag_ref(&reg, "paged", "t0");
    let opts = ListOptions {
        page_size: Some(3),
        last: None,
    };
    assert_eq!(
        client.list_tags_with(&r, &opts).await.unwrap(),
        expected_tags
    );
    let opts = ListOptions {
        page_size: Some(2),
        last: Some("t3".into()),
    };
    assert_eq!(
        client.list_tags_with(&r, &opts).await.unwrap(),
        vec!["t4", "t5", "t6"]
    );

    let page = raw(reg.addr(), "GET", "/v2/paged/tags/list?n=2", &[], b"").await;
    assert_eq!(page.status, 200);
    assert_eq!(
        page.header("Link"),
        Some("</v2/paged/tags/list?n=2&last=t1>; rel=\"next\"")
    );
    let body: serde_json::Value = serde_json::from_slice(&page.body).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"name": "paged", "tags": ["t0", "t1"]})
    );
    let last = raw(reg.addr(), "GET", "/v2/paged/tags/list?n=10", &[], b"").await;
    assert!(last.header("Link").is_none());
    let bad = raw(reg.addr(), "GET", "/v2/paged/tags/list?n=x", &[], b"").await;
    assert_eq!(bad.status, 400);
    let unknown = raw(reg.addr(), "GET", "/v2/unknown/tags/list", &[], b"").await;
    assert_eq!(
        (unknown.status, unknown.code().as_str()),
        (404, "NAME_UNKNOWN")
    );

    let mut repos = Vec::new();
    for name in ["z/last", "a/first", "m/middle", "b", "paged"] {
        if name != "paged" {
            push_manifest(&client, &tag_ref(&reg, name, "x"), serde_json::json!({})).await;
        }
        repos.push(name.to_string());
    }
    repos.sort();
    let opts = ListOptions {
        page_size: Some(2),
        last: None,
    };
    assert_eq!(
        client.catalog_with(&reg.host(), &opts).await.unwrap(),
        repos
    );
    let page = raw(reg.addr(), "GET", "/v2/_catalog?n=1&last=b", &[], b"").await;
    let body: serde_json::Value = serde_json::from_slice(&page.body).unwrap();
    assert_eq!(body["repositories"], serde_json::json!(["m/middle"]));
    assert_eq!(
        page.header("Link"),
        Some("</v2/_catalog?n=1&last=m/middle>; rel=\"next\"")
    );
}

#[tokio::test]
async fn referrers_with_filtering() {
    let client = client();
    let reg = TestRegistry::spawn().await.unwrap();
    let subject_ref = tag_ref(&reg, "refs/app", "v1");
    let (subject_bytes, subject) =
        push_manifest(&client, &subject_ref, serde_json::json!({})).await;
    let subject_desc = serde_json::json!({
        "mediaType": OCI_MANIFEST, "digest": subject, "size": subject_bytes.len(),
    });

    let mut artifacts = Vec::new();
    for (i, at) in [
        "application/vnd.example.sbom",
        "application/vnd.example.sig",
    ]
    .into_iter()
    .enumerate()
    {
        let extra = serde_json::json!({
            "artifactType": at,
            "subject": subject_desc,
            "annotations": {"org.example.n": i.to_string()},
        });
        let bytes = {
            let config = client.put_blob(&subject_ref, b"{}").await.unwrap();
            let mut m = serde_json::json!({
                "schemaVersion": 2,
                "mediaType": OCI_MANIFEST,
                "config": {"mediaType": "application/vnd.oci.empty.v1+json", "digest": config, "size": 2},
                "layers": [],
            });
            for (k, v) in extra.as_object().unwrap() {
                m[k] = v.clone();
            }
            serde_json::to_vec(&m).unwrap()
        };
        let digest = Digest::sha256(&bytes);
        let resp = client
            .put_manifest_raw(
                &digest_ref(&reg, "refs/app", &digest),
                &bytes,
                &MediaType::OciManifest,
            )
            .await
            .unwrap();
        assert_eq!(resp.oci_subject.as_ref(), Some(&subject));
        artifacts.push((digest, at, bytes.len()));
    }
    // A manifest without an artifactType reports its config media type.
    let (_, cfg_typed) = push_manifest(
        &client,
        &tag_ref(&reg, "refs/app", "plain"),
        serde_json::json!({"subject": subject_desc}),
    )
    .await;

    let all = client
        .referrers(&subject_ref, &subject, None)
        .await
        .unwrap();
    assert_eq!(all.manifests().len(), 3);
    for (digest, at, size) in &artifacts {
        let d = all
            .manifests()
            .iter()
            .find(|d| &d.digest == digest)
            .unwrap();
        assert_eq!(d.size, *size as u64);
        assert_eq!(d.media_type, MediaType::OciManifest);
        assert_eq!(d.artifact_type.as_ref().map(|a| a.as_str()), Some(*at));
        assert!(d.annotations.contains_key("org.example.n"));
    }
    let plain = all
        .manifests()
        .iter()
        .find(|d| d.digest == cfg_typed)
        .unwrap();
    assert_eq!(
        plain.artifact_type.as_ref().map(|a| a.as_str()),
        Some("application/vnd.oci.image.config.v1+json")
    );

    let sig: MediaType = "application/vnd.example.sig".parse().unwrap();
    let filtered = client
        .referrers(&subject_ref, &subject, Some(&sig))
        .await
        .unwrap();
    assert_eq!(filtered.manifests().len(), 1);
    assert_eq!(filtered.manifests()[0].digest, artifacts[1].0);

    let path =
        format!("/v2/refs/app/referrers/{subject}?artifactType=application%2Fvnd.example.sbom");
    let resp = raw(reg.addr(), "GET", &path, &[], b"").await;
    assert_eq!(resp.header("Content-Type"), Some(OCI_INDEX));
    assert_eq!(resp.header("OCI-Filters-Applied"), Some("artifactType"));
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["manifests"].as_array().unwrap().len(), 1);

    // Unknown subjects and repositories have an empty list.
    let none = Digest::sha256(b"nothing");
    assert!(
        client
            .referrers(&subject_ref, &none, None)
            .await
            .unwrap()
            .manifests()
            .is_empty()
    );
    let resp = raw(
        reg.addr(),
        "GET",
        &format!("/v2/refs/none/referrers/{none}"),
        &[],
        b"",
    )
    .await;
    assert_eq!(resp.status, 200);
    let resp = raw(reg.addr(), "GET", "/v2/refs/app/referrers/latest", &[], b"").await;
    assert_eq!((resp.status, resp.code().as_str()), (400, "DIGEST_INVALID"));
}

#[tokio::test]
async fn referrers_can_be_disabled() {
    let client = client();
    let registry = Registry::builder().referrers(false).build().unwrap();
    let reg = TestRegistry::spawn_with(registry).await.unwrap();
    let r = tag_ref(&reg, "norefs", "v1");
    let (bytes, subject) = push_manifest(&client, &r, serde_json::json!({})).await;
    let (_, _) = push_manifest(
        &client,
        &tag_ref(&reg, "norefs", "art"),
        serde_json::json!({"subject": {"mediaType": OCI_MANIFEST, "digest": subject, "size": bytes.len()}}),
    )
    .await;
    let resp = raw(
        reg.addr(),
        "GET",
        &format!("/v2/norefs/referrers/{subject}"),
        &[],
        b"",
    )
    .await;
    assert_eq!((resp.status, resp.code().as_str()), (404, "METHOD_UNKNOWN"));
    // The client falls back to the referrers tag schema (absent here).
    assert!(
        client
            .referrers(&r, &subject, None)
            .await
            .unwrap()
            .manifests()
            .is_empty()
    );
}

#[tokio::test]
async fn warnings_are_attached() {
    let registry = Registry::builder()
        .warning(1.0, "be careful")
        .build()
        .unwrap();
    let reg = TestRegistry::spawn_with(registry).await.unwrap();
    let resp = raw(reg.addr(), "GET", "/v2/", &[], b"").await;
    assert_eq!(resp.header("Warning"), Some("299 - \"be careful\""));
}
