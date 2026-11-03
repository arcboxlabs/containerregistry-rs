//! Streaming, ranged, resumable and parallel blob transfers.
//!
//! Mock-server tests run always; tests against a real registry run with
//! `REGISTRY_INTEGRATION=1` (see `tests/compose/registry`).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use containerregistry_image::Digest;
use containerregistry_registry::{
    Client, ClientConfig, DownloadOptions, Error, Reference, UploadCheckpoint, UploadOptions,
    UploadStatus, error_from_io,
};
use tokio::io::AsyncReadExt;

fn integration_enabled() -> bool {
    std::env::var("REGISTRY_INTEGRATION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn registry_addr() -> String {
    std::env::var("REGISTRY_ANON_ADDR").unwrap_or_else(|_| "127.0.0.1:5000".to_string())
}

fn unique_repo(name: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("streaming/{name}-{ts}")
}

fn client() -> Client {
    Client::with_config(ClientConfig::new().with_https(false)).expect("client")
}

/// Deterministic pseudo-random data (xorshift), so blobs do not compress.
fn data(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Mock server helpers
// ---------------------------------------------------------------------------

struct Req {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

impl Req {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn read_req(stream: &mut std::net::TcpStream) -> Req {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).unwrap_or(0) == 0 {
            break;
        }
        buf.push(byte[0]);
    }
    let text = String::from_utf8_lossy(&buf).to_string();
    let mut lines = text.split("\r\n");
    let mut first = lines.next().unwrap_or("").split_whitespace();
    let method = first.next().unwrap_or("").to_string();
    let path = first.next().unwrap_or("").to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    let _ = stream.read_exact(&mut body);
    Req {
        method,
        path,
        headers,
    }
}

/// A download that is cut off half-way must resume with a `Range` request
/// and still verify the digest of the whole blob.
#[tokio::test]
async fn test_blob_stream_resumes_after_connection_drop() {
    let blob = data(200_000, 7);
    let digest = Digest::sha256(&blob);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let ranges = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    let ranges_srv = ranges.clone();
    let blob_srv = blob.clone();
    let handle = thread::spawn(move || {
        for i in 0..2 {
            let (mut s, _) = listener.accept().unwrap();
            let req = read_req(&mut s);
            ranges_srv
                .lock()
                .unwrap()
                .push(req.header("range").map(str::to_string));
            if i == 0 {
                // Promise everything, deliver 60_000 bytes, then hang up.
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    blob_srv.len()
                );
                s.write_all(head.as_bytes()).unwrap();
                s.write_all(&blob_srv[..60_000]).unwrap();
                s.flush().unwrap();
                drop(s);
            } else {
                let from: usize = req
                    .header("range")
                    .and_then(|r| r.strip_prefix("bytes="))
                    .and_then(|r| r.split('-').next())
                    .and_then(|n| n.parse().ok())
                    .unwrap();
                let rest = &blob_srv[from..];
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nConnection: close\r\n\r\n",
                    rest.len(),
                    from,
                    blob_srv.len() - 1,
                    blob_srv.len()
                );
                s.write_all(head.as_bytes()).unwrap();
                s.write_all(rest).unwrap();
            }
        }
    });

    let reference: Reference = format!("{addr}/repo:tag").parse().unwrap();
    let mut reader = client().get_blob_stream(&reference, &digest).await.unwrap();
    assert_eq!(reader.size(), Some(blob.len() as u64));
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, blob);
    handle.join().unwrap();

    let ranges = ranges.lock().unwrap();
    assert_eq!(ranges[0], None);
    // The resume must start exactly where the first response stopped.
    assert_eq!(ranges[1].as_deref(), Some("bytes=60000-199999"));
}

/// A stream whose bytes do not match the digest fails at EOF with
/// `DigestMismatch`.
#[tokio::test]
async fn test_blob_stream_reports_digest_mismatch_at_eof() {
    let blob = b"actual content".to_vec();
    let wrong = Digest::sha256(b"expected content");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let _ = read_req(&mut s);
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            blob.len()
        );
        s.write_all(head.as_bytes()).unwrap();
        s.write_all(&blob).unwrap();
    });
    let reference: Reference = format!("{addr}/repo:tag").parse().unwrap();
    let err = client().get_blob(&reference, &wrong).await.unwrap_err();
    assert!(matches!(err, Error::DigestMismatch { .. }), "{err:?}");
    handle.join().unwrap();
}

/// A failed chunk is re-sent from the offset the registry reports, not from
/// the start of the chunk and not from the start of the blob.
#[tokio::test]
async fn test_chunk_retry_resumes_from_committed_offset() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let log = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
    let log_srv = log.clone();
    let handle = thread::spawn(move || {
        let responses = [
            // POST
            "HTTP/1.1 202 Accepted\r\nLocation: /v2/repo/blobs/uploads/u1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            // PATCH 0-9 fails after the registry stored 4 bytes.
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            // GET status -> 4 bytes committed.
            "HTTP/1.1 204 No Content\r\nRange: 0-3\r\nLocation: /v2/repo/blobs/uploads/u1\r\nConnection: close\r\n\r\n",
            // PATCH 4-9
            "HTTP/1.1 202 Accepted\r\nRange: 0-9\r\nLocation: /v2/repo/blobs/uploads/u1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            // PUT
            "HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ];
        for resp in responses {
            let (mut s, _) = listener.accept().unwrap();
            let req = read_req(&mut s);
            log_srv.lock().unwrap().push((
                format!("{} {}", req.method, req.path),
                req.header("content-range").map(str::to_string),
            ));
            s.write_all(resp.as_bytes()).unwrap();
        }
    });

    let reference: Reference = format!("{addr}/repo:tag").parse().unwrap();
    let client = Client::with_config(
        ClientConfig::new()
            .with_https(false)
            .with_retry_backoff(Duration::from_millis(1)),
    )
    .unwrap();
    let blob = b"0123456789".to_vec();
    let uploaded = client
        .push_blob_stream(
            &reference,
            std::io::Cursor::new(blob.clone()),
            UploadOptions::default().with_chunk_size(10),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.digest, Digest::sha256(&blob));
    assert_eq!(uploaded.status, UploadStatus::Uploaded);
    handle.join().unwrap();

    let log = log.lock().unwrap();
    let summary: Vec<(String, Option<String>)> = log.clone();
    assert_eq!(summary[0].0, "POST /v2/repo/blobs/uploads/");
    assert_eq!(
        summary[1],
        ("PATCH /v2/repo/blobs/uploads/u1".into(), Some("0-9".into()))
    );
    assert_eq!(summary[2].0, "GET /v2/repo/blobs/uploads/u1");
    assert_eq!(
        summary[3],
        ("PATCH /v2/repo/blobs/uploads/u1".into(), Some("4-9".into()))
    );
    assert!(summary[4].0.starts_with(&format!(
        "PUT /v2/repo/blobs/uploads/u1?digest={}",
        Digest::sha256(&blob)
    )));
}

// ---------------------------------------------------------------------------
// Real registry
// ---------------------------------------------------------------------------

async fn setup(name: &str) -> Option<(Client, Reference)> {
    if !integration_enabled() {
        eprintln!("skipping (set REGISTRY_INTEGRATION=1)");
        return None;
    }
    let client = client();
    let addr = registry_addr();
    for _ in 0..50 {
        if client.ping(&addr).await.is_ok() {
            let reference = format!("{addr}/{}:latest", unique_repo(name))
                .parse()
                .unwrap();
            return Some((client, reference));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("registry {addr} not ready");
}

#[tokio::test]
async fn test_integration_stream_upload_and_download() {
    let Some((client, reference)) = setup("stream").await else {
        return;
    };
    let blob = data(9 * 1024 * 1024 + 123, 1);
    let digest = Digest::sha256(&blob);
    let progress = Arc::new(AtomicU64::new(0));
    let p = progress.clone();

    // Single streamed PATCH; digest computed on the fly.
    let uploaded = client
        .push_blob_stream(
            &reference,
            std::io::Cursor::new(blob.clone()),
            UploadOptions::default().with_progress(Arc::new(move |u| {
                p.store(u.complete, Ordering::Relaxed);
            })),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.digest, digest);
    assert_eq!(uploaded.size, blob.len() as u64);
    assert_eq!(progress.load(Ordering::Relaxed), blob.len() as u64);

    // Re-pushing with a known digest is a no-op.
    let again = client
        .push_blob_stream(
            &reference,
            std::io::Cursor::new(blob.clone()),
            UploadOptions::default().with_digest(digest.clone()),
        )
        .await
        .unwrap();
    assert_eq!(again.status, UploadStatus::AlreadyExists);

    let mut reader = client.get_blob_stream(&reference, &digest).await.unwrap();
    assert_eq!(reader.size(), Some(blob.len() as u64));
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert!(out == blob, "downloaded bytes differ");

    // Range reads.
    let mut r = client
        .get_blob_range(&reference, &digest, 1_000_000..1_000_100)
        .await
        .unwrap();
    let mut part = Vec::new();
    r.read_to_end(&mut part).await.unwrap();
    assert_eq!(part, blob[1_000_000..1_000_100]);
    let mut r = client
        .get_blob_range(&reference, &digest, (blob.len() as u64 - 10)..)
        .await
        .unwrap();
    let mut tail = Vec::new();
    r.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, blob[blob.len() - 10..]);
}

#[tokio::test]
async fn test_integration_chunked_stream_and_digest_check() {
    let Some((client, reference)) = setup("chunked").await else {
        return;
    };
    let blob = data(5 * 1024 * 1024 + 7, 2);
    let digest = Digest::sha256(&blob);
    let checkpoints = Arc::new(Mutex::new(Vec::<UploadCheckpoint>::new()));
    let c = checkpoints.clone();
    let uploaded = client
        .push_blob_stream(
            &reference,
            std::io::Cursor::new(blob.clone()),
            UploadOptions::default()
                .with_chunk_size(1024 * 1024)
                .with_checkpoint(Arc::new(move |cp| c.lock().unwrap().push(cp.clone()))),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.digest, digest);
    let offsets: Vec<u64> = checkpoints
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.offset)
        .collect();
    assert_eq!(
        offsets,
        vec![
            1 << 20,
            2 << 20,
            3 << 20,
            4 << 20,
            5 << 20,
            blob.len() as u64
        ]
    );
    assert_eq!(client.get_blob(&reference, &digest).await.unwrap(), blob);

    // A wrong expected digest aborts before commit.
    let other = data(1000, 3);
    let err = client
        .push_blob_stream(
            &reference,
            std::io::Cursor::new(other.clone()),
            UploadOptions::default().with_digest(Digest::sha256(b"nope")),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DigestMismatch { .. }), "{err:?}");
    assert!(
        client
            .blob_exists(&reference, &Digest::sha256(&other))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_integration_download_resume_and_parallel() {
    let Some((client, reference)) = setup("download").await else {
        return;
    };
    let blob = data(8 * 1024 * 1024 + 99, 4);
    let digest = Digest::sha256(&blob);
    client.put_blob(&reference, &blob).await.unwrap();
    let dir = tempfile::tempdir().unwrap();

    // Parallel ranged download.
    let dest = dir.path().join("parallel");
    let n = client
        .download_blob(
            &reference,
            &digest,
            &dest,
            DownloadOptions::default()
                .with_parallelism(4)
                .with_part_size(1024 * 1024),
        )
        .await
        .unwrap();
    assert_eq!(n, blob.len() as u64);
    assert!(std::fs::read(&dest).unwrap() == blob);
    assert!(!dir.path().join("parallel.partial").exists());
    assert!(!dir.path().join("parallel.partial.json").exists());

    // Sequential resume from a partial file left by an earlier attempt.
    let dest = dir.path().join("resumed");
    std::fs::write(dir.path().join("resumed.partial"), &blob[..3_000_000]).unwrap();
    client
        .download_blob(&reference, &digest, &dest, DownloadOptions::default())
        .await
        .unwrap();
    assert!(std::fs::read(&dest).unwrap() == blob);

    // A corrupt partial file is detected and discarded.
    let dest = dir.path().join("corrupt");
    let mut bad = blob[..1000].to_vec();
    bad[10] ^= 0xff;
    std::fs::write(dir.path().join("corrupt.partial"), &bad).unwrap();
    let err = client
        .download_blob(&reference, &digest, &dest, DownloadOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DigestMismatch { .. }), "{err:?}");
    assert!(!dest.exists());
    assert!(!dir.path().join("corrupt.partial").exists());
    // ...so the next attempt starts clean and succeeds.
    client
        .download_blob(&reference, &digest, &dest, DownloadOptions::default())
        .await
        .unwrap();
    assert!(std::fs::read(&dest).unwrap() == blob);

    // Parallel resume: parts recorded as done are not fetched again.
    let dest = dir.path().join("parts");
    let partial = dir.path().join("parts.partial");
    let mut holey = vec![0u8; blob.len()];
    holey[..2 * 1024 * 1024].copy_from_slice(&blob[..2 * 1024 * 1024]);
    std::fs::write(&partial, &holey).unwrap();
    let parts = blob.len().div_ceil(1024 * 1024);
    let mut done = vec![false; parts];
    done[0] = true;
    done[1] = true;
    std::fs::write(
        dir.path().join("parts.partial.json"),
        serde_json::json!({
            "digest": digest.to_string(),
            "size": blob.len(),
            "part_size": 1024 * 1024,
            "done": done,
        })
        .to_string(),
    )
    .unwrap();
    let fetched = Arc::new(AtomicU64::new(u64::MAX));
    let f = fetched.clone();
    client
        .download_blob(
            &reference,
            &digest,
            &dest,
            DownloadOptions::default()
                .with_parallelism(3)
                .with_part_size(1024 * 1024)
                .with_progress(Arc::new(move |p| {
                    f.fetch_min(p.complete, Ordering::Relaxed);
                })),
        )
        .await
        .unwrap();
    assert!(std::fs::read(&dest).unwrap() == blob);
    // Progress starts from the already-completed 2 MiB.
    assert!(fetched.load(Ordering::Relaxed) > 2 * 1024 * 1024);
}

#[tokio::test]
async fn test_integration_file_upload_resumes_session() {
    let Some((client, reference)) = setup("file").await else {
        return;
    };
    let blob = data(3 * 1024 * 1024 + 5, 5);
    let digest = Digest::sha256(&blob);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disk.img");
    std::fs::write(&path, &blob).unwrap();

    // Simulate a previous process that uploaded 2 MiB and then died.
    let mut session = client.start_blob_upload(&reference).await.unwrap();
    session
        .write_chunk(bytes::Bytes::copy_from_slice(&blob[..1024 * 1024]))
        .await
        .unwrap();
    session
        .write_chunk(bytes::Bytes::copy_from_slice(
            &blob[1024 * 1024..2 * 1024 * 1024],
        ))
        .await
        .unwrap();
    let location = session.location().to_string();
    drop(session);

    let resumed = client
        .resume_blob_upload(&reference, &location)
        .await
        .unwrap();
    assert_eq!(resumed.offset(), 2 * 1024 * 1024);

    let uploaded = client
        .push_blob_file(
            &reference,
            &path,
            UploadOptions::default()
                .with_chunk_size(512 * 1024)
                .with_resume_location(location),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.digest, digest);
    assert_eq!(uploaded.size, blob.len() as u64);
    assert_eq!(client.get_blob(&reference, &digest).await.unwrap(), blob);

    // Streamed (single PATCH) file upload of a fresh blob.
    let blob2 = data(1024 * 1024, 6);
    std::fs::write(&path, &blob2).unwrap();
    let up2 = client
        .push_blob_file(&reference, &path, UploadOptions::default())
        .await
        .unwrap();
    assert_eq!(up2.digest, Digest::sha256(&blob2));

    // Cross-repository mount.
    let other: Reference = format!(
        "{}/{}:latest",
        reference.registry(),
        unique_repo("mount-target")
    )
    .parse()
    .unwrap();
    let mounted = client
        .push_blob_file(
            &other,
            &path,
            UploadOptions::default()
                .with_digest(up2.digest.clone())
                .with_mount_from(reference.repository()),
        )
        .await
        .unwrap();
    assert_eq!(mounted.status, UploadStatus::Mounted);
    let mut r = client.get_blob_stream(&other, &up2.digest).await.unwrap();
    let mut out = Vec::new();
    r.read_to_end(&mut out)
        .await
        .map_err(error_from_io)
        .unwrap();
    assert!(out == blob2);
}
