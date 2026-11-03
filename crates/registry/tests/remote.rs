//! `remote` image/index reads and concurrent writes against an in-process
//! registry.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use containerregistry_image::{Digest, MediaType, Platform};
use containerregistry_registry::remote::{self, Options};
use containerregistry_registry::{Client, Progress, Reference};
use containerregistry_server::TestRegistry;
use containerregistry_v1::stream::StreamLayer;
use containerregistry_v1::{Image, ImageIndex, Layer, empty, mutate, random, validate};

fn reference(host: &str, repo: &str, tag: &str) -> Reference {
    format!("{host}/{repo}:{tag}").parse().unwrap()
}

#[tokio::test]
async fn image_roundtrip_with_progress_and_concurrency() {
    let reg = TestRegistry::spawn().await.unwrap();
    let client = Client::new().unwrap();
    let img = random::image(256 * 1024, 8).await.unwrap();
    let r = reference(&reg.host(), "test/img", "v1");

    let done = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let (d, t) = (done.clone(), total.clone());
    let opts = Options::default()
        .with_jobs(4)
        .with_progress(Arc::new(move |p: Progress| {
            d.store(p.complete, Ordering::Relaxed);
            t.store(p.total, Ordering::Relaxed);
        }));
    remote::write(&client, &r, img.clone(), &opts)
        .await
        .unwrap();
    let blobs: u64 = {
        let m = img.manifest().await.unwrap();
        m.layers().iter().map(|l| l.size).sum::<u64>() + m.config().size
    };
    assert_eq!(total.load(Ordering::Relaxed), blobs);
    assert_eq!(done.load(Ordering::Relaxed), blobs);

    let got = remote::image(&client, &r, &Options::default())
        .await
        .unwrap();
    assert_eq!(got.digest().await.unwrap(), img.digest().await.unwrap());
    assert_eq!(
        got.raw_manifest().await.unwrap(),
        img.raw_manifest().await.unwrap()
    );
    validate::image(got.as_ref(), validate::Options::default())
        .await
        .unwrap();

    // Copy to another repository on the same registry: blobs are mounted.
    let r2 = reference(&reg.host(), "test/copy", "v1");
    remote::write(&client, &r2, got.clone(), &Options::default())
        .await
        .unwrap();
    let copied = remote::image(&client, &r2, &Options::default())
        .await
        .unwrap();
    validate::image(copied.as_ref(), validate::Options::default())
        .await
        .unwrap();

    // Remote layers download into files with the resumable downloader.
    let dir = tempfile::tempdir().unwrap();
    let layer = copied.layers().await.unwrap().remove(0);
    let dest = dir.path().join("blob");
    assert!(layer.download_to(&dest).await.unwrap());
    let data = std::fs::read(&dest).unwrap();
    assert_eq!(Digest::sha256(&data), layer.digest().await.unwrap());
}

#[tokio::test]
async fn index_roundtrip_and_platform_selection() {
    let reg = TestRegistry::spawn().await.unwrap();
    let client = Client::new().unwrap();
    let idx = random::index(1024, 2, 3).await.unwrap();
    let r = reference(&reg.host(), "test/idx", "multi");
    remote::write_index(&client, &r, idx.clone(), &Options::default())
        .await
        .unwrap();

    let got = remote::index(&client, &r).await.unwrap();
    assert_eq!(got.digest().await.unwrap(), idx.digest().await.unwrap());
    validate::index(got.as_ref(), validate::Options::default())
        .await
        .unwrap();

    let want = idx.index_manifest().await.unwrap().manifests()[1].clone();
    let opts = Options::default().with_platform(Platform::new("arch1", "linux"));
    let img = remote::image(&client, &r, &opts).await.unwrap();
    assert_eq!(img.digest().await.unwrap(), want.digest);
    assert_eq!(img.descriptor().await.unwrap().platform, want.platform);
    assert!(
        remote::image(&client, &r, &Options::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn streaming_layer_and_artifact_push() {
    let reg = TestRegistry::spawn().await.unwrap();
    let client = Client::new().unwrap();

    // A layer of unknown size: digest and diff ID are computed during upload.
    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i * 7 % 251) as u8).collect();
    let stream: Arc<dyn Layer> = Arc::new(StreamLayer::new(Box::pin(Cursor::new(data))));
    let img = mutate::append_layers(empty::image(), vec![stream]);
    let r = reference(&reg.host(), "test/stream", "latest");
    remote::write(&client, &r, img.clone(), &Options::default())
        .await
        .unwrap();
    let got = remote::image(&client, &r, &Options::default())
        .await
        .unwrap();
    assert_eq!(got.digest().await.unwrap(), img.digest().await.unwrap());
    validate::image(got.as_ref(), validate::Options::default())
        .await
        .unwrap();

    // An OCI artifact with opaque, custom-typed layers (Tart-style disk).
    let disk = MediaType::Other("application/vnd.cirruslabs.tart.disk.v2".into());
    let mut layers: Vec<Arc<dyn Layer>> = Vec::new();
    for i in 0..3u8 {
        layers.push(Arc::new(
            containerregistry_v1::static_layer::StaticLayer::new(vec![i; 100_000], disk.clone()),
        ));
    }
    let art = mutate::media_type(
        mutate::config_media_type(
            mutate::append_layers(empty::image(), layers),
            MediaType::Other("application/vnd.cirruslabs.tart.config.v1".into()),
        ),
        MediaType::OciManifest,
    );
    let ra = reference(&reg.host(), "test/vm", "sonoma");
    remote::write(&client, &ra, art.clone(), &Options::default())
        .await
        .unwrap();
    let got = remote::image(&client, &ra, &Options::default())
        .await
        .unwrap();
    assert_eq!(
        got.raw_manifest().await.unwrap(),
        art.raw_manifest().await.unwrap()
    );
    let ls = got.layers().await.unwrap();
    assert_eq!(ls[2].media_type().await.unwrap(), disk);
    assert_eq!(
        ls[2].diff_id().await.unwrap(),
        ls[2].digest().await.unwrap()
    );
    validate::image(got.as_ref(), validate::Options::default())
        .await
        .unwrap();
}
