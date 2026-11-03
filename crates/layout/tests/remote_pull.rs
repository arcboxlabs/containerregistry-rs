//! Pulling registry images into a layout, including resuming a partial blob.

use containerregistry_image::MediaType;
use containerregistry_layout::{AppendOptions, Layout};
use containerregistry_registry::remote::{self, Options};
use containerregistry_registry::{Client, Reference};
use containerregistry_server::TestRegistry;
use containerregistry_v1::{Image, ImageIndex, mutate, random, validate};

#[tokio::test]
async fn pull_into_layout_resumes_partial_blob() {
    let reg = TestRegistry::spawn().await.unwrap();
    let client = Client::new().unwrap();
    let big = random::layer(5 << 20, MediaType::OciLayer).await.unwrap();
    let img = mutate::append_layers(random::image(1024, 2).await.unwrap(), vec![big.clone()]);
    let r: Reference = format!("{}/vm/disk:latest", reg.host()).parse().unwrap();
    remote::write(&client, &r, img.clone(), &Options::default())
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    // Simulate an interrupted earlier pull of the big layer.
    let digest = big.digest().await.unwrap();
    let dest = layout.blob_path(&digest);
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    let full = containerregistry_v1::read_all(big.compressed().await.unwrap())
        .await
        .unwrap();
    let mut partial = dest.clone().into_os_string();
    partial.push(".partial");
    std::fs::write(&partial, &full[..full.len() / 3]).unwrap();

    let remote_img = remote::image(&client, &r, &Options::default())
        .await
        .unwrap();
    layout
        .append_image(remote_img.as_ref(), &AppendOptions::default())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), full);
    assert!(!std::path::Path::new(&partial).exists());

    let root = layout.image_index_v1().unwrap();
    let got = root.image(&img.digest().await.unwrap()).await.unwrap();
    validate::image(got.as_ref(), validate::Options::default())
        .await
        .unwrap();

    // And push it back out from the layout (layout layers upload from file).
    let r2: Reference = format!("{}/vm/copy:latest", reg.host()).parse().unwrap();
    remote::write(&client, &r2, got, &Options::default())
        .await
        .unwrap();
    let back = remote::image(&client, &r2, &Options::default())
        .await
        .unwrap();
    assert_eq!(back.digest().await.unwrap(), img.digest().await.unwrap());
}
