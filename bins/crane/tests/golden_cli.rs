//! Golden-style CLI tests for crane commands.

use std::fs;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use containerregistry_image::{Descriptor, Digest, ImageConfig, Manifest, MediaType, OciManifest};
use containerregistry_layout::Layout;
use containerregistry_registry::{Client, ClientConfig, ManifestOrIndex, Reference};

fn integration_enabled() -> bool {
    std::env::var("REGISTRY_INTEGRATION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn anon_registry_addr() -> String {
    std::env::var("REGISTRY_ANON_ADDR").unwrap_or_else(|_| "127.0.0.1:5000".to_string())
}

async fn wait_for_registry(addr: &str) {
    let client = Client::with_config(ClientConfig::new().with_https(false)).expect("client");
    let mut attempts = 0;
    loop {
        match client.ping(addr).await {
            Ok(()) => return,
            Err(_) => {
                attempts += 1;
                if attempts > 50 {
                    panic!("registry {} not ready", addr);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

fn unique_repo(name: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    format!("cli-crane/{name}-{ts}")
}

fn build_minimal_manifest() -> (Vec<u8>, Vec<u8>, Manifest, Digest, Digest) {
    let config = ImageConfig::new("amd64", "linux");
    let config_bytes = config.to_bytes().expect("config bytes");
    let config_digest = Digest::sha256(&config_bytes);

    let layer_bytes = b"crane-cli-layer".to_vec();
    let layer_digest = Digest::sha256(&layer_bytes);

    let config_desc = Descriptor::new(
        MediaType::OciConfig,
        config_digest.clone(),
        config_bytes.len() as u64,
    );
    let layer_desc = Descriptor::new(
        MediaType::OciLayer,
        layer_digest.clone(),
        layer_bytes.len() as u64,
    );

    let oci = OciManifest::new(config_desc, vec![layer_desc]);
    let manifest = Manifest::Oci(oci);

    (
        config_bytes,
        layer_bytes,
        manifest,
        config_digest,
        layer_digest,
    )
}

fn run_crane(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_crane"))
        .args(args)
        .output()
        .expect("run crane")
}

#[tokio::test]
async fn test_crane_commands_golden() {
    if !integration_enabled() {
        eprintln!("skipping CLI golden test (set REGISTRY_INTEGRATION=1)");
        return;
    }

    let addr = anon_registry_addr();
    wait_for_registry(&addr).await;

    let repo = unique_repo("golden");
    let tag = "v1";
    let image = format!("{addr}/{repo}:{tag}");
    let reference: Reference = image.parse().unwrap();

    let (config_bytes, layer_bytes, manifest, config_digest, layer_digest) =
        build_minimal_manifest();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();
    client.put_blob(&reference, &config_bytes).await.unwrap();
    client.put_blob(&reference, &layer_bytes).await.unwrap();
    let manifest_digest = client.put_manifest(&reference, &manifest).await.unwrap();

    // digest
    let output = run_crane(&["--insecure", "digest", &image]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        manifest_digest.to_string()
    );

    let output = run_crane(&["--insecure", "digest", "--full-ref", &image]);
    assert!(output.status.success());
    let full = format!(
        "{}/{}@{}",
        reference.registry(),
        reference.repository(),
        manifest_digest
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), full);

    // manifest
    let output = run_crane(&["--insecure", "manifest", &image]);
    assert!(output.status.success());
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).expect("manifest json");
    let expected: serde_json::Value =
        serde_json::from_slice(&manifest.to_bytes().unwrap()).expect("expected manifest json");
    assert_eq!(actual, expected);

    // config
    let output = run_crane(&["--insecure", "config", &image]);
    assert!(output.status.success());
    let actual_cfg: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("config json");
    let expected_cfg: serde_json::Value =
        serde_json::from_slice(&config_bytes).expect("expected config json");
    assert_eq!(actual_cfg, expected_cfg);

    // ls
    let output = run_crane(&["--insecure", "ls", &format!("{addr}/{repo}")]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.lines().any(|line| line.trim() == tag));

    // pull
    let pull_dir = tempfile::tempdir().unwrap();
    let output = run_crane(&[
        "--insecure",
        "pull",
        "--format",
        "oci",
        "--output",
        pull_dir.path().to_str().unwrap(),
        &image,
    ]);
    assert!(output.status.success());
    let pulled = Layout::open(pull_dir.path()).expect("open pulled layout");
    let index = pulled.index().expect("read pulled index");
    let pulled_desc = index.manifests().first().expect("manifest desc");
    assert_eq!(pulled_desc.digest, manifest_digest);

    // push (from layout)
    let layout_dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(layout_dir.path()).unwrap();
    layout
        .write_blob_with_digest(&config_bytes, &config_digest)
        .unwrap();
    layout
        .write_blob_with_digest(&layer_bytes, &layer_digest)
        .unwrap();
    layout.write_manifest(&manifest).unwrap();
    layout.add_manifest(&manifest).unwrap();

    let repo_push = unique_repo("push");
    let image_push = format!("{addr}/{repo_push}:{tag}");
    let output = run_crane(&[
        "--insecure",
        "push",
        &image_push,
        layout_dir.path().to_str().unwrap(),
    ]);
    assert!(output.status.success());

    let reference_push: Reference = image_push.parse().unwrap();
    let (got, got_digest) = client.get_manifest(&reference_push).await.unwrap();
    assert!(matches!(got, ManifestOrIndex::Manifest(_)));
    assert_eq!(got_digest, manifest_digest);

    // copy
    let repo_copy = unique_repo("copy");
    let image_copy = format!("{addr}/{repo_copy}:{tag}");
    let output = run_crane(&["--insecure", "copy", &image, &image_copy]);
    assert!(output.status.success());
    let reference_copy: Reference = image_copy.parse().unwrap();
    let (_got, copied_digest) = client.get_manifest(&reference_copy).await.unwrap();
    assert_eq!(copied_digest, manifest_digest);

    // catalog
    let output = run_crane(&["--insecure", "catalog", &addr]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.lines().any(|line| line.trim() == repo));

    // Cleanup temp dirs (best effort)
    let _ = fs::remove_dir_all(layout_dir.path());
}
