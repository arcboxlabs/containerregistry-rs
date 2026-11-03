//! Golden-style CLI tests for gcrane commands.

use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use containerregistry_image::{Descriptor, Digest, ImageConfig, Manifest, MediaType, OciManifest};
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
    format!("cli-gcrane/{name}-{ts}")
}

fn build_minimal_manifest() -> (Vec<u8>, Vec<u8>, Manifest, Digest, Digest) {
    let config = ImageConfig::new("amd64", "linux");
    let config_bytes = config.to_bytes().expect("config bytes");
    let config_digest = Digest::sha256(&config_bytes);

    let layer_bytes = b"gcrane-cli-layer".to_vec();
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

fn run_gcrane(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_gcrane"))
        .args(args)
        .output()
        .expect("run gcrane")
}

#[tokio::test]
async fn test_gcrane_cp_ls_gc_golden() {
    if !integration_enabled() {
        eprintln!("skipping gcrane CLI test (set REGISTRY_INTEGRATION=1)");
        return;
    }

    let addr = anon_registry_addr();
    wait_for_registry(&addr).await;

    let repo = unique_repo("src");
    let tag = "v1";
    let image = format!("{addr}/{repo}:{tag}");
    let reference: Reference = image.parse().unwrap();

    let (config_bytes, layer_bytes, manifest, _config_digest, _layer_digest) =
        build_minimal_manifest();
    let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();
    client.put_blob(&reference, &config_bytes).await.unwrap();
    client.put_blob(&reference, &layer_bytes).await.unwrap();
    let manifest_digest = client.put_manifest(&reference, &manifest).await.unwrap();

    let repo_dst = unique_repo("dst");
    let image_dst = format!("{addr}/{repo_dst}:{tag}");

    let output = run_gcrane(&["--insecure", "cp", &image, &image_dst]);
    assert!(output.status.success(), "cp failed: {:?}", output);

    let reference_dst: Reference = image_dst.parse().unwrap();
    let (got, got_digest) = client.get_manifest(&reference_dst).await.unwrap();
    assert!(matches!(got, ManifestOrIndex::Manifest(_)));
    assert_eq!(got_digest, manifest_digest);

    let output = run_gcrane(&["--insecure", "ls", &format!("{addr}/{repo}")]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Like go's gcrane, plain registries list as `repo:tag`.
    let want = format!("{addr}/{repo}:{tag}");
    assert!(stdout.lines().any(|line| line.trim() == want), "{}", stdout);

    // Every manifest here is tagged, so gc reports nothing.
    let output = run_gcrane(&["--insecure", "gc", &format!("{addr}/{repo}")]);
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}
