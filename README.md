# containerregistry-rs

A Rust implementation of [go-containerregistry](https://github.com/google/go-containerregistry), providing OCI container registry client libraries and `crane`/`gcrane`-compatible CLI tools.

## Features

- **OCI & Docker support** — Handles both OCI Image/Distribution specs and Docker v2 schema 2 manifests, with automatic format detection
- **Multi-arch images** — Full support for image indexes / manifest lists with platform filtering
- **OCI image layouts** — Read and write [OCI Image Layout](https://github.com/opencontainers/image-spec/blob/main/image-layout.md) directories
- **Auth** — Docker config (`~/.docker/config.json`), credential helpers, bearer token negotiation, and basic auth
- **Digest verification** — SHA-256/384/512 content verification on all downloads
- **Retry & resilience** — Exponential backoff with `Retry-After` support, automatic bearer token refresh on 401
- **Streaming blobs** — Uploads and downloads stream through `AsyncRead` and hash on the fly, so multi-gigabyte blobs never sit in memory
- **Resumable & parallel transfers** — `Range` reads, auto-resuming downloads, parallel ranged downloads to files, chunked uploads that resume from the registry's offset (also across processes), concurrent layer transfers and cross-repo mounts
- **Image model** — `Image` / `ImageIndex` / `Layer` traits with `mutate`, `tarball` (incl. legacy `docker save`), `empty`, `random`, `partial`, `stream`, `validate`, `cache` and matchers, backed by registries, OCI layouts and tarballs
- **Registry server** — An in-memory or disk-backed OCI registry that passes the distribution-spec conformance suite (`crane registry serve`)

## Crates

| Package | Path | Description |
|---------|------|-------------|
| `containerregistry` | `.` | Facade crate re-exporting all library crates |
| `containerregistry-image` | `crates/image` | Core types: `Manifest`, `ImageIndex`, `Descriptor`, `Digest`, `MediaType`, `Platform` |
| `containerregistry-v1` | `crates/v1` | `Image`/`ImageIndex`/`Layer` traits, `mutate`, `tarball`, `empty`, `random`, `partial`, `stream`, `validate`, `cache` (go `pkg/v1`) |
| `containerregistry-registry` | `crates/registry` | HTTP registry client, streaming/resumable/parallel blobs, `remote` images (go `pkg/v1/remote`) |
| `containerregistry-auth` | `crates/auth` | Keychains, Docker config, credential helpers, bearer/basic auth (go `pkg/authn`) |
| `containerregistry-layout` | `crates/layout` | OCI image layout read/write, layouts as v1 sources and sinks, gc (go `pkg/v1/layout`) |
| `containerregistry-server` | `crates/server` | In-memory / disk-backed registry (go `pkg/registry`) |
| `containerregistry-crane` | `bins/crane` | crane-compatible CLI with go crane's full command set |
| `containerregistry-gcrane` | `bins/gcrane` | gcrane-compatible CLI (copy -r, ls -r/--json, gc, plus every crane command) |
| `containerregistry-testing` | `crates/testing` | Test harness: fixtures, golden comparators, parity runner, fault injection |

## Quick Start

### As a library

```rust
use containerregistry::registry::{Client, Reference};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new()?;
    let reference: Reference = "ghcr.io/my-org/my-image:latest".parse()?;

    // Get manifest (auto-detects manifest vs index)
    let (manifest_or_index, digest) = client.get_manifest(&reference).await?;
    println!("digest: {}", digest);

    // Download a blob
    let blob = client.get_blob(&reference, &digest).await?;

    Ok(())
}
```

Large blobs (e.g. VM disk layers) stream instead:

```rust,ignore
use containerregistry::registry::{DownloadOptions, UploadOptions};

// Verified, auto-resuming stream (AsyncRead + AsyncBufRead)
let mut reader = client.get_blob_stream(&reference, &digest).await?;
// Byte range
let part = client.get_blob_range(&reference, &digest, 0..1024).await?;
// To a file: resumes `<dest>.partial`, 8 concurrent ranges
client
    .download_blob(&reference, &digest, dest, DownloadOptions::default().with_parallelism(8))
    .await?;
// Upload a file in 64 MiB chunks, resuming after failures
client
    .push_blob_file(&reference, path, UploadOptions::default().with_chunk_size(64 << 20))
    .await?;
```

Whole images copy with concurrent, deduplicated, mount-aware layer transfers:

```rust,ignore
use containerregistry::registry::remote::{self, Options};

let img = remote::image(&client, &src, &Options::default()).await?;
remote::write(&client, &dst, img, &Options::default().with_jobs(8)).await?;
```

### CLI

```bash
# Build the CLI tools
cargo build --release

# Get image digest
crane digest ghcr.io/my-org/my-image:latest

# Pull to an OCI layout (or a docker tarball: --format tarball|legacy)
crane pull --format oci ghcr.io/my-org/my-image:latest ./image

# Push from an OCI layout or tarball
crane push ./image ghcr.io/my-org/my-image:v2

# Mutate, flatten, rebase, export, index append/filter, edit, validate, ...
crane mutate ghcr.io/my-org/my-image:v2 -l team=infra -t ghcr.io/my-org/my-image:v3

# Run a local registry
crane registry serve --address 127.0.0.1:5000

# List tags
crane ls ghcr.io/my-org/my-image

# Copy between registries
crane copy source-registry/image:tag dest-registry/image:tag
```

## Building

```bash
cargo build            # Debug build
cargo build --release  # Release build
cargo clippy           # Lint (CI enforces -D warnings)
cargo fmt              # Format
```

## Testing

```bash
# Unit and mock-server tests
cargo test

# Integration tests (requires local Docker registries)
cd tests/compose/registry && docker compose up -d
REGISTRY_INTEGRATION=1 cargo test
cd tests/compose/registry && docker compose down -v

# Parity tests (compare Rust crane vs Go crane)
CRANE_RUST_PATH=./target/debug/crane cargo test -p containerregistry-testing
```

## Specifications

- [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
- [OCI Image Spec](https://github.com/opencontainers/image-spec/blob/main/spec.md)
- [OCI Image Layout Spec](https://github.com/opencontainers/image-spec/blob/main/image-layout.md)

When specs are ambiguous, go-containerregistry behavior is followed.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
