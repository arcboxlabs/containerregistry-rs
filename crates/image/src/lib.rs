//! OCI/Docker image manifest, index, and layer types.
//!
//! This crate provides types for working with container image manifests,
//! including OCI image manifests, Docker v2 schema 2 manifests, and
//! multi-architecture image indexes.
//!
//! # Examples
//!
//! Parse an OCI manifest:
//!
//! ```
//! use containerregistry_image::{OciManifest, Descriptor, MediaType};
//!
//! let manifest = OciManifest::new(
//!     Descriptor::new(
//!         MediaType::OciConfig,
//!         "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".parse().unwrap(),
//!         2,
//!     ),
//!     vec![],
//! );
//!
//! let bytes = manifest.to_bytes().unwrap();
//! let digest = manifest.digest().unwrap();
//! ```

mod config;
mod descriptor;
mod digest;
mod error;
mod index;
mod manifest;
mod media_type;

pub use config::{ContainerConfig, EmptyObject, Healthcheck, History, ImageConfig, RootFs};
pub use descriptor::{Descriptor, Platform};
pub use digest::{Algorithm, Digest, Hasher};
pub use error::Error;
pub use index::{DockerManifestList, ImageIndex, OciIndex};
pub use manifest::{DockerManifest, Manifest, OciManifest};
pub use media_type::MediaType;

/// Result type for image operations.
pub type Result<T> = std::result::Result<T, Error>;
