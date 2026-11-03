//! Image, index and layer abstractions (go-containerregistry `pkg/v1`).
//!
//! The traits [`Image`], [`ImageIndex`] and [`Layer`] model content
//! independently of where it lives; implementations exist for registries
//! (`containerregistry-registry`'s `remote` module), OCI layouts
//! (`containerregistry-layout`), `docker save` tarballs ([`tarball`]) and
//! memory ([`empty`], [`random`], [`static_layer`]). Images are transformed
//! with [`mutate`], checked with [`validate`] and cached with [`cache`].
//!
//! All content is streamed through [`BoxReader`]s, so layers of any size can
//! flow between sources and sinks without being held in memory.

pub mod cache;
pub mod compression;
pub mod empty;
mod error;
mod image;
mod layer;
pub mod matcher;
pub mod mutate;
pub mod partial;
mod pipe;
pub mod random;
pub mod static_layer;
pub mod stream;
pub mod tarball;
mod tarutil;
pub mod validate;

pub use containerregistry_image::ImageIndex as IndexManifest;
pub use error::{Error, Result};
pub use image::{Image, ImageIndex, Taggable, probe_media_type};
pub use layer::{BoxReader, Layer, MountSource, read_all};
