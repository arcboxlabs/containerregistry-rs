//! OCI image layout read/write operations.
//!
//! This crate provides functionality for reading and writing OCI image layouts
//! on the local filesystem, as defined by the OCI Image Layout Specification.
//!
//! # Overview
//!
//! An OCI image layout is a directory structure containing:
//! - `oci-layout` - A JSON file indicating the layout version
//! - `index.json` - An image index pointing to manifests
//! - `blobs/<algorithm>/<hex>` - Content-addressable blob storage
//!
//! # Example
//!
//! ```no_run
//! use containerregistry_layout::Layout;
//!
//! // Create a new layout
//! let layout = Layout::create("/tmp/my-image")?;
//!
//! // Write a blob
//! let digest = layout.write_blob(b"layer content")?;
//!
//! // Read back the blob
//! let data = layout.read_blob(&digest)?;
//! # Ok::<(), containerregistry_layout::Error>(())
//! ```

mod error;
mod layout;
mod oci_layout;
mod v1;

pub use error::Error;
pub use layout::Layout;
pub use oci_layout::OciLayout;
pub use v1::AppendOptions;

/// Result type for layout operations.
pub type Result<T> = std::result::Result<T, Error>;

/// OCI image layout version.
pub const IMAGE_LAYOUT_VERSION: &str = "1.0.0";
