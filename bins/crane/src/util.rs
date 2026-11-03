//! Shared helpers: references, platforms, loading and publishing images.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};

use containerregistry_image::{Digest, MediaType, Platform};
use containerregistry_layout::Layout;
use containerregistry_registry::remote::{self, Options};
use containerregistry_registry::{Client, Reference};
use containerregistry_v1::{Image, ImageIndex, Taggable, tarball};

/// `--platform`: one platform, a comma-separated list (for
/// `index filter`), or `all`.
#[derive(Clone, Debug)]
pub enum PlatformFlag {
    All,
    One(Platform),
    Many(Vec<Platform>),
}

impl std::str::FromStr for PlatformFlag {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "all" {
            return Ok(PlatformFlag::All);
        }
        if s.contains(',') {
            return Ok(PlatformFlag::Many(parse_platforms(s)?));
        }
        Ok(PlatformFlag::One(s.parse().map_err(|e| {
            anyhow::anyhow!(
                "invalid platform {:?}: {} (expected os/arch[/variant][:osversion])",
                s,
                e
            )
        })?))
    }
}

/// Parses a comma-separated list of platforms.
pub fn parse_platforms(s: &str) -> Result<Vec<Platform>> {
    s.split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            p.parse()
                .map_err(|e| anyhow::anyhow!("invalid platform {:?}: {}", p, e))
        })
        .collect()
}

/// Everything commands need from the global flags.
pub struct Ctx {
    pub client: Client,
    pub platform: Option<PlatformFlag>,
    pub nondistributable: bool,
    pub jobs: usize,
}

impl Ctx {
    pub fn options(&self) -> Options {
        let mut o = Options::default().with_jobs(self.jobs);
        if let Some(PlatformFlag::One(p)) = &self.platform {
            o = o.with_platform(p.clone());
        }
        o
    }

    pub fn options_with_jobs(&self, jobs: usize) -> Options {
        self.options()
            .with_jobs(if jobs == 0 { self.jobs } else { jobs })
    }

    /// The `--platform` list (for `index filter`).
    pub fn platforms(&self) -> Vec<Platform> {
        match &self.platform {
            Some(PlatformFlag::One(p)) => vec![p.clone()],
            Some(PlatformFlag::Many(ps)) => ps.clone(),
            _ => Vec::new(),
        }
    }

    pub fn platform(&self) -> Option<&Platform> {
        match &self.platform {
            Some(PlatformFlag::One(p)) => Some(p),
            _ => None,
        }
    }

    /// Fetches an image, resolving an index to `--platform` (default
    /// linux/amd64), like go-containerregistry's `crane` does.
    pub async fn image(&self, r: &Reference) -> Result<Arc<dyn Image>> {
        if matches!(
            self.platform,
            Some(PlatformFlag::All | PlatformFlag::Many(_))
        ) {
            bail!("--platform must name a single platform for this command");
        }
        remote::image(&self.client, r, &self.options())
            .await
            .with_context(|| format!("fetching image {}", r))
    }

    /// Fetches an image or index. With a single `--platform`, an index is
    /// resolved to that platform's image.
    pub async fn taggable(&self, r: &Reference) -> Result<Taggable> {
        let d = remote::get(&self.client, r)
            .await
            .with_context(|| format!("fetching {}", r))?;
        Ok(match self.platform() {
            Some(p) if d.is_index() => Taggable::Image(d.image(p).await?),
            _ => d.taggable(),
        })
    }

    /// Fetches an index.
    pub async fn index(&self, r: &Reference) -> Result<Arc<dyn ImageIndex>> {
        remote::index(&self.client, r)
            .await
            .with_context(|| format!("fetching index {}", r))
    }

    /// Pushes an image or index and returns its digest.
    pub async fn push(&self, r: &Reference, t: &Taggable) -> Result<Digest> {
        self.check_distributable(t).await?;
        remote::push(&self.client, r, t, &self.options())
            .await
            .with_context(|| format!("pushing {}", r))?;
        Ok(t.digest().await?)
    }

    /// Refuses to upload non-distributable layers unless
    /// `--allow-nondistributable-artifacts` was given.
    async fn check_distributable(&self, t: &Taggable) -> Result<()> {
        if self.nondistributable {
            return Ok(());
        }
        let images: Vec<Arc<dyn Image>> = match t {
            Taggable::Image(i) => vec![i.clone()],
            Taggable::Index(_) => return Ok(()),
        };
        for img in images {
            if let Ok(m) = img.manifest().await
                && m.layers()
                    .iter()
                    .any(|l| l.media_type.is_nondistributable())
            {
                bail!(
                    "image has non-distributable (foreign) layers; pass --allow-nondistributable-artifacts to push them"
                );
            }
        }
        Ok(())
    }

    /// Publishes `img` to `dst`, or by digest to `fallback`'s repository
    /// when `dst` is empty; returns the reference by digest.
    pub async fn publish(
        &self,
        img: Arc<dyn Image>,
        dst: Option<&str>,
        fallback: &Reference,
    ) -> Result<Reference> {
        match dst {
            Some(t) => {
                let r = parse_ref(t)?;
                let d = self.push(&r, &Taggable::Image(img)).await?;
                Ok(r.with_new_digest(d))
            }
            None => {
                let d = remote::write_by_digest(&self.client, fallback, img, &self.options())
                    .await
                    .with_context(|| format!("pushing to {}", fallback))?;
                Ok(fallback.clone().with_new_digest(d))
            }
        }
    }

    /// Publishes an index to `dst` (or by digest to `fallback`'s repo).
    pub async fn publish_index(
        &self,
        idx: Arc<dyn ImageIndex>,
        dst: Option<&str>,
        fallback: &Reference,
    ) -> Result<Reference> {
        let d = idx.digest().await?;
        let r = match dst {
            Some(t) => parse_ref(t)?,
            None => fallback.clone().with_new_digest(d.clone()),
        };
        self.push(&r, &Taggable::Index(idx)).await?;
        Ok(r.with_new_digest(d))
    }
}

pub fn parse_ref(s: &str) -> Result<Reference> {
    s.parse()
        .with_context(|| format!("parsing reference {:?}", s))
}

/// The `repo:tag` name a tarball records for `r` (digest references get the
/// tag `i-was-a-digest`, like go-containerregistry).
pub fn tarball_tag(r: &Reference) -> String {
    let tag = r.tag().unwrap_or("i-was-a-digest");
    format!("{}/{}:{}", r.registry(), r.repository(), tag)
}

/// Loads an image or index from a `docker save` tarball or an OCI layout
/// directory (go `crane push`'s `loadImage`).
pub async fn load_local(path: &Path, as_index: bool) -> Result<Taggable> {
    if !path.is_dir() {
        let img = tarball::image_from_path(path, None)
            .await
            .with_context(|| format!("loading {} as tarball", path.display()))?;
        return Ok(Taggable::Image(img));
    }
    let layout =
        Layout::open(path).with_context(|| format!("opening layout {}", path.display()))?;
    let idx = layout.image_index_v1()?;
    if as_index {
        return Ok(Taggable::Index(idx));
    }
    let manifest = idx.index_manifest().await?;
    match manifest.manifests() {
        [] => bail!("layout {} contains no manifests", path.display()),
        [d] if d.media_type.is_index() => Ok(Taggable::Index(idx.image_index(&d.digest).await?)),
        [d] => Ok(Taggable::Image(idx.image(&d.digest).await?)),
        // go requires --index here; pushing the layout's index is the
        // only sensible interpretation, so do it.
        _ => Ok(Taggable::Index(idx)),
    }
}

/// Writes `data` to stdout without a trailing newline.
pub fn print_raw(data: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    out.write_all(data)?;
    out.flush()?;
    Ok(())
}

/// Whether a media type is an OCI (vs Docker) manifest type.
pub fn is_oci(mt: &MediaType) -> bool {
    matches!(mt, MediaType::OciManifest | MediaType::OciIndex)
}
