//! Listing APIs: tags, catalog, referrers, and GCR's extended tag listing.

use std::collections::BTreeMap;
use std::time::Instant;

use reqwest::StatusCode;
use reqwest::header::{ACCEPT, HeaderMap, LINK};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use containerregistry_image::{Digest, ImageIndex, MediaType, OciIndex};

use crate::client::{CATALOG_SCOPE, Client, Target};
use crate::metrics::Operation;
use crate::reference::Reference;
use crate::{Error, Result};

/// Tag list response from the registry.
#[derive(Debug, Deserialize)]
struct TagList {
    /// Tags can be null for empty repositories on some registries.
    tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct Catalog {
    repositories: Option<Vec<String>>,
}

/// Image metadata from GCR / Artifact Registry's extended tags list
/// (go-containerregistry `google.ManifestInfo`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleManifestInfo {
    /// Size of the image in bytes (as a decimal string).
    #[serde(default)]
    pub image_size_bytes: String,
    /// Layer ID (legacy).
    #[serde(default)]
    pub layer_id: String,
    /// Media type of the manifest.
    #[serde(default)]
    pub media_type: String,
    /// Tags pointing at this manifest.
    #[serde(default)]
    pub tag: Vec<String>,
    /// Creation time, milliseconds since the epoch (decimal string).
    #[serde(default)]
    pub time_created_ms: String,
    /// Upload time, milliseconds since the epoch (decimal string).
    #[serde(default)]
    pub time_uploaded_ms: String,
}

/// GCR's extended tags list (go-containerregistry `google.Tags`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoogleTags {
    /// Child repositories.
    #[serde(default)]
    pub child: Vec<String>,
    /// Manifests keyed by digest.
    #[serde(default)]
    pub manifest: BTreeMap<String, GoogleManifestInfo>,
    /// Repository name.
    #[serde(default)]
    pub name: String,
    /// Tags.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Paging options for list operations.
#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    /// Page size (`n`); `None` lets the registry choose.
    pub page_size: Option<usize>,
    /// Start listing after this entry (`last`).
    pub last: Option<String>,
}

fn parse_link_next(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(LINK)?.to_str().ok()?;
    for part in value.split(',') {
        let part = part.trim();
        let (Some(start), Some(end)) = (part.find('<'), part.find('>')) else {
            continue;
        };
        if part.contains("rel=\"next\"") || part.contains("rel=next") {
            return Some(part[start + 1..end].to_string());
        }
    }
    None
}

impl Client {
    fn resolve_link(&self, registry: &str, link: &str) -> String {
        self.resolve_location(registry, link)
            .unwrap_or_else(|_| link.to_string())
    }

    /// Lists tags for a repository, following pagination.
    #[instrument(skip(self), level = "debug", fields(reference = %reference))]
    pub async fn list_tags(&self, reference: &Reference) -> Result<Vec<String>> {
        self.list_tags_with(reference, &ListOptions::default())
            .await
    }

    /// Lists tags with paging options, following `Link` headers.
    pub async fn list_tags_with(
        &self,
        reference: &Reference,
        options: &ListOptions,
    ) -> Result<Vec<String>> {
        let start = Instant::now();
        let result = async {
            let mut url = self.url(
                reference.registry(),
                &format!("/{}/tags/list", reference.repository()),
            );
            let mut query = Vec::new();
            if let Some(n) = options.page_size {
                query.push(("n", n.to_string()));
            }
            if let Some(last) = &options.last {
                query.push(("last", last.clone()));
            }
            let mut tags = Vec::new();
            let mut first = true;
            loop {
                let q = if first { query.clone() } else { Vec::new() };
                let resp = self
                    .send(
                        &Target::pull(reference),
                        Some(Operation::ListTags),
                        true,
                        || self.http.get(&url).query(&q),
                    )
                    .await?;
                if resp.status() != StatusCode::OK {
                    return Err(match resp.status() {
                        StatusCode::NOT_FOUND => {
                            Error::NotFound(reference.repository().to_string())
                        }
                        _ => {
                            self.error_from_response(resp, "list tags", &reference.to_string())
                                .await
                        }
                    });
                }
                let next = parse_link_next(resp.headers())
                    .map(|l| self.resolve_link(reference.registry(), &l));
                let body: TagList = resp.json().await.map_err(|e| self.map_transport_error(e))?;
                tags.extend(body.tags.unwrap_or_default());
                first = false;
                match next {
                    Some(n) => url = n,
                    None => return Ok(tags),
                }
            }
        }
        .await;
        self.observe(Operation::ListTags, start, &result, 0);
        result
    }

    /// Lists the repositories in a registry (`/v2/_catalog`), following
    /// pagination.
    pub async fn catalog(&self, registry: &str) -> Result<Vec<String>> {
        self.catalog_with(registry, &ListOptions::default()).await
    }

    /// Lists repositories with paging options.
    pub async fn catalog_with(&self, registry: &str, options: &ListOptions) -> Result<Vec<String>> {
        let start = Instant::now();
        let result = async {
            let mut url = self.url(registry, "/_catalog");
            let mut query = Vec::new();
            if let Some(n) = options.page_size {
                query.push(("n", n.to_string()));
            }
            if let Some(last) = &options.last {
                query.push(("last", last.clone()));
            }
            let target = Target::registry(registry).with_scope(CATALOG_SCOPE.to_string());
            let mut repos = Vec::new();
            let mut first = true;
            loop {
                let q = if first { query.clone() } else { Vec::new() };
                let resp = self
                    .send(&target, Some(Operation::Catalog), true, || {
                        self.http.get(&url).query(&q)
                    })
                    .await?;
                if resp.status() != StatusCode::OK {
                    return Err(self.error_from_response(resp, "catalog", registry).await);
                }
                let next = parse_link_next(resp.headers()).map(|l| self.resolve_link(registry, &l));
                let body: Catalog = resp.json().await.map_err(|e| self.map_transport_error(e))?;
                repos.extend(body.repositories.unwrap_or_default());
                first = false;
                match next {
                    Some(n) => url = n,
                    None => return Ok(repos),
                }
            }
        }
        .await;
        self.observe(Operation::Catalog, start, &result, 0);
        result
    }

    /// Gets referrers for the given subject digest from the referrers API.
    ///
    /// See [`Client::referrers`] for the variant with the tag-schema
    /// fallback and artifact-type filtering.
    pub async fn get_referrers(
        &self,
        reference: &Reference,
        digest: &Digest,
    ) -> Result<ImageIndex> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/referrers/{}", reference.repository(), digest),
        );
        let resp = self
            .send(&Target::pull(reference), None, true, || {
                self.http
                    .get(&url)
                    .header(ACCEPT, "application/vnd.oci.image.index.v1+json")
            })
            .await?;
        match resp.status() {
            StatusCode::OK => {
                let body = resp
                    .bytes()
                    .await
                    .map_err(|e| self.map_transport_error(e))?;
                ImageIndex::from_bytes_unchecked(&body, Some(&MediaType::OciIndex))
                    .map_err(Error::from)
            }
            _ => Err(self
                .error_from_response(resp, "get referrers", &digest.to_string())
                .await),
        }
    }

    /// Lists manifests that refer to `digest` (OCI 1.1 referrers).
    ///
    /// Uses the referrers API and falls back to the referrers tag schema
    /// (`<alg>-<hex>` tag) when the registry does not implement it, like
    /// go-containerregistry. When `artifact_type` is set, results are
    /// filtered client-side if the registry did not filter them.
    pub async fn referrers(
        &self,
        reference: &Reference,
        digest: &Digest,
        artifact_type: Option<&MediaType>,
    ) -> Result<ImageIndex> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/referrers/{}", reference.repository(), digest),
        );
        let query: Vec<(&str, String)> = artifact_type
            .map(|a| vec![("artifactType", a.as_str().to_string())])
            .unwrap_or_default();
        let resp = self
            .send(&Target::pull(reference), None, true, || {
                self.http
                    .get(&url)
                    .query(&query)
                    .header(ACCEPT, "application/vnd.oci.image.index.v1+json")
            })
            .await?;
        let (mut index, filtered) = match resp.status() {
            StatusCode::OK
                if resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|ct| {
                        ct.starts_with("application/vnd.oci.image.index.v1+json")
                    }) =>
            {
                let filtered = resp.headers().contains_key("oci-filters-applied");
                let body = resp
                    .bytes()
                    .await
                    .map_err(|e| self.map_transport_error(e))?;
                (
                    ImageIndex::from_bytes_unchecked(&body, Some(&MediaType::OciIndex))?,
                    filtered,
                )
            }
            StatusCode::OK | StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => {
                // Fallback: the referrers tag schema.
                let tag_ref = reference.clone().with_new_tag(referrers_tag(digest));
                match self.get_manifest_raw(&tag_ref).await {
                    Ok(raw) => (raw.parse()?.into_index().unwrap_or_else(empty_index), false),
                    Err(Error::NotFound(_)) => (empty_index(), true),
                    Err(e) => return Err(e),
                }
            }
            _ => {
                return Err(self
                    .error_from_response(resp, "get referrers", &digest.to_string())
                    .await);
            }
        };
        if let Some(at) = artifact_type
            && !filtered
        {
            index
                .manifests_mut()
                .retain(|d| d.artifact_type.as_ref() == Some(at));
        }
        Ok(index)
    }

    /// Lists a repository with GCR's extended tag response, which includes
    /// child repositories and per-manifest metadata (go-containerregistry
    /// `google.List`).
    pub async fn google_list(&self, reference: &Reference) -> Result<GoogleTags> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/tags/list", reference.repository()),
        );
        let resp = self
            .send(
                &Target::pull(reference),
                Some(Operation::ListTags),
                true,
                || self.http.get(&url),
            )
            .await?;
        if resp.status() != StatusCode::OK {
            return Err(self
                .error_from_response(resp, "list tags", &reference.to_string())
                .await);
        }
        resp.json().await.map_err(|e| self.map_transport_error(e))
    }
}

/// The fallback tag for referrers of `digest` (`sha256-<hex>`).
pub fn referrers_tag(digest: &Digest) -> String {
    format!("{}-{}", digest.algorithm(), digest.hex())
}

fn empty_index() -> ImageIndex {
    ImageIndex::Oci(OciIndex::new(Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn test_parse_link_next() {
        let mut h = HeaderMap::new();
        h.insert(
            LINK,
            HeaderValue::from_static("</v2/_catalog?last=b&n=2>; rel=\"next\""),
        );
        assert_eq!(
            parse_link_next(&h).as_deref(),
            Some("/v2/_catalog?last=b&n=2")
        );
        h.insert(LINK, HeaderValue::from_static("<x>; rel=\"prev\""));
        assert_eq!(parse_link_next(&h), None);
    }

    #[test]
    fn test_referrers_tag() {
        let d = Digest::sha256(b"x");
        assert_eq!(referrers_tag(&d), format!("sha256-{}", d.hex()));
    }
}
