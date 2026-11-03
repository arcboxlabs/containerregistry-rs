//! Manifests, tags, catalog and referrers.

use std::collections::{BTreeMap, HashMap};

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use containerregistry_image::Digest;
use serde_json::{Map, Value, json};

use crate::error::RegistryError;
use crate::handlers::{Inner, json_response, paginate};

/// Largest manifest accepted, as recommended by the distribution spec.
const MAX_MANIFEST_SIZE: usize = 4 << 20;

const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";

/// Media types whose descriptors (inside an index) must name a manifest that
/// already exists in the repository, as go-containerregistry checks.
const MANIFEST_TYPES: &[&str] = &[
    "application/vnd.oci.image.manifest.v1+json",
    OCI_INDEX,
    "application/vnd.docker.distribution.manifest.v2+json",
    DOCKER_LIST,
    "application/vnd.docker.distribution.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v1+prettyjws",
];

pub(crate) struct StoredManifest {
    content_type: String,
    data: Bytes,
    /// Digest of the `subject` descriptor, if any.
    subject: Option<String>,
    /// `artifactType`, or the config media type for images without one.
    artifact_type: Option<String>,
    annotations: Option<Map<String, Value>>,
}

/// A repository: manifests keyed by digest, and tags pointing at digests.
#[derive(Default)]
pub(crate) struct Repo {
    manifests: HashMap<String, StoredManifest>,
    tags: BTreeMap<String, String>,
}

fn parse_digest(s: &str) -> Result<Digest, RegistryError> {
    s.parse()
        .map_err(|e| RegistryError::digest_invalid(format!("invalid digest {s:?}: {e}")))
}

/// Tag grammar from the distribution spec.
fn valid_tag(tag: &str) -> bool {
    let bytes = tag.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

impl Repo {
    /// Resolves a tag or digest to the digest key of a stored manifest.
    fn resolve(&self, reference: &str) -> Result<String, RegistryError> {
        let key = if reference.contains(':') {
            parse_digest(reference)?.to_string()
        } else {
            self.tags
                .get(reference)
                .cloned()
                .ok_or_else(|| RegistryError::manifest_unknown(reference))?
        };
        if self.manifests.contains_key(&key) {
            Ok(key)
        } else {
            Err(RegistryError::manifest_unknown(reference))
        }
    }
}

pub(crate) fn get(
    state: &Inner,
    name: &str,
    reference: &str,
    head: bool,
) -> Result<Response, RegistryError> {
    let repos = state.repos.read().expect("manifest lock");
    let repo = repos
        .get(name)
        .ok_or_else(|| RegistryError::name_unknown(name))?;
    let key = repo.resolve(reference)?;
    let m = &repo.manifests[&key];
    let body = if head {
        Body::empty()
    } else {
        Body::from(m.data.clone())
    };
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("Docker-Content-Digest", &key)
        .header(header::CONTENT_LENGTH, m.data.len());
    if !m.content_type.is_empty() {
        builder = builder.header(header::CONTENT_TYPE, &m.content_type);
    }
    builder.body(body).map_err(RegistryError::internal)
}

pub(crate) async fn put(
    state: &Inner,
    name: &str,
    reference: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, RegistryError> {
    let data = axum::body::to_bytes(body, MAX_MANIFEST_SIZE)
        .await
        .map_err(|e| {
            RegistryError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "SIZE_INVALID",
                format!("reading manifest (limit {MAX_MANIFEST_SIZE} bytes): {e}"),
            )
        })?;
    let parsed: Option<Value> = serde_json::from_slice(&data).ok();
    let mut content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    if content_type.is_empty()
        && let Some(mt) = parsed
            .as_ref()
            .and_then(|v| v.get("mediaType"))
            .and_then(Value::as_str)
    {
        content_type = mt.to_string();
    }
    let is_index = content_type == OCI_INDEX || content_type == DOCKER_LIST;
    if parsed.is_none() && MANIFEST_TYPES.contains(&content_type.as_str()) {
        return Err(RegistryError::new(
            StatusCode::BAD_REQUEST,
            "MANIFEST_INVALID",
            "manifest is not valid JSON",
        ));
    }

    // Pushing by digest verifies it (with the digest's own algorithm); a tag
    // push is addressed by the sha256 of the bytes.
    let (key, tag) = if reference.contains(':') {
        let expected = parse_digest(reference)?;
        let actual = Digest::compute(expected.algorithm(), &data);
        if actual != expected {
            return Err(RegistryError::digest_invalid(format!(
                "manifest digest {actual} does not match {expected}"
            )));
        }
        (expected.to_string(), None)
    } else {
        if !valid_tag(reference) {
            return Err(RegistryError::new(
                StatusCode::BAD_REQUEST,
                "TAG_INVALID",
                format!("invalid tag {reference:?}"),
            ));
        }
        (
            Digest::sha256(&data).to_string(),
            Some(reference.to_string()),
        )
    };

    let field = |name: &str| parsed.as_ref().and_then(|v| v.get(name));
    let subject = field("subject")
        .and_then(|s| s.get("digest"))
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Digest>().ok())
        .map(|d| d.to_string());
    let artifact_type = field("artifactType")
        .and_then(Value::as_str)
        .or_else(|| {
            field("config")
                .and_then(|c| c.get("mediaType"))
                .and_then(Value::as_str)
        })
        .map(str::to_string);
    let annotations = field("annotations")
        .and_then(Value::as_object)
        .filter(|m| !m.is_empty())
        .cloned();

    let mut repos = state.repos.write().expect("manifest lock");
    if is_index {
        // Like go-containerregistry: an index's child manifests must exist.
        let empty = Repo::default();
        let repo = repos.get(name).unwrap_or(&empty);
        let children = field("manifests")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for child in children {
            let media_type = child.get("mediaType").and_then(Value::as_str);
            if !media_type.is_some_and(|mt| MANIFEST_TYPES.contains(&mt)) {
                continue;
            }
            let digest = child
                .get("digest")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let known = digest
                .parse::<Digest>()
                .is_ok_and(|d| repo.manifests.contains_key(&d.to_string()));
            if !known {
                return Err(RegistryError::new(
                    StatusCode::NOT_FOUND,
                    "MANIFEST_UNKNOWN",
                    format!("sub-manifest {digest:?} not found"),
                ));
            }
        }
    }
    let repo = repos.entry(name.to_string()).or_default();
    repo.manifests.insert(
        key.clone(),
        StoredManifest {
            content_type,
            data,
            subject: subject.clone(),
            artifact_type,
            annotations,
        },
    );
    if let Some(tag) = tag {
        repo.tags.insert(tag, key.clone());
    }
    drop(repos);

    let mut builder = Response::builder()
        .status(StatusCode::CREATED)
        .header(header::LOCATION, format!("/v2/{name}/manifests/{key}"))
        .header("Docker-Content-Digest", &key)
        .header(header::CONTENT_LENGTH, 0);
    if state.referrers
        && let Some(subject) = subject
    {
        builder = builder.header("OCI-Subject", subject);
    }
    builder.body(Body::empty()).map_err(RegistryError::internal)
}

/// Deletes a tag, or a manifest by digest (with any tags pointing at it).
pub(crate) fn delete(
    state: &Inner,
    name: &str,
    reference: &str,
) -> Result<Response, RegistryError> {
    let mut repos = state.repos.write().expect("manifest lock");
    let repo = repos
        .get_mut(name)
        .ok_or_else(|| RegistryError::name_unknown(name))?;
    if reference.contains(':') {
        let key = parse_digest(reference)?.to_string();
        if repo.manifests.remove(&key).is_none() {
            return Err(RegistryError::manifest_unknown(reference));
        }
        repo.tags.retain(|_, digest| *digest != key);
    } else if repo.tags.remove(reference).is_none() {
        return Err(RegistryError::manifest_unknown(reference));
    }
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(header::CONTENT_LENGTH, 0)
        .body(Body::empty())
        .map_err(RegistryError::internal)
}

pub(crate) fn tags(
    state: &Inner,
    name: &str,
    query: &HashMap<String, String>,
) -> Result<Response, RegistryError> {
    let tags: Vec<String> = {
        let repos = state.repos.read().expect("manifest lock");
        let repo = repos
            .get(name)
            .ok_or_else(|| RegistryError::name_unknown(name))?;
        repo.tags.keys().cloned().collect()
    };
    let (tags, link) = paginate(tags, query, &format!("/v2/{name}/tags/list"))?;
    json_response(&json!({ "name": name, "tags": tags }), link, &[])
}

pub(crate) fn catalog(
    state: &Inner,
    query: &HashMap<String, String>,
) -> Result<Response, RegistryError> {
    let repos: Vec<String> = state
        .repos
        .read()
        .expect("manifest lock")
        .keys()
        .cloned()
        .collect();
    let (repos, link) = paginate(repos, query, "/v2/_catalog")?;
    json_response(&json!({ "repositories": repos }), link, &[])
}

pub(crate) fn referrers(
    state: &Inner,
    name: &str,
    digest: &str,
    query: &HashMap<String, String>,
) -> Result<Response, RegistryError> {
    let subject = parse_digest(digest)?.to_string();
    let filter = query.get("artifactType").filter(|s| !s.is_empty()).cloned();
    let mut descriptors: Vec<(String, Value)> = Vec::new();
    {
        let repos = state.repos.read().expect("manifest lock");
        // An unknown repository has no referrers (an empty index, per spec).
        if let Some(repo) = repos.get(name) {
            for (key, m) in &repo.manifests {
                if m.subject.as_deref() != Some(subject.as_str()) {
                    continue;
                }
                if filter.is_some() && m.artifact_type != filter {
                    continue;
                }
                let mut desc = json!({
                    "mediaType": m.content_type,
                    "digest": key,
                    "size": m.data.len(),
                });
                if let Some(at) = &m.artifact_type {
                    desc["artifactType"] = json!(at);
                }
                if let Some(annotations) = &m.annotations {
                    desc["annotations"] = Value::Object(annotations.clone());
                }
                descriptors.push((key.clone(), desc));
            }
        }
    }
    descriptors.sort_by(|a, b| a.0.cmp(&b.0));
    let index = json!({
        "schemaVersion": 2,
        "mediaType": OCI_INDEX,
        "manifests": descriptors.into_iter().map(|(_, d)| d).collect::<Vec<_>>(),
    });
    let mut extra = vec![(header::CONTENT_TYPE.as_str(), OCI_INDEX)];
    if filter.is_some() {
        extra.push(("OCI-Filters-Applied", "artifactType"));
    }
    json_response(&index, None, &extra)
}
