//! Descriptor matchers (go-containerregistry `pkg/v1/match`).

use std::collections::HashSet;
use std::sync::Arc;

use containerregistry_image::{Descriptor, Digest, Platform};

/// The annotation holding a reference name in OCI layouts.
pub const ANNOTATION_REF_NAME: &str = "org.opencontainers.image.ref.name";

/// A predicate over descriptors.
pub type Matcher = Arc<dyn Fn(&Descriptor) -> bool + Send + Sync>;

/// Matches the `org.opencontainers.image.ref.name` annotation.
pub fn name(name: impl Into<String>) -> Matcher {
    annotation(ANNOTATION_REF_NAME, name)
}

/// Matches descriptors carrying annotation `key` with `value`.
pub fn annotation(key: impl Into<String>, value: impl Into<String>) -> Matcher {
    let key = key.into();
    let value = value.into();
    Arc::new(move |d| d.annotations.get(&key) == Some(&value))
}

/// Matches descriptors whose platform equals one of `platforms`
/// (descriptors without a platform never match).
pub fn platforms(platforms: Vec<Platform>) -> Matcher {
    Arc::new(move |d| {
        d.platform
            .as_ref()
            .is_some_and(|p| platforms.iter().any(|want| p.equals(want)))
    })
}

/// Matches descriptors whose platform satisfies one of `specs` (empty fields
/// in a spec match anything). This is the matching `crane --platform` uses.
pub fn platforms_satisfying(specs: Vec<Platform>) -> Matcher {
    Arc::new(move |d| {
        d.platform
            .as_ref()
            .is_some_and(|p| specs.iter().any(|spec| p.satisfies(spec)))
    })
}

/// Matches any of the given media types.
pub fn media_types<I, S>(media_types: I) -> Matcher
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let set: HashSet<String> = media_types.into_iter().map(Into::into).collect();
    Arc::new(move |d| set.contains(d.media_type.as_str()))
}

/// Matches any of the given digests.
pub fn digests(digests: impl IntoIterator<Item = Digest>) -> Matcher {
    let set: HashSet<Digest> = digests.into_iter().collect();
    Arc::new(move |d| set.contains(&d.digest))
}

/// Negates a matcher.
pub fn not(m: Matcher) -> Matcher {
    Arc::new(move |d| !m(d))
}

/// Matches when any matcher matches.
pub fn any(matchers: Vec<Matcher>) -> Matcher {
    Arc::new(move |d| matchers.iter().any(|m| m(d)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use containerregistry_image::MediaType;

    fn desc() -> Descriptor {
        Descriptor::from_bytes(MediaType::OciManifest, b"x")
            .with_annotation(ANNOTATION_REF_NAME, "v1")
            .with_platform(Platform::new("arm64", "linux").with_variant("v8"))
    }

    #[test]
    fn matchers() {
        let d = desc();
        assert!(name("v1")(&d));
        assert!(!name("v2")(&d));
        assert!(platforms(vec![
            Platform::new("arm64", "linux").with_variant("v8")
        ])(&d));
        assert!(!platforms(vec![Platform::new("arm64", "linux")])(&d));
        assert!(platforms_satisfying(vec![Platform::new("arm64", "linux")])(
            &d
        ));
        assert!(media_types([MediaType::OciManifest.as_str()])(&d));
        assert!(digests([Digest::sha256(b"x")])(&d));
        assert!(!not(digests([Digest::sha256(b"x")]))(&d));
    }
}
