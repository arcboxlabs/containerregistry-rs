//! Tar helpers shared by mutate, tarball and validate.

use std::io::{self, Read, Write};
use std::path::Path;

use tokio::runtime::Handle;
use tokio_util::io::SyncIoBridge;

use crate::layer::BoxReader;

/// Converts an async reader into a blocking one. Must only be read from a
/// blocking thread (e.g. inside `spawn_blocking`).
pub(crate) fn sync_reader(reader: BoxReader, handle: Handle) -> impl Read + Send {
    SyncIoBridge::new_with_handle(reader, handle)
}

/// Lexically cleans a slash-separated path like Go's `path.Clean`.
pub(crate) fn clean_path(p: &str) -> String {
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in p.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{}", joined),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// Go's `path.Dir`.
pub(crate) fn dir(p: &str) -> String {
    match p.rfind('/') {
        Some(i) => clean_path(&p[..i + 1]),
        None => ".".to_string(),
    }
}

/// Go's `path.Base` for cleaned, non-empty paths.
pub(crate) fn base(p: &str) -> &str {
    let t = p.trim_end_matches('/');
    match t.rfind('/') {
        Some(i) => &t[i + 1..],
        None => t,
    }
}

/// Go's `path.Join` of two elements.
pub(crate) fn join(a: &str, b: &str) -> String {
    if a.is_empty() {
        return clean_path(b);
    }
    clean_path(&format!("{}/{}", a, b))
}

fn has_windows_drive_prefix(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':'
}

/// Paths that would escape the extraction root.
pub(crate) fn unsafe_archive_path(name: &str) -> bool {
    let clean = clean_path(&name.replace('\\', "/"));
    clean == ".."
        || clean.starts_with("../")
        || name.starts_with('\\')
        || has_windows_drive_prefix(&clean)
}

/// Relative link targets that resolve outside the root.
pub(crate) fn unsafe_relative_link(name: &str, link: &str) -> bool {
    let clean = clean_path(&link.replace('\\', "/"));
    if clean.starts_with('/') {
        return false;
    }
    if has_windows_drive_prefix(&clean) {
        return true;
    }
    clean_path(&join(&dir(name), &clean)).starts_with("..")
}

/// Appends an entry, re-encoding long names and link targets as needed.
pub(crate) fn append_entry<W: Write, R: Read>(
    builder: &mut tar::Builder<W>,
    mut header: tar::Header,
    path: &str,
    link: Option<&Path>,
    data: &mut R,
) -> io::Result<()> {
    let et = header.entry_type();
    match link {
        Some(target) if et.is_symlink() || et.is_hard_link() => {
            header.set_size(0);
            builder.append_link(&mut header, path, target)
        }
        _ => builder.append_data(&mut header, path, data),
    }
}

/// Lossy UTF-8 path of an entry.
pub(crate) fn entry_path<R: Read>(entry: &tar::Entry<'_, R>) -> io::Result<String> {
    Ok(entry.path()?.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_path_semantics() {
        assert_eq!(clean_path("./a/b/../c/"), "a/c");
        assert_eq!(clean_path("../x"), "../x");
        assert_eq!(clean_path("/../x"), "/x");
        assert_eq!(clean_path(""), ".");
        assert_eq!(dir("a/b/c"), "a/b");
        assert_eq!(dir("c"), ".");
        assert_eq!(base("a/b/.wh.c"), ".wh.c");
        assert!(unsafe_archive_path("../etc/passwd"));
        assert!(unsafe_archive_path("C:/x"));
        assert!(!unsafe_archive_path("a/../b"));
        assert!(unsafe_relative_link("a/l", "../../x"));
        assert!(!unsafe_relative_link("a/b/l", "../x"));
        assert!(!unsafe_relative_link("l", "/etc/x"));
    }
}
