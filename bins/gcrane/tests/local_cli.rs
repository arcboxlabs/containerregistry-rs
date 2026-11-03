//! gcrane against `gcrane registry serve` (a crane command gcrane
//! delegates), covering recursive copy, ls and gc on a registry without
//! GCR extensions.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};

struct Server(Child, String);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn serve() -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_gcrane"))
        .args(["registry", "serve", "--address", "127.0.0.1:0"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut line = String::new();
    stderr.read_line(&mut line).unwrap();
    std::thread::spawn(move || std::io::copy(&mut stderr, &mut std::io::sink()));
    let port = line.trim().rsplit(' ').next().unwrap().to_string();
    Server(child, format!("127.0.0.1:{port}"))
}

fn gcrane(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_gcrane"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "gcrane {:?}: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn recursive_copy_ls_gc_and_crane_delegation() {
    let srv = serve();
    let r = srv.1.as_str();
    let dir = tempfile::tempdir().unwrap();
    let layer = dir.path().join("l.tar");
    let mut b = tar::Builder::new(std::fs::File::create(&layer).unwrap());
    let mut h = tar::Header::new_gnu();
    h.set_size(1);
    b.append_data(&mut h, "f", &b"x"[..]).unwrap();
    b.finish().unwrap();
    let layer = layer.to_str().unwrap();

    // crane commands work through gcrane, global flags included.
    gcrane(&["append", "-f", layer, "-t", &format!("{r}/src/a:v1")]);
    gcrane(&[
        "append",
        "-f",
        layer,
        "--oci-empty-base",
        "-t",
        &format!("{r}/src/b/c:v2"),
    ]);
    assert_eq!(
        gcrane(&["ls", &format!("{r}/src/a")]),
        format!("{r}/src/a:v1\n")
    );
    assert!(
        gcrane(&[
            "--platform",
            "linux/amd64",
            "digest",
            &format!("{r}/src/a:v1")
        ])
        .starts_with("sha256:")
    );

    gcrane(&[
        "cp",
        "-r",
        "-j",
        "2",
        &format!("{r}/src"),
        &format!("{r}/dst"),
    ]);
    assert_eq!(
        gcrane(&["ls", &format!("{r}/dst/a")]),
        format!("{r}/dst/a:v1\n")
    );
    assert_eq!(
        gcrane(&["ls", &format!("{r}/dst/b/c")]),
        format!("{r}/dst/b/c:v2\n")
    );
    assert_eq!(
        gcrane(&["digest", &format!("{r}/dst/b/c:v2")]),
        gcrane(&["digest", &format!("{r}/src/b/c:v2")])
    );
    let listed = gcrane(&["ls", "-r", "--json", &format!("{r}/dst")]);
    assert_eq!(listed.lines().count(), 2);
    assert!(gcrane(&["gc", "-r", &format!("{r}/dst")]).is_empty());
}
