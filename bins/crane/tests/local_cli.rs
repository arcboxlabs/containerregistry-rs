//! End-to-end CLI tests against `crane registry serve` (no external
//! services needed).

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

use serde_json::Value;

pub struct Server {
    child: Child,
    pub addr: String,
}

impl Server {
    pub fn start() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_crane"))
            .args(["registry", "serve", "--address", "127.0.0.1:0"])
            .stderr(Stdio::piped())
            .spawn()
            .expect("start registry");
        let mut line = String::new();
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        stderr.read_line(&mut line).unwrap();
        // Keep draining the request log so the server never blocks or
        // hits a closed pipe.
        std::thread::spawn(move || std::io::copy(&mut stderr, &mut std::io::sink()));
        let port = line.trim().rsplit(' ').next().unwrap().to_string();
        assert!(line.starts_with("serving on port"), "{}", line);
        Server {
            child,
            addr: format!("127.0.0.1:{}", port),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

fn crane_with(args: &[&str], stdin: Option<&[u8]>, envs: &[(&str, &Path)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_crane"));
    cmd.args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("run crane");
    {
        let mut si = child.stdin.take().unwrap();
        if let Some(data) = stdin {
            si.write_all(data).unwrap();
        }
    }
    child.wait_with_output().unwrap()
}

fn ok(args: &[&str]) -> String {
    ok_stdin(args, None)
}

fn ok_stdin(args: &[&str], stdin: Option<&[u8]>) -> String {
    let out = crane_with(args, stdin, &[]);
    assert!(
        out.status.success(),
        "crane {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn json(args: &[&str]) -> Value {
    serde_json::from_str(&ok(args)).unwrap()
}

fn layer_tar(dir: &Path, name: &str, file: &str, content: &str) -> String {
    let path = dir.join(name);
    let mut b = tar::Builder::new(std::fs::File::create(&path).unwrap());
    let mut h = tar::Header::new_gnu();
    h.set_size(content.len() as u64);
    h.set_mode(0o644);
    b.append_data(&mut h, file, content.as_bytes()).unwrap();
    b.finish().unwrap();
    path.to_str().unwrap().to_string()
}

fn tar_entries(data: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut a = tar::Archive::new(data);
    a.entries()
        .unwrap()
        .map(|e| {
            let mut e = e.unwrap();
            let n = e.path().unwrap().to_string_lossy().into_owned();
            let mut v = Vec::new();
            std::io::Read::read_to_end(&mut e, &mut v).unwrap();
            (n, v)
        })
        .collect()
}

#[test]
fn crane_commands_end_to_end() {
    let srv = Server::start();
    let r = srv.addr.as_str();
    let dir = tempfile::tempdir().unwrap();
    let l1 = layer_tar(dir.path(), "l1.tar", "etc/motd", "hello");
    let l2 = layer_tar(dir.path(), "l2.tar", "bin/app", "app");

    // append, manifest, config, digest, tag, ls, catalog
    let app = format!("{r}/test/app:v1");
    let pushed = ok(&[
        "append",
        "-f",
        &l1,
        "-f",
        &l2,
        "--oci-empty-base",
        "-t",
        &app,
    ]);
    let digest = ok(&["digest", &app]).trim().to_string();
    assert_eq!(pushed.trim(), format!("{r}/test/app@{digest}"));
    let m = json(&["manifest", &app]);
    assert_eq!(m["mediaType"], "application/vnd.oci.image.manifest.v1+json");
    assert_eq!(m["layers"].as_array().unwrap().len(), 2);
    assert_eq!(
        m["layers"][0]["mediaType"],
        "application/vnd.oci.image.layer.v1.tar+gzip"
    );
    let cfg = json(&["config", &app]);
    assert_eq!(cfg["rootfs"]["diff_ids"].as_array().unwrap().len(), 2);
    ok(&["tag", &app, "v2"]);
    assert_eq!(ok(&["ls", &format!("{r}/test/app")]), "v1\nv2\n");
    assert_eq!(
        ok(&["ls", "--full-ref", &format!("{r}/test/app")]),
        format!("{r}/test/app:v1\n{r}/test/app:v2\n")
    );
    assert_eq!(ok(&["catalog", r]), "test/app\n");

    // blob streams the exact bytes
    let cfg_digest = m["config"]["digest"].as_str().unwrap();
    let blob = ok(&["blob", &format!("{r}/test/app@{cfg_digest}")]);
    assert_eq!(serde_json::from_str::<Value>(&blob).unwrap(), cfg);

    // mutate
    let mutated = format!("{r}/test/app:mut");
    ok(&[
        "mutate",
        &app,
        "-l",
        "k=v",
        "-e",
        "A=1",
        "--entrypoint",
        "/bin/app",
        "-u",
        "nobody",
        "-w",
        "/w",
        "--exposed-ports",
        "8080",
        "--set-platform",
        "linux/arm64/v8",
        "-a",
        "note=x",
        "-t",
        &mutated,
    ]);
    let mc = json(&["config", &mutated]);
    assert_eq!(mc["architecture"], "arm64");
    assert_eq!(mc["variant"], "v8");
    assert_eq!(mc["config"]["Labels"]["k"], "v");
    assert_eq!(mc["config"]["Env"][0], "A=1");
    assert_eq!(mc["config"]["Entrypoint"][0], "/bin/app");
    assert_eq!(mc["config"]["User"], "nobody");
    assert!(mc["config"]["ExposedPorts"]["8080/tcp"].is_object());
    assert_eq!(json(&["manifest", &mutated])["annotations"]["note"], "x");

    // flatten + export
    let flat = format!("{r}/test/app:flat");
    ok(&["flatten", &app, "-t", &flat]);
    assert_eq!(
        json(&["manifest", &flat])["layers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let out = crane_with(&["export", &flat, "-"], None, &[]);
    let mut names: Vec<_> = tar_entries(&out.stdout).into_iter().map(|e| e.0).collect();
    names.sort();
    assert_eq!(names, vec!["bin/app", "etc/motd"]);
    ok(&["validate", "--remote", &flat]);

    // edit config and fs from stdin
    let edited = format!("{r}/test/app:edited");
    let new_cfg = serde_json::to_vec(&{
        let mut c = cfg.clone();
        c["author"] = "me".into();
        c
    })
    .unwrap();
    ok_stdin(&["edit", "config", &app, "-t", &edited], Some(&new_cfg));
    assert_eq!(json(&["config", &edited])["author"], "me");
    let fs = format!("{r}/test/app:fs");
    ok_stdin(
        &["edit", "fs", &app, "-f", "/etc/motd", "-t", &fs],
        Some(b"bye"),
    );
    let out = crane_with(&["export", &fs, "-"], None, &[]);
    let motd = tar_entries(&out.stdout)
        .into_iter()
        .find(|e| e.0 == "etc/motd")
        .unwrap();
    assert_eq!(motd.1, b"bye");

    // copy, copy -a, no-clobber
    ok(&["copy", &app, &format!("{r}/copy/app:v1")]);
    assert_eq!(ok(&["digest", &format!("{r}/copy/app:v1")]).trim(), digest);
    ok(&[
        "copy",
        "-a",
        &format!("{r}/test/app"),
        &format!("{r}/all/app"),
    ]);
    assert_eq!(
        ok(&["ls", &format!("{r}/all/app")]),
        ok(&["ls", &format!("{r}/test/app")])
    );
    let out = crane_with(
        &["copy", "-n", &mutated, &format!("{r}/copy/app:v1")],
        None,
        &[],
    );
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("refusing to clobber"));

    // index append / filter / platform selection
    let amd = format!("{r}/test/app:amd64");
    ok(&["mutate", &app, "--set-platform", "linux/amd64", "-t", &amd]);
    let idx = format!("{r}/test/idx:multi");
    ok(&["index", "append", "-m", &mutated, "-m", &amd, "-t", &idx]);
    let im = json(&["manifest", &idx]);
    assert_eq!(im["manifests"].as_array().unwrap().len(), 2);
    assert_eq!(
        ok(&["--platform", "linux/arm64", "digest", &idx]).trim(),
        ok(&["digest", &mutated]).trim()
    );
    let arm = format!("{r}/test/idx:arm");
    ok(&[
        "--platform",
        "linux/arm64",
        "index",
        "filter",
        &idx,
        "-t",
        &arm,
    ]);
    let am = json(&["manifest", &arm]);
    assert_eq!(am["manifests"].as_array().unwrap().len(), 1);
    assert_eq!(am["manifests"][0]["platform"]["architecture"], "arm64");
    ok(&["validate", "--remote", &idx]);

    // pull (tarball, legacy, oci) and push back
    let tb = dir.path().join("img.tar");
    ok(&["pull", &app, tb.to_str().unwrap()]);
    ok(&["validate", "--tarball", tb.to_str().unwrap()]);
    // Like go-containerregistry, tarball images carry Docker manifests, so
    // the OCI image's config survives but its manifest digest changes.
    let tb_digest = ok(&["digest", "--tarball", tb.to_str().unwrap()])
        .trim()
        .to_string();
    let pushed = ok(&["push", tb.to_str().unwrap(), &format!("{r}/pushed/tar:v1")]);
    assert_eq!(pushed.trim(), format!("{r}/pushed/tar@{tb_digest}"));
    assert_eq!(json(&["config", &format!("{r}/pushed/tar:v1")]), cfg);
    let legacy = dir.path().join("legacy.tar");
    ok(&["pull", "--format", "legacy", &app, legacy.to_str().unwrap()]);
    let names: Vec<_> = tar_entries(&std::fs::read(&legacy).unwrap())
        .into_iter()
        .map(|e| e.0)
        .collect();
    assert!(names.contains(&"repositories".to_string()));
    assert_eq!(
        names.iter().filter(|n| n.ends_with("/layer.tar")).count(),
        2
    );
    let layout = dir.path().join("layout");
    ok(&[
        "pull",
        "--format",
        "oci",
        "--annotate-ref",
        &idx,
        layout.to_str().unwrap(),
    ]);
    let pushed = ok(&[
        "push",
        layout.to_str().unwrap(),
        &format!("{r}/pushed/idx:v1"),
    ]);
    assert!(pushed.trim().ends_with(ok(&["digest", &idx]).trim()));
    ok(&["layout", "gc", layout.to_str().unwrap()]);

    // rebase using base image annotations
    let b1 = format!("{r}/base:one");
    let b2 = format!("{r}/base:two");
    ok(&["append", "-f", &l1, "--oci-empty-base", "-t", &b1]);
    ok(&[
        "append",
        "-f",
        &l2,
        "--oci-empty-base",
        "-t",
        &b1.replace("one", "two"),
    ]);
    let child = format!("{r}/child:v1");
    let l3 = layer_tar(dir.path(), "l3.tar", "top", "t");
    ok(&[
        "append",
        "-b",
        &b1,
        "-f",
        &l3,
        "--set-base-image-annotations",
        "-t",
        &child,
    ]);
    let rebased = format!("{r}/child:rebased");
    ok(&["rebase", &child, "--new_base", &b2, "-t", &rebased]);
    let rl: Vec<String> = json(&["manifest", &rebased])["layers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["digest"].as_str().unwrap().to_string())
        .collect();
    let b2l = json(&["manifest", &b2])["layers"][0]["digest"]
        .as_str()
        .unwrap()
        .to_string();
    let cl = json(&["manifest", &child])["layers"][1]["digest"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(rl, vec![b2l, cl]);
    ok(&["validate", "--remote", &rebased]);

    // delete
    ok(&["delete", &format!("{r}/test/app:v2")]);
    assert!(!ok(&["ls", &format!("{r}/test/app")]).contains("v2"));
}

#[test]
fn crane_auth_login_get_logout() {
    let dir = tempfile::tempdir().unwrap();
    let env = [("DOCKER_CONFIG", dir.path())];
    let out = crane_with(
        &[
            "auth",
            "login",
            "example.com",
            "-u",
            "user",
            "--password-stdin",
        ],
        Some(b"s3cret\n"),
        &env,
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = crane_with(&["auth", "get", "example.com"], None, &env);
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["Username"], "user");
    assert_eq!(v["Secret"], "s3cret");
    // Credential helper protocol: registry on stdin.
    let out = crane_with(&["auth", "get"], Some(b"example.com\n"), &env);
    assert!(out.status.success());
    let out = crane_with(&["auth", "logout", "example.com"], None, &env);
    assert!(out.status.success());
    let out = crane_with(&["auth", "get", "example.com"], None, &env);
    assert!(!out.status.success());
}
