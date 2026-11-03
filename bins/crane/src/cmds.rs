//! Command implementations.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{ArgAction, Args, Subcommand};
use futures::{StreamExt, TryStreamExt};
use tokio::io::AsyncWriteExt;

use containerregistry_auth::{Credential, DockerConfig, default_keychain};
use containerregistry_image::{EmptyObject, History, MediaType, Platform};
use containerregistry_layout::{AppendOptions, Layout};
use containerregistry_registry::Reference;
use containerregistry_v1::compression::Compression;
use containerregistry_v1::mutate::{self, Addendum, IndexAddendum};
use containerregistry_v1::partial::{Child, children};
use containerregistry_v1::stream::StreamLayer;
use containerregistry_v1::tarball::{self, LayerOptions};
use containerregistry_v1::{
    Image, ImageIndex, Layer, Taggable, cache, empty, matcher, validate as v,
};

use crate::util::{Ctx, is_oci, load_local, parse_ref, print_raw, tarball_tag};

const BASE_NAME: &str = "org.opencontainers.image.base.name";
const BASE_DIGEST: &str = "org.opencontainers.image.base.digest";

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected KEY=VALUE, got {:?}", s))
}

fn repo_tag(r: &Reference, tag: &str) -> Result<Reference> {
    parse_ref(&format!("{}/{}:{}", r.registry(), r.repository(), tag))
}

async fn layer_media_type(base: &dyn Image) -> MediaType {
    match base.media_type().await {
        Ok(mt) if is_oci(&mt) => MediaType::OciLayerGzip,
        _ => MediaType::DockerLayerGzip,
    }
}

async fn file_layers(paths: &[String], mt: MediaType) -> Result<Vec<Arc<dyn Layer>>> {
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        let opts = LayerOptions {
            media_type: Some(mt.clone()),
            compression: Compression::from_media_type(&mt),
            ..Default::default()
        };
        out.push(
            tarball::layer_from_file(p, opts)
                .await
                .with_context(|| format!("creating layer from {}", p))?,
        );
    }
    Ok(out)
}

fn oci_empty_image() -> Arc<dyn Image> {
    mutate::media_type(
        mutate::config_media_type(empty::image(), MediaType::OciConfig),
        MediaType::OciManifest,
    )
}

// ---------------------------------------------------------------------------
// append

#[derive(Args)]
pub struct AppendArgs {
    /// Name of base image to append to
    #[arg(short = 'b', long = "base")]
    base: Option<String>,
    /// Tag to apply to resulting image
    #[arg(short = 't', long = "new_tag", alias = "new-tag")]
    new_tag: Option<String>,
    /// Path to tarball to append to image
    #[arg(
        short = 'f',
        long = "new_layer",
        alias = "new-layer",
        value_delimiter = ',',
        required = true
    )]
    new_layer: Vec<String>,
    /// Path to new tarball of resulting image
    #[arg(short, long)]
    output: Option<String>,
    /// Annotate the resulting image as being based on the base image
    #[arg(long)]
    set_base_image_annotations: bool,
    /// Use OCI media types for an empty base image
    #[arg(long)]
    oci_empty_base: bool,
}

pub async fn append(ctx: &Ctx, a: AppendArgs) -> Result<()> {
    let (base, base_ref) = match &a.base {
        Some(b) => {
            let r = parse_ref(b)?;
            (ctx.image(&r).await?, Some(r))
        }
        None if a.oci_empty_base => (oci_empty_image(), None),
        None => (empty::image(), None),
    };
    let layers = file_layers(&a.new_layer, layer_media_type(base.as_ref()).await).await?;
    let mut img = mutate::append_layers(base.clone(), layers);
    if a.set_base_image_annotations
        && let Some(r) = &base_ref
    {
        let anns = BTreeMap::from([
            (BASE_NAME.to_string(), r.to_string()),
            (BASE_DIGEST.to_string(), base.digest().await?.to_string()),
        ]);
        img = mutate::annotations(img, anns);
    }
    if let Some(out) = &a.output {
        let tag = a
            .new_tag
            .clone()
            .unwrap_or_else(|| "image:latest".to_string());
        tarball::write_to_file(out, vec![(vec![tag], img)]).await?;
        return Ok(());
    }
    let tag = a
        .new_tag
        .as_deref()
        .context("--new_tag is required unless --output is set")?;
    let r = parse_ref(tag)?;
    let d = ctx.push(&r, &Taggable::Image(img)).await?;
    println!("{}", r.with_new_digest(d));
    Ok(())
}

// ---------------------------------------------------------------------------
// auth

#[derive(Subcommand)]
pub enum AuthCmd {
    /// Implements a credential helper: prints credentials for a registry
    Get {
        /// Registry (read from stdin when omitted)
        registry: Option<String>,
    },
    /// Log in to a registry
    Login {
        /// Registry host
        server: String,
        /// Username
        #[arg(short, long)]
        username: Option<String>,
        /// Password
        #[arg(short, long)]
        password: Option<String>,
        /// Take the password from stdin
        #[arg(long)]
        password_stdin: bool,
    },
    /// Log out of a registry
    Logout {
        /// Registry host
        server: String,
    },
    /// Retrieves a token for a remote repo
    Token {
        /// Repository
        repo: String,
        /// Scopes to mount from
        #[arg(short = 'm', long = "mount", value_delimiter = ',')]
        mount: Vec<String>,
        /// Output in header format
        #[arg(short = 'H', long)]
        header: bool,
        /// Request push scopes
        #[arg(long)]
        push: bool,
    },
}

fn registry_host(server: &str) -> String {
    let s = server
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let host = s.split('/').next().unwrap_or(s);
    if host == "docker.io" || host == "index.docker.io" || host == "registry-1.docker.io" {
        "index.docker.io".to_string()
    } else {
        host.to_string()
    }
}

pub async fn auth(ctx: &Ctx, cmd: AuthCmd) -> Result<()> {
    match cmd {
        AuthCmd::Get { registry } => {
            let registry = match registry {
                Some(r) => r,
                None => {
                    let mut s = String::new();
                    std::io::stdin().read_to_string(&mut s)?;
                    s.trim().to_string()
                }
            };
            let cred = default_keychain().resolve(&registry_host(&registry))?;
            let (user, secret) = match cred {
                Credential::Anonymous => {
                    print!("credentials not found in native keychain");
                    std::process::exit(1);
                }
                Credential::Basic { username, password } => (username, password),
                Credential::IdentityToken(t) => ("<token>".to_string(), t),
                Credential::Bearer(t) => (String::new(), t),
            };
            println!(
                "{}",
                serde_json::json!({ "Username": user, "Secret": secret })
            );
            Ok(())
        }
        AuthCmd::Login {
            server,
            username,
            password,
            password_stdin,
        } => {
            let password = if password_stdin {
                let mut s = String::new();
                std::io::stdin().read_to_string(&mut s)?;
                s.trim_end_matches(['\n', '\r']).to_string()
            } else {
                password.context("--password or --password-stdin is required")?
            };
            let username = username.context("--username is required")?;
            let path = DockerConfig::default_config_path()?;
            let mut cfg = DockerConfig::load_or_default(&path)?;
            cfg.store_credential(
                &registry_host(&server),
                &Credential::basic(username, password),
            )?;
            cfg.save_to(&path)?;
            eprintln!("logged in via {}", path.display());
            Ok(())
        }
        AuthCmd::Logout { server } => {
            let path = DockerConfig::default_config_path()?;
            let mut cfg = DockerConfig::load_or_default(&path)?;
            if cfg.erase_credential(&registry_host(&server))? {
                cfg.save_to(&path)?;
                eprintln!("logged out via {}", path.display());
            } else {
                eprintln!("not logged in to {}", server);
            }
            Ok(())
        }
        AuthCmd::Token {
            repo,
            mount,
            header,
            push,
        } => {
            let r = parse_ref(&repo)?;
            let value = ctx
                .client
                .authorization_header(&r, push, &mount)
                .await?
                .context("registry does not issue tokens for anonymous access")?;
            if header {
                println!("Authorization: {}", value);
            } else {
                let token = value
                    .strip_prefix("Bearer ")
                    .or_else(|| value.strip_prefix("Basic "))
                    .unwrap_or(&value);
                println!("{}", serde_json::json!({ "token": token }));
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// simple registry commands

pub async fn blob(ctx: &Ctx, blob: &str) -> Result<()> {
    let r = parse_ref(blob)?;
    let d = r
        .digest()
        .context("blob reference must include a digest")?
        .clone();
    let mut reader = ctx.client.get_blob_stream(&r, &d).await?;
    let mut out = tokio::io::stdout();
    tokio::io::copy(&mut reader, &mut out).await?;
    out.flush().await?;
    Ok(())
}

pub async fn catalog(ctx: &Ctx, registry: &str, full_ref: bool) -> Result<()> {
    for repo in ctx.client.catalog(registry).await? {
        if full_ref {
            println!("{}/{}", registry, repo);
        } else {
            println!("{}", repo);
        }
    }
    Ok(())
}

pub async fn config(ctx: &Ctx, image: &str) -> Result<()> {
    let img = ctx.image(&parse_ref(image)?).await?;
    print_raw(&img.raw_config_file().await?)
}

pub async fn delete(ctx: &Ctx, image: &str) -> Result<()> {
    ctx.client.delete_manifest(&parse_ref(image)?).await?;
    Ok(())
}

pub async fn digest(
    ctx: &Ctx,
    image: Option<&str>,
    tarball: Option<&str>,
    full_ref: bool,
) -> Result<()> {
    if let Some(path) = tarball {
        let img = tarball::image_from_path(path, image).await?;
        println!("{}", img.digest().await?);
        return Ok(());
    }
    let r = parse_ref(image.context("IMAGE is required")?)?;
    let d = ctx.taggable(&r).await?.digest().await?;
    if full_ref {
        println!("{}", r.with_new_digest(d));
    } else {
        println!("{}", d);
    }
    Ok(())
}

pub async fn ls(ctx: &Ctx, repository: &str, full_ref: bool, omit_digest_tags: bool) -> Result<()> {
    let r = parse_ref(repository)?;
    for tag in ctx.client.list_tags(&r).await? {
        if omit_digest_tags && is_digest_tag(&tag) {
            continue;
        }
        if full_ref {
            println!("{}/{}:{}", r.registry(), r.repository(), tag);
        } else {
            println!("{}", tag);
        }
    }
    Ok(())
}

fn is_digest_tag(tag: &str) -> bool {
    ["sha256-", "sha512-"].iter().any(|p| {
        tag.strip_prefix(p).is_some_and(|rest| {
            let hex: String = rest.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
            hex.len() >= 64
        })
    })
}

pub async fn manifest(ctx: &Ctx, image: &str) -> Result<()> {
    let t = ctx.taggable(&parse_ref(image)?).await?;
    print_raw(&t.raw_manifest().await?)
}

pub async fn tag(ctx: &Ctx, image: &str, tag: &str) -> Result<()> {
    ctx.client.tag(&parse_ref(image)?, tag).await?;
    Ok(())
}

pub async fn validate(
    ctx: &Ctx,
    tarball_path: Option<&str>,
    remote: Option<&str>,
    fast: bool,
) -> Result<()> {
    let opts = v::Options { fast };
    let (name, result) = match (tarball_path, remote) {
        (Some(p), _) => {
            let img = tarball::image_from_path(p, None).await?;
            (p.to_string(), v::image(img.as_ref(), opts).await)
        }
        (None, Some(r)) => match ctx.taggable(&parse_ref(r)?).await? {
            Taggable::Image(i) => (r.to_string(), v::image(i.as_ref(), opts).await),
            Taggable::Index(i) => (r.to_string(), v::index(i.as_ref(), opts).await),
        },
        (None, None) => bail!("one of --tarball or --remote is required"),
    };
    match result {
        Ok(()) => {
            println!("PASS: {}", name);
            Ok(())
        }
        Err(e) => {
            println!("FAIL: {}: {}", name, e);
            Err(e.into())
        }
    }
}

// ---------------------------------------------------------------------------
// copy

#[derive(Args)]
pub struct CopyArgs {
    /// Source image
    source: String,
    /// Destination image
    destination: String,
    /// Copy all tags from SRC to DST
    #[arg(short = 'a', long)]
    all_tags: bool,
    /// Avoid overwriting existing tags in DST
    #[arg(short = 'n', long)]
    no_clobber: bool,
    /// The maximum number of concurrent copies (default: number of CPUs)
    #[arg(short = 'j', long, default_value_t = 0)]
    jobs: usize,
}

async fn copy_one(
    ctx: &Ctx,
    src: &Reference,
    dst: &Reference,
    jobs: usize,
    no_clobber: bool,
) -> Result<()> {
    if no_clobber && dst.is_tag() && ctx.client.head_manifest(dst).await.is_ok() {
        bail!("refusing to clobber existing tag {}", dst);
    }
    let t = ctx.taggable(src).await?;
    containerregistry_registry::remote::push(&ctx.client, dst, &t, &ctx.options_with_jobs(jobs))
        .await
        .with_context(|| format!("copying {} to {}", src, dst))?;
    Ok(())
}

pub async fn copy(ctx: &Ctx, a: CopyArgs) -> Result<()> {
    let src = parse_ref(&a.source)?;
    let dst = parse_ref(&a.destination)?;
    if !a.all_tags {
        copy_one(ctx, &src, &dst, a.jobs, a.no_clobber).await?;
        eprintln!("Copied {} -> {}", a.source, a.destination);
        return Ok(());
    }
    let tags = ctx.client.list_tags(&src).await?;
    let jobs = if a.jobs == 0 { ctx.jobs } else { a.jobs };
    let (src, dst, no_clobber) = (&src, &dst, a.no_clobber);
    let futs: Vec<_> = tags
        .iter()
        .map(|t| async move {
            let s = repo_tag(src, t)?;
            let d = repo_tag(dst, t)?;
            copy_one(ctx, &s, &d, 1, no_clobber).await?;
            eprintln!("Copied {} -> {}", s, d);
            Ok::<_, anyhow::Error>(())
        })
        .collect();
    futures::stream::iter(futs)
        .buffer_unordered(jobs.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// export / flatten

async fn image_or_stdin(
    ctx: &Ctx,
    image: &str,
) -> Result<(Arc<dyn Image>, Option<tempfile::TempPath>)> {
    if image != "-" {
        return Ok((ctx.image(&parse_ref(image)?).await?, None));
    }
    let tmp = tempfile::NamedTempFile::new()?.into_temp_path();
    let mut f = tokio::fs::File::create(&tmp).await?;
    tokio::io::copy(&mut tokio::io::stdin(), &mut f).await?;
    f.flush().await?;
    let img = tarball::image_from_path(&tmp, None).await?;
    Ok((img, Some(tmp)))
}

pub async fn export(ctx: &Ctx, image: &str, out: &str) -> Result<()> {
    let (img, _tmp) = image_or_stdin(ctx, image).await?;
    let mut r = mutate::extract(img);
    if out == "-" {
        let mut o = tokio::io::stdout();
        tokio::io::copy(&mut r, &mut o).await?;
        o.flush().await?;
    } else {
        let mut f = tokio::fs::File::create(out).await?;
        tokio::io::copy(&mut r, &mut f).await?;
        f.flush().await?;
    }
    Ok(())
}

async fn flatten_image(img: Arc<dyn Image>) -> Result<Arc<dyn Image>> {
    let digest = img.digest().await?;
    let oci = is_oci(&img.media_type().await?);
    let mut cfg = img.config_file().await?;
    cfg.rootfs.diff_ids.clear();
    cfg.history.clear();
    let base = if oci {
        oci_empty_image()
    } else {
        empty::image()
    };
    let base = mutate::config_file(base, cfg);
    let mt = if oci {
        MediaType::OciLayerGzip
    } else {
        MediaType::DockerLayerGzip
    };
    let layer = StreamLayer::new(mutate::extract(img.clone())).with_media_type(mt);
    let anns = img
        .manifest()
        .await?
        .annotations()
        .cloned()
        .unwrap_or_default();
    let flat = mutate::append(
        base,
        vec![Addendum {
            layer: Some(Arc::new(layer)),
            history: History {
                created_by: Some(format!("crane flatten {}", digest)),
                comment: Some("flattened by crane".to_string()),
                ..Default::default()
            },
            ..Default::default()
        }],
    )?;
    Ok(if anns.is_empty() {
        flat
    } else {
        mutate::annotations(flat, anns)
    })
}

pub async fn flatten(ctx: &Ctx, image: &str, tag: Option<&str>) -> Result<()> {
    let r = parse_ref(image)?;
    let out = match ctx.taggable(&r).await? {
        Taggable::Image(img) => ctx.publish(flatten_image(img).await?, tag, &r).await?,
        Taggable::Index(idx) => {
            let mut adds = Vec::new();
            for (d, child) in children(idx.as_ref()).await? {
                if let Child::Image(img) = child {
                    let flat = flatten_image(img).await?;
                    // Children must exist before the index is pushed.
                    let fd = containerregistry_registry::remote::write_by_digest(
                        &ctx.client,
                        &r,
                        flat.clone(),
                        &ctx.options(),
                    )
                    .await?;
                    eprintln!("flattened {} -> {}", d.digest, fd);
                    let mut add = IndexAddendum::new(Child::Image(flat));
                    add.platform = d.platform.clone();
                    add.annotations = d.annotations.clone();
                    adds.push(add);
                }
            }
            let base = mutate::remove_manifests(idx, Arc::new(|_| true));
            ctx.publish_index(mutate::append_manifests(base, adds), tag, &r)
                .await?
        }
    };
    println!("{}", out);
    Ok(())
}

// ---------------------------------------------------------------------------
// index

#[derive(Subcommand)]
pub enum IndexCmd {
    /// Modifies a remote index by filtering based on platform (--platform)
    Filter {
        /// Index reference
        image: String,
        /// Tag to apply to resulting image
        #[arg(short, long)]
        tag: Option<String>,
    },
    /// Append manifests to a remote index
    Append {
        /// Base index (default: empty)
        base: Option<String>,
        /// Tag to apply to resulting image
        #[arg(short, long)]
        tag: Option<String>,
        /// References to manifests to append to the base index
        #[arg(short = 'm', long = "manifest", value_delimiter = ',')]
        manifests: Vec<String>,
        /// Use Docker media types for an empty base index
        #[arg(long)]
        docker_empty_base: bool,
        /// Append each child of an appended index rather than the index itself
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        flatten: bool,
    },
}

pub async fn index(ctx: &Ctx, cmd: IndexCmd) -> Result<()> {
    match cmd {
        IndexCmd::Filter { image, tag } => {
            let platforms = ctx.platforms();
            if platforms.is_empty() {
                bail!("--platform is required");
            }
            let r = parse_ref(&image)?;
            let idx = ctx.index(&r).await?;
            let keep = matcher::platforms_satisfying(platforms);
            let filtered = mutate::remove_manifests(idx, matcher::not(keep));
            println!("{}", ctx.publish_index(filtered, tag.as_deref(), &r).await?);
            Ok(())
        }
        IndexCmd::Append {
            base,
            tag,
            manifests,
            docker_empty_base,
            flatten,
        } => {
            let (idx, base_ref) = match &base {
                Some(b) => {
                    let r = parse_ref(b)?;
                    (ctx.index(&r).await?, Some(r))
                }
                None if docker_empty_base => (
                    mutate::index_media_type(empty::index(), MediaType::DockerManifestList),
                    None,
                ),
                None => (empty::index(), None),
            };
            let mut adds = Vec::new();
            for m in &manifests {
                let d =
                    containerregistry_registry::remote::get(&ctx.client, &parse_ref(m)?).await?;
                if d.is_index() {
                    let child_idx = d.image_index();
                    if flatten {
                        for (_, c) in children(child_idx.as_ref()).await? {
                            adds.push(IndexAddendum::new(c));
                        }
                    } else {
                        adds.push(IndexAddendum::new(Child::Index(child_idx)));
                    }
                } else {
                    let img = d.image(&Platform::new("", "")).await?;
                    let mut add = IndexAddendum::new(Child::Image(img.clone()));
                    if let Ok(cf) = img.config_file().await
                        && !cf.os.is_empty()
                    {
                        let mut p = Platform::new(cf.architecture.clone(), cf.os.clone());
                        p.variant = cf.variant.clone();
                        p.os_version = cf.os_version.clone();
                        add = add.with_platform(p);
                    }
                    adds.push(add);
                }
            }
            let out = mutate::append_manifests(idx, adds);
            let fallback = match (&tag, &base_ref) {
                (Some(t), _) => parse_ref(t)?,
                (None, Some(b)) => b.clone(),
                (None, None) => bail!("must specify --tag when there is no base index"),
            };
            let dst = tag.as_deref().or(base.as_deref());
            println!("{}", ctx.publish_index(out, dst, &fallback).await?);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// mutate

#[derive(Args)]
pub struct MutateArgs {
    /// Image reference
    image: String,
    /// New annotations to add (KEY=VALUE)
    #[arg(short = 'a', long = "annotation", value_parser = parse_kv)]
    annotations: Vec<(String, String)>,
    /// New labels to add (KEY=VALUE)
    #[arg(short = 'l', long = "label", value_parser = parse_kv)]
    labels: Vec<(String, String)>,
    /// New envvar to add (KEY=VALUE)
    #[arg(short = 'e', long = "env")]
    env: Vec<String>,
    /// New entrypoint to set
    #[arg(long, value_delimiter = ',')]
    entrypoint: Option<Vec<String>>,
    /// New cmd to set
    #[arg(long, value_delimiter = ',')]
    cmd: Option<Vec<String>>,
    /// Repository to push the mutated image to (by digest)
    #[arg(long)]
    repo: Option<String>,
    /// New tag reference to apply to mutated image
    #[arg(short, long)]
    tag: Option<String>,
    /// Path to new tarball of resulting image
    #[arg(short, long)]
    output: Option<String>,
    /// Path to tarball to append to image
    #[arg(long = "append", value_delimiter = ',')]
    append: Vec<String>,
    /// New user to set
    #[arg(short, long)]
    user: Option<String>,
    /// New working dir to set
    #[arg(short, long)]
    workdir: Option<String>,
    /// New ports to expose
    #[arg(long, value_delimiter = ',')]
    exposed_ports: Vec<String>,
    /// New platform to set in the form os/arch[/variant][:osversion]
    #[arg(long)]
    set_platform: Option<Platform>,
}

pub async fn mutate(ctx: &Ctx, a: MutateArgs) -> Result<()> {
    let r = parse_ref(&a.image)?;
    let base = ctx.image(&r).await?;
    let mut img = base.clone();
    if !a.append.is_empty() {
        let layers = file_layers(&a.append, layer_media_type(base.as_ref()).await).await?;
        img = mutate::append_layers(img, layers);
    }
    let mut cf = img.config_file().await?;
    let c = cf.config.get_or_insert_with(Default::default);
    c.labels.extend(a.labels.iter().cloned());
    for e in &a.env {
        let key = e.split('=').next().unwrap_or(e);
        c.env.retain(|x| x.split('=').next() != Some(key));
        c.env.push(e.clone());
    }
    if let Some(ep) = a.entrypoint {
        c.entrypoint = Some(ep);
    }
    if let Some(cmd) = a.cmd {
        c.cmd = Some(cmd);
    }
    if let Some(u) = a.user {
        c.user = Some(u);
    }
    if let Some(w) = a.workdir {
        c.working_dir = Some(w);
    }
    for p in &a.exposed_ports {
        let p = if p.contains('/') {
            p.clone()
        } else {
            format!("{}/tcp", p)
        };
        c.exposed_ports.insert(p, EmptyObject {});
    }
    if let Some(p) = a.set_platform {
        cf.os = p.os;
        cf.architecture = p.architecture;
        cf.variant = p.variant;
        cf.os_version = p.os_version;
    }
    img = mutate::config_file(img, cf);
    if !a.annotations.is_empty() {
        img = mutate::annotations(img, a.annotations.into_iter().collect());
    }

    if let Some(out) = &a.output {
        let tag = a.tag.clone().unwrap_or_else(|| tarball_tag(&r));
        tarball::write_to_file(out, vec![(vec![tag], img)]).await?;
        return Ok(());
    }
    let fallback = match &a.repo {
        Some(repo) => parse_ref(repo)?,
        None => r,
    };
    println!("{}", ctx.publish(img, a.tag.as_deref(), &fallback).await?);
    Ok(())
}

// ---------------------------------------------------------------------------
// pull / push

#[derive(Args)]
pub struct PullArgs {
    /// IMAGE... TARBALL (or IMAGE... with --output)
    #[arg(required = true)]
    args: Vec<String>,
    /// Path to cache image layers
    #[arg(short = 'c', long = "cache_path", alias = "cache-path")]
    cache_path: Option<String>,
    /// Format in which to save images ("tarball", "legacy", or "oci")
    #[arg(long)]
    format: Option<String>,
    /// Preserve the image reference as an annotation (--format=oci)
    #[arg(long)]
    annotate_ref: bool,
    /// Output path (alternative to the trailing positional path; implies
    /// --format=oci by default)
    #[arg(short, long, hide = true)]
    output: Option<String>,
}

pub async fn pull(ctx: &Ctx, a: PullArgs) -> Result<()> {
    let (srcs, path) = match &a.output {
        Some(o) => (a.args.as_slice(), o.clone()),
        None => {
            if a.args.len() < 2 {
                bail!("usage: crane pull IMAGE... TARBALL");
            }
            let (p, s) = a.args.split_last().expect("non-empty");
            (s, p.clone())
        }
    };
    let format = a
        .format
        .clone()
        .unwrap_or_else(|| if a.output.is_some() { "oci" } else { "tarball" }.to_string());
    let cache: Option<Arc<dyn cache::Cache>> = a
        .cache_path
        .as_ref()
        .map(|p| Arc::new(cache::FilesystemCache::new(p)) as Arc<dyn cache::Cache>);

    match format.as_str() {
        "oci" => {
            let layout = match Layout::open(&path) {
                Ok(l) => l,
                Err(_) => Layout::create(&path)?,
            };
            for src in srcs {
                let r = parse_ref(src)?;
                let mut opts = AppendOptions::default();
                if a.annotate_ref {
                    opts = opts.with_annotation(matcher::ANNOTATION_REF_NAME, r.to_string());
                }
                let d = containerregistry_registry::remote::get(&ctx.client, &r).await?;
                let digest = if d.is_index() && ctx.platform().is_none() {
                    let idx = d.image_index();
                    layout.append_index(idx.as_ref(), &opts).await?;
                    idx.digest().await?
                } else {
                    let mut img = ctx.image(&r).await?;
                    if let Some(c) = &cache {
                        img = cache::image(img, c.clone());
                    }
                    layout.append_image(img.as_ref(), &opts).await?;
                    img.digest().await?
                };
                println!("{}", digest);
            }
        }
        "tarball" | "legacy" => {
            let mut images = Vec::new();
            for src in srcs {
                let r = parse_ref(src)?;
                let mut img = ctx.image(&r).await?;
                if let Some(c) = &cache {
                    img = cache::image(img, c.clone());
                }
                images.push((vec![tarball_tag(&r)], img));
            }
            if format == "tarball" {
                tarball::write_to_file(&path, images).await?;
            } else {
                let mut r = tarball::write_legacy(images).await?;
                let mut f = tokio::fs::File::create(&path).await?;
                tokio::io::copy(&mut r, &mut f).await?;
                f.flush().await?;
            }
        }
        other => bail!(
            "unexpected --format: {:?} (valid values are: tarball, legacy, and oci)",
            other
        ),
    }
    Ok(())
}

#[derive(Args)]
pub struct PushArgs {
    /// Path to a tarball or OCI layout
    path: String,
    /// Image reference
    image: String,
    /// Push a collection of images as a single index
    #[arg(long)]
    index: bool,
    /// Path to file where the published image reference will be written
    #[arg(long)]
    image_refs: Option<String>,
}

pub async fn push(ctx: &Ctx, a: PushArgs) -> Result<()> {
    // go-containerregistry's order is PATH IMAGE; earlier versions of this
    // CLI took IMAGE PATH, which is still accepted.
    let (path, image) = if !Path::new(&a.path).exists() && Path::new(&a.image).exists() {
        (a.image.as_str(), a.path.as_str())
    } else {
        (a.path.as_str(), a.image.as_str())
    };
    let r = parse_ref(image)?;
    let t = load_local(Path::new(path), a.index).await?;
    let d = ctx.push(&r, &t).await?;
    let out = r.with_new_digest(d);
    if let Some(f) = &a.image_refs {
        std::fs::write(f, out.to_string())?;
    }
    println!("{}", out);
    Ok(())
}

// ---------------------------------------------------------------------------
// rebase

#[derive(Args)]
pub struct RebaseArgs {
    /// Image to rebase
    image: Option<String>,
    /// Original image to rebase (deprecated: use the positional argument)
    #[arg(long)]
    original: Option<String>,
    /// Old base image to remove
    #[arg(long = "old_base", alias = "old-base")]
    old_base: Option<String>,
    /// New base image to insert
    #[arg(long = "new_base", alias = "new-base")]
    new_base: Option<String>,
    /// Tag to apply to rebased image
    #[arg(short, long, alias = "rebased")]
    tag: Option<String>,
}

pub async fn rebase(ctx: &Ctx, a: RebaseArgs) -> Result<()> {
    let orig_s = a
        .image
        .or(a.original)
        .context("an image to rebase is required")?;
    let orig_ref = parse_ref(&orig_s)?;
    let orig = ctx.image(&orig_ref).await?;
    let anns = orig
        .manifest()
        .await?
        .annotations()
        .cloned()
        .unwrap_or_default();
    let old_s = match a.old_base {
        Some(s) => s,
        None => match (anns.get(BASE_NAME), anns.get(BASE_DIGEST)) {
            (Some(n), Some(d)) => {
                let n = parse_ref(n)?;
                format!("{}/{}@{}", n.registry(), n.repository(), d)
            }
            _ => bail!("--old_base is required (the image has no base image annotations)"),
        },
    };
    let new_s = match a.new_base {
        Some(s) => s,
        None => anns
            .get(BASE_NAME)
            .cloned()
            .context("--new_base is required (the image has no base image annotations)")?,
    };
    let old = ctx.image(&parse_ref(&old_s)?).await?;
    let new_ref = parse_ref(&new_s)?;
    let new = ctx.image(&new_ref).await?;
    let mut rebased = mutate::rebase(orig.clone(), old, new.clone()).await?;
    let mt = orig.media_type().await?;
    rebased = mutate::media_type(rebased, mt.clone());
    if is_oci(&mt) {
        rebased = mutate::config_media_type(rebased, MediaType::OciConfig);
    }
    let mut new_anns = anns;
    new_anns.insert(BASE_NAME.to_string(), new_s.clone());
    new_anns.insert(BASE_DIGEST.to_string(), new.digest().await?.to_string());
    rebased = mutate::annotations(rebased, new_anns);
    let dst = a.tag.or_else(|| orig_ref.is_tag().then(|| orig_s.clone()));
    let out = ctx.publish(rebased, dst.as_deref(), &orig_ref).await?;
    eprintln!("Rebased {} onto {}", orig_s, new_s);
    println!("{}", out);
    Ok(())
}

// ---------------------------------------------------------------------------
// registry / layout

#[derive(Subcommand)]
pub enum RegistryCmd {
    /// Serve a registry implementation
    Serve {
        /// Address to listen on (default ":$PORT", or a random port)
        #[arg(long)]
        address: Option<String>,
        /// Path to a directory where blobs will be stored
        #[arg(long)]
        disk: Option<String>,
        /// Store blobs on disk in a temporary directory
        #[arg(long)]
        blobs_to_disk: bool,
    },
}

pub async fn registry(cmd: RegistryCmd) -> Result<()> {
    let RegistryCmd::Serve {
        address,
        disk,
        blobs_to_disk,
    } = cmd;
    use containerregistry_server::{Options, Registry, Storage};
    let _tmp;
    let storage = match (disk, blobs_to_disk) {
        (Some(d), _) => Storage::Disk(d.into()),
        (None, true) => {
            let t = tempfile::tempdir()?;
            let p = t.path().to_path_buf();
            _tmp = t;
            Storage::Disk(p)
        }
        (None, false) => Storage::Memory,
    };
    let addr = address
        .unwrap_or_else(|| format!(":{}", std::env::var("PORT").unwrap_or_else(|_| "0".into())));
    let addr = if addr.starts_with(':') {
        format!("0.0.0.0{}", addr)
    } else {
        addr
    };
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("serving on port {}", listener.local_addr()?.port());
    let reg = Registry::new(Options {
        storage,
        ..Default::default()
    })?;
    reg.serve(listener).await?;
    Ok(())
}

#[derive(Subcommand)]
pub enum LayoutCmd {
    /// Garbage collect unreferenced blobs in an OCI layout
    Gc {
        /// Layout directory
        path: String,
    },
}

pub async fn layout(cmd: LayoutCmd) -> Result<()> {
    let LayoutCmd::Gc { path } = cmd;
    let layout = Layout::open(&path)?;
    for d in layout.garbage_collect().await? {
        eprintln!("garbage collecting: {}", d);
        layout.remove_blob(&d)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_tags() {
        assert!(is_digest_tag(&format!("sha256-{}", "a".repeat(64))));
        assert!(is_digest_tag(&format!("sha256-{}.sig", "0".repeat(64))));
        assert!(!is_digest_tag("sha256-abc"));
        assert!(!is_digest_tag("latest"));
    }
}
