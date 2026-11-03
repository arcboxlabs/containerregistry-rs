//! gcrane CLI - crane with GCR / Artifact Registry extensions
//! (a port of go-containerregistry's `gcrane`).
//!
//! `copy`, `ls` and `gc` understand Google's extended tag listing
//! (child repositories and untagged manifests) and can recurse through a
//! repository tree; every other command is crane's.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};

use containerregistry_registry::remote::{self, Options};
use containerregistry_registry::{Client, ClientConfig, GoogleTags, Reference};

#[derive(Parser)]
#[command(name = "gcrane")]
#[command(
    about = "gcrane is a tool for managing container images, specifically for GCR; \
all crane commands are also available"
)]
#[command(version)]
struct Cli {
    /// Allow image references to be fetched without TLS
    #[arg(long, global = true)]
    insecure: bool,

    /// Enable debug logs
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Efficiently copy a remote image from src to dst
    #[command(alias = "cp")]
    Copy {
        /// Source image or repository
        source: String,
        /// Destination image or repository
        destination: String,
        /// Whether to recurse through repos
        #[arg(short, long)]
        recursive: bool,
        /// The maximum number of concurrent copies
        #[arg(short = 'j', long, default_value_t = default_jobs())]
        jobs: usize,
    },
    /// List the contents of a repo
    Ls {
        /// Repository
        repository: String,
        /// Whether to recurse through repos
        #[arg(short, long)]
        recursive: bool,
        /// Format the response from the registry as JSON, one line per repo
        #[arg(long)]
        json: bool,
    },
    /// List images that are not tagged
    Gc {
        /// Repository
        repository: String,
        /// Whether to recurse through repos
        #[arg(short, long)]
        recursive: bool,
    },
}

/// Whether the subcommand (the first positional argument) is a crane
/// command rather than one gcrane overrides.
fn delegates_to_crane(args: &[OsString]) -> bool {
    let mut it = args.iter().skip(1).map(|a| a.to_string_lossy());
    while let Some(a) = it.next() {
        if a == "--platform" {
            it.next();
            continue;
        }
        if a.starts_with('-') {
            continue;
        }
        return !matches!(a.as_ref(), "copy" | "cp" | "ls" | "gc" | "help");
    }
    false
}

fn default_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[tokio::main]
async fn main() {
    let args: Vec<OsString> = std::env::args_os().collect();
    // Hand crane commands the original arguments, global flags included.
    if delegates_to_crane(&args) {
        containerregistry_crane::main_with_args(args).await;
        return;
    }
    let cli = Cli::parse_from(&args);
    let level = if cli.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::WARN
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive(level.into()),
        )
        .with_writer(io::stderr)
        .init();
    if let Err(e) = run(cli).await {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let config = ClientConfig::new()
        .with_https(!cli.insecure)
        .with_insecure(cli.insecure)
        .with_user_agent(format!("gcrane/{}", env!("CARGO_PKG_VERSION")));
    let client =
        Client::with_keychain(config, Arc::new(containerregistry_auth::default_keychain()))?;
    match cli.command {
        Commands::Copy {
            source,
            destination,
            recursive,
            jobs,
        } => copy(&client, &source, &destination, recursive, jobs).await,
        Commands::Ls {
            repository,
            recursive,
            json,
        } => ls(&client, &repository, recursive, json).await,
        Commands::Gc {
            repository,
            recursive,
        } => gc(&client, &repository, recursive).await,
    }
}

fn parse(s: &str) -> Result<Reference> {
    s.parse()
        .with_context(|| format!("parsing reference {:?}", s))
}

/// `registry/repository` of a reference.
fn repo_name(r: &Reference) -> String {
    format!("{}/{}", r.registry(), r.repository())
}

/// Lists `repo` and, when `recursive`, every child repository
/// (go `google.Walk`), depth first.
///
/// Registries without GCR's extensions report no children; there the
/// repositories under `root` are found through the catalog instead.
async fn walk(client: &Client, root: &str, recursive: bool) -> Result<Vec<(String, GoogleTags)>> {
    let root_ref = parse(root)?;
    let root_tags = match client.google_list(&root_ref).await {
        Ok(t) => Some(t),
        // With recursion the root itself need not be a repository.
        Err(e) if recursive && e.status_code() == Some(404) => None,
        Err(e) => return Err(e).with_context(|| format!("listing {}", root)),
    };
    let has_extensions = root_tags
        .as_ref()
        .is_some_and(|t| !t.child.is_empty() || !t.manifest.is_empty());
    if recursive && !has_extensions {
        let prefix = format!("{}/", root_ref.repository());
        let mut out: Vec<(String, GoogleTags)> = root_tags
            .map(|t| (root.to_string(), t))
            .into_iter()
            .collect();
        for repo in client.catalog(root_ref.registry()).await? {
            if repo.starts_with(&prefix) {
                let name = format!("{}/{}", root_ref.registry(), repo);
                let tags = client.google_list(&parse(&name)?).await?;
                out.push((name, tags));
            }
        }
        return Ok(out);
    }

    let mut out = Vec::new();
    let mut stack = vec![root.to_string()];
    while let Some(repo) = stack.pop() {
        let tags = match (&root_tags, repo == root) {
            (Some(t), true) => t.clone(),
            _ => client
                .google_list(&parse(&repo)?)
                .await
                .with_context(|| format!("listing {}", repo))?,
        };
        if recursive {
            for child in tags.child.iter().rev() {
                stack.push(format!("{}/{}", repo, child));
            }
        }
        out.push((repo, tags));
    }
    Ok(out)
}

fn print_images(repo: &str, tags: &GoogleTags, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(tags)?);
        return Ok(());
    }
    for (digest, info) in &tags.manifest {
        println!("{}@{}", repo, digest);
        for t in &info.tag {
            println!("{}:{}", repo, t);
        }
    }
    Ok(())
}

async fn ls(client: &Client, repository: &str, recursive: bool, json: bool) -> Result<()> {
    let repo = repo_name(&parse(repository)?);
    if recursive {
        for (r, tags) in walk(client, &repo, true).await? {
            print_images(&r, &tags, json)?;
        }
        return Ok(());
    }
    let tags = client.google_list(&parse(&repo)?).await?;
    if !json {
        if tags.manifest.is_empty() && tags.child.is_empty() {
            // No GCR extensions: list the tags like a normal registry.
            for t in &tags.tags {
                println!("{}:{}", repo, t);
            }
            return Ok(());
        }
        for child in &tags.child {
            println!("{}/{}", repo, child);
        }
    }
    print_images(&repo, &tags, json)
}

async fn gc(client: &Client, repository: &str, recursive: bool) -> Result<()> {
    let repo = repo_name(&parse(repository)?);
    for (r, tags) in walk(client, &repo, recursive).await? {
        for (digest, info) in &tags.manifest {
            if info.tag.is_empty() {
                println!("{}@{}", r, digest);
            }
        }
    }
    Ok(())
}

/// One manifest to copy and the tags to apply to it.
struct Job {
    src: Reference,
    dst: Reference,
    tags: Vec<String>,
}

async fn copy(
    client: &Client,
    source: &str,
    destination: &str,
    recursive: bool,
    jobs: usize,
) -> Result<()> {
    let opts = Options::default().with_jobs(jobs);
    if !recursive {
        let (src, dst) = (parse(source)?, parse(destination)?);
        let t = remote::get(client, &src).await?.taggable();
        remote::push(client, &dst, &t, &opts).await?;
        eprintln!("Copied {} -> {}", source, destination);
        return Ok(());
    }

    let src_root = repo_name(&parse(source)?);
    let dst_root = repo_name(&parse(destination)?);
    let mut work = Vec::new();
    for (repo, tags) in walk(client, &src_root, true).await? {
        let suffix = &repo[src_root.len()..];
        let dst_repo = format!("{}{}", dst_root, suffix);
        if tags.manifest.is_empty() {
            // A registry without GCR extensions: copy by tag.
            for t in &tags.tags {
                work.push(Job {
                    src: parse(&format!("{}:{}", repo, t))?,
                    dst: parse(&format!("{}:{}", dst_repo, t))?,
                    tags: Vec::new(),
                });
            }
            continue;
        }
        let mut by_digest: BTreeMap<&String, Vec<String>> = BTreeMap::new();
        for (d, info) in &tags.manifest {
            by_digest.insert(d, info.tag.clone());
        }
        for (d, ts) in by_digest {
            work.push(Job {
                src: parse(&format!("{}@{}", repo, d))?,
                dst: parse(&format!("{}@{}", dst_repo, d))?,
                tags: ts,
            });
        }
    }
    eprintln!("Copying {} manifests", work.len());
    let futs: Vec<_> = work
        .into_iter()
        .map(|job| {
            let opts = opts.clone().with_jobs(1);
            async move {
                let t = remote::get(client, &job.src).await?.taggable();
                remote::push(client, &job.dst, &t, &opts)
                    .await
                    .with_context(|| format!("copying {} to {}", job.src, job.dst))?;
                for tag in &job.tags {
                    client.tag(&job.dst, tag).await?;
                }
                eprintln!("Copied {} -> {}", job.src, job.dst);
                Ok::<_, anyhow::Error>(())
            }
        })
        .collect();
    futures::stream::iter(futs)
        .buffer_unordered(jobs.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}
