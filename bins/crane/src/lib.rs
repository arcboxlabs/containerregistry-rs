//! The crane CLI (a port of go-containerregistry's `crane`), as a library
//! so that `gcrane` can reuse every command.

mod cmds;
mod edit;
mod util;

use std::io;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};

use containerregistry_registry::{Client, ClientConfig};

use util::{Ctx, PlatformFlag};

#[derive(Parser)]
#[command(name = "crane")]
#[command(about = "Crane is a tool for managing container images")]
#[command(version)]
pub struct Cli {
    /// Allow image references to be fetched without TLS
    #[arg(long, global = true)]
    insecure: bool,

    /// Specifies the platform in the form os/arch[/variant][:osversion]
    /// (e.g. linux/amd64), or "all"
    #[arg(long, global = true)]
    platform: Option<PlatformFlag>,

    /// Enable debug logs
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Allow pushing non-distributable (foreign) layers
    #[arg(long, global = true)]
    allow_nondistributable_artifacts: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Append contents of a tarball to a remote image
    Append(cmds::AppendArgs),
    /// Log in or access credentials
    #[command(subcommand)]
    Auth(cmds::AuthCmd),
    /// Read a blob from the registry
    Blob {
        /// Blob reference (repo@sha256:...)
        blob: String,
    },
    /// List the repos in a registry
    Catalog {
        /// Registry host
        registry: String,
        /// Print the full image reference
        #[arg(long)]
        full_ref: bool,
    },
    /// Get the config of an image
    Config {
        /// Image reference
        image: String,
    },
    /// Efficiently copy a remote image from src to dst while retaining the digest value
    #[command(alias = "cp")]
    Copy(cmds::CopyArgs),
    /// Delete an image reference from its registry
    Delete {
        /// Image reference
        image: String,
    },
    /// Get the digest of an image
    Digest {
        /// Image reference
        image: Option<String>,
        /// Path to tarball containing the image
        #[arg(long)]
        tarball: Option<String>,
        /// Print the full image reference by digest
        #[arg(long)]
        full_ref: bool,
    },
    /// Edit the contents of an image
    #[command(subcommand)]
    Edit(edit::EditCmd),
    /// Export filesystem of a container image as a tarball
    Export {
        /// Image reference, or "-" to read a tarball from stdin
        image: String,
        /// Output path, or "-" for stdout
        #[arg(default_value = "-")]
        tarball: String,
    },
    /// Flatten an image's layers into a single layer
    Flatten {
        /// Image reference
        image: String,
        /// New tag to apply to the flattened image (default: push by digest)
        #[arg(short, long)]
        tag: Option<String>,
    },
    /// Modify an image index
    #[command(subcommand)]
    Index(cmds::IndexCmd),
    /// List the tags in a repo
    #[command(alias = "list")]
    Ls {
        /// Repository
        repository: String,
        /// Print the full image reference
        #[arg(long)]
        full_ref: bool,
        /// Omit digest tags (e.g. ':sha256-...')
        #[arg(short = 'O', long)]
        omit_digest_tags: bool,
    },
    /// Get the manifest of an image
    Manifest {
        /// Image reference
        image: String,
    },
    /// Modify image labels, annotations, config and layers
    Mutate(Box<cmds::MutateArgs>),
    /// Pull remote images by reference and store their contents locally
    Pull(cmds::PullArgs),
    /// Push local image contents to a remote registry
    Push(cmds::PushArgs),
    /// Rebase an image onto a new base image
    Rebase(cmds::RebaseArgs),
    /// Efficiently tag a remote image
    Tag {
        /// Image reference
        image: String,
        /// New tag
        tag: String,
    },
    /// Validate that an image is well-formed
    Validate {
        /// Path to tarball to validate
        #[arg(long)]
        tarball: Option<String>,
        /// Name of remote image to validate
        #[arg(long)]
        remote: Option<String>,
        /// Skip downloading/digesting layers
        #[arg(long)]
        fast: bool,
    },
    /// Print the version
    Version,
    /// Registry utilities
    #[command(subcommand)]
    Registry(cmds::RegistryCmd),
    /// Manage OCI layouts
    #[command(subcommand)]
    Layout(cmds::LayoutCmd),
}

/// Parses `args` (including the binary name) and runs the command,
/// exiting the process with status 1 on error.
pub async fn main_with_args<I, T>(args: I)
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let cli = Cli::parse_from(args);
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
        .with_user_agent(format!("crane/{}", env!("CARGO_PKG_VERSION")));
    let keychain = Arc::new(containerregistry_auth::default_keychain());
    let ctx = Ctx {
        client: Client::with_keychain(config, keychain)?,
        platform: cli.platform,
        nondistributable: cli.allow_nondistributable_artifacts,
        jobs: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
    };

    match cli.command {
        Commands::Append(a) => cmds::append(&ctx, a).await,
        Commands::Auth(a) => cmds::auth(&ctx, a).await,
        Commands::Blob { blob } => cmds::blob(&ctx, &blob).await,
        Commands::Catalog { registry, full_ref } => cmds::catalog(&ctx, &registry, full_ref).await,
        Commands::Config { image } => cmds::config(&ctx, &image).await,
        Commands::Copy(a) => cmds::copy(&ctx, a).await,
        Commands::Delete { image } => cmds::delete(&ctx, &image).await,
        Commands::Digest {
            image,
            tarball,
            full_ref,
        } => cmds::digest(&ctx, image.as_deref(), tarball.as_deref(), full_ref).await,
        Commands::Edit(e) => edit::run(&ctx, e).await,
        Commands::Export { image, tarball } => cmds::export(&ctx, &image, &tarball).await,
        Commands::Flatten { image, tag } => cmds::flatten(&ctx, &image, tag.as_deref()).await,
        Commands::Index(i) => cmds::index(&ctx, i).await,
        Commands::Ls {
            repository,
            full_ref,
            omit_digest_tags,
        } => cmds::ls(&ctx, &repository, full_ref, omit_digest_tags).await,
        Commands::Manifest { image } => cmds::manifest(&ctx, &image).await,
        Commands::Mutate(a) => cmds::mutate(&ctx, *a).await,
        Commands::Pull(a) => cmds::pull(&ctx, a).await,
        Commands::Push(a) => cmds::push(&ctx, a).await,
        Commands::Rebase(a) => cmds::rebase(&ctx, a).await,
        Commands::Tag { image, tag } => cmds::tag(&ctx, &image, &tag).await,
        Commands::Validate {
            tarball,
            remote,
            fast,
        } => cmds::validate(&ctx, tarball.as_deref(), remote.as_deref(), fast).await,
        Commands::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Commands::Registry(r) => cmds::registry(r).await,
        Commands::Layout(l) => cmds::layout(l).await,
    }
}
