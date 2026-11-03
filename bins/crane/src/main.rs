//! crane CLI - A tool for interacting with remote images and registries.

#[tokio::main]
async fn main() {
    containerregistry_crane::main_with_args(std::env::args_os()).await;
}
