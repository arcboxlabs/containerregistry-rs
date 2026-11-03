//! Runs a registry: `cargo run -p containerregistry-server --example serve -- [ADDR] [--disk DIR] [--no-referrers]`.

use containerregistry_server::Registry;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut addr = "127.0.0.1:5000".to_string();
    let mut builder = Registry::builder();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--disk" => {
                let dir = args.next().expect("--disk needs a directory");
                builder = builder.disk(dir);
            }
            "--no-referrers" => builder = builder.referrers(false),
            _ => addr = arg,
        }
    }
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("serving registry on {}", listener.local_addr()?);
    builder.build()?.serve(listener).await
}
