use anyhow::{bail, Result};
use clap::Parser;
use nos_followers::config::{Config, Settings};
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::TcpStream;

/// A simple utility to send JSONL lines to the followers server via a TCP connection.
///
/// Example usage:
/// 1. Read from a JSONL file:
///    jsonl_importer --file path/to/file.jsonl
///
/// 2. Pipe JSONL data from stdin:
///    nak req -k 3 --paginate --paginate-interval 2s relay.nos.social | jsonl_importer
#[derive(Parser, Debug)]
#[command(
    name = "jsonl_importer",
    version = "1.0",
    about = "Send JSONL lines to the followers' server via a TCP connection",
    long_about = "A utility to send JSONL lines to the followers' server via a TCP connection to \
127.0.0.1:3001.\n\n\
Example usage:\n\
1. Read from a JSONL file:\n\
   jsonl_importer --file path/to/file.jsonl\n\n\
2. Pipe JSONL data from stdin:\n\
   nak req -k 3 --paginate --paginate-interval 2s relay.nos.social | jsonl_importer"
)]
struct Args {
    /// Path to the JSONL file (if not provided, reads from stdin).
    #[arg(
        short,
        long,
        help = "Path to the JSONL file (reads from stdin if not provided)"
    )]
    file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("config")?;
    let settings = config.get::<Settings>()?;

    let args = Args::parse();
    let address = SocketAddr::from(([0, 0, 0, 0], settings.tcp_importer_port));

    // Try connecting to the server via TCP
    let Ok(mut stream) = TcpStream::connect(&address).await else {
        bail!(
            "Failed to connect to the server at {}.\nIs the followers server running?",
            address
        );
    };

    println!("Connected to server at: {}", address);

    // Input can be from a file or stdin
    let input: Box<dyn tokio::io::AsyncBufRead + Unpin> = if let Some(file_path) = args.file {
        let file = tokio::fs::File::open(file_path).await?;
        Box::new(AsyncBufReader::new(file))
    } else {
        Box::new(AsyncBufReader::new(tokio::io::stdin()))
    };

    let mut lines = input.lines();

    // Read and send each line
    while let Some(line) = lines.next_line().await? {
        let jsonl_line = line.trim();

        if !jsonl_line.is_empty() {
            stream.write_all(jsonl_line.as_bytes()).await?;
            stream.write_all(b"\n").await?;
            println!("Sent: {}", jsonl_line);
        }
    }

    Ok(())
}
