use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};

use agent_master::{serve, Processes};

#[derive(Parser)]
#[command(
    about = "Runs an agent's processes, under terminals of their own or without, and gives control of them over a port"
)]
struct Cli {
    /// Address to accept control connections on, e.g. 127.0.0.1:7620 or 0.0.0.0:7620.
    #[arg(long, env = "AGENT_MASTER_LISTEN")]
    listen: String,
    /// How long each process has to end gently once agent-master is asked to
    /// end, before it is killed, in milliseconds.
    #[arg(long, env = "AGENT_MASTER_STOP_GRACE_MS", default_value_t = 5_000)]
    stop_grace_ms: u64,
}

/// Asked to end, agent-master takes its processes down first: one under a
/// terminal would end with the terminal anyway, one without a terminal would not.
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let listener = TcpListener::bind(&cli.listen)
        .await
        .with_context(|| format!("listening on {}", cli.listen))?;
    log::info!("listening on {}", listener.local_addr().context("reading the bound address")?);
    let mut terminated = signal(SignalKind::terminate()).context("watching for SIGTERM")?;
    let mut interrupted = signal(SignalKind::interrupt()).context("watching for SIGINT")?;
    let processes = Arc::new(Processes::new());
    let served = tokio::select! {
        served = serve(listener, processes.clone()) => served,
        _ = terminated.recv() => {
            log::info!("asked to end, taking every process down");
            Ok(())
        }
        _ = interrupted.recv() => {
            log::info!("interrupted, taking every process down");
            Ok(())
        }
    };
    processes.close(Duration::from_millis(cli.stop_grace_ms)).await;
    served
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_process_gets_five_seconds_to_end_gently_unless_told_otherwise() {
        std::env::remove_var("AGENT_MASTER_STOP_GRACE_MS");
        let cli = Cli::try_parse_from(["svora-am", "--listen", "127.0.0.1:1"]).expect("parse");
        assert_eq!(cli.stop_grace_ms, 5_000);
    }
}
