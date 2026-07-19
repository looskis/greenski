mod config;
mod event_sink;
mod lifecycle;
mod model;
mod runtime_state;
mod server;
mod store;
mod transport;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use runtime_state::RuntimeState;
use server::AppState;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

#[derive(Parser)]
#[command(name = "greenski", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send a WhatsApp message through the local daemon.
    Send {
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        text: Option<String>,
        #[arg(long, default_value = "whatsapp")]
        protocol: String,
        #[arg(long)]
        client_ref: Option<String>,
        #[arg(value_names = ["TO", "TEXT"])]
        positional: Vec<String>,
    },
    /// Read the durable event journal as newline-delimited JSON.
    Events {
        #[arg(long, default_value_t = 0)]
        since: i64,
        #[arg(long)]
        limit: Option<u64>,
        #[arg(long)]
        follow: bool,
    },
    /// List WhatsApp chats stored from history sync and live traffic.
    Chats {
        #[arg(long, default_value_t = 100)]
        limit: u64,
    },
    /// List stored messages for a phone number or chat JID.
    Messages {
        #[arg(long = "from")]
        from: String,
        #[arg(long, default_value_t = 50)]
        limit: u64,
        #[arg(long)]
        before: Option<i64>,
    },
    /// Ask the primary phone for an older history page for a chat.
    Sync {
        #[arg(long = "from")]
        from: String,
        #[arg(long, default_value_t = 100)]
        count: i32,
    },
    /// Display a QR code and pair Greenski as a linked device.
    #[command(alias = "setup")]
    Pair,
    /// Start the daemon in the background if needed.
    Up,
    /// Stop a daemon started by Greenski.
    Down,
    /// Show daemon and WhatsApp connection state.
    Status,
    /// Install and load the per-user macOS LaunchAgent.
    Install,
    /// Unload and remove the macOS LaunchAgent.
    Uninstall,
    /// Run the daemon in the foreground.
    #[command(hide = true)]
    Run,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Send {
            to,
            text,
            protocol,
            client_ref,
            positional,
        } => lifecycle::send(to, text, protocol, client_ref, positional),
        Command::Events {
            since,
            limit,
            follow,
        } => lifecycle::events(since, limit, follow),
        Command::Chats { limit } => lifecycle::chats(limit),
        Command::Messages {
            from,
            limit,
            before,
        } => lifecycle::messages(from, limit, before),
        Command::Sync { from, count } => lifecycle::sync_history(from, count),
        Command::Pair => lifecycle::pair(),
        Command::Up => lifecycle::up(),
        Command::Down => lifecycle::down(),
        Command::Status => lifecycle::status(),
        Command::Install => lifecycle::install(),
        Command::Uninstall => lifecycle::uninstall(),
        Command::Run => run(),
    }
}

fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "greenski=info,warn".into()),
        )
        .init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_run())
}

async fn async_run() -> Result<()> {
    let _guard = lifecycle::DaemonGuard::acquire()?;
    let config = Arc::new(config::Config::load_or_init()?);
    let store = store::Store::open(&config::state_db_path()).await?;
    let runtime = Arc::new(RuntimeState::new());
    let (events, _) = broadcast::channel(1024);
    let sink = event_sink::spawn(
        store.clone(),
        config.webhook_url.clone(),
        config.hmac_secret.clone(),
        events.clone(),
    );

    let bot = transport::build_bot(
        &config::whatsapp_db_path(),
        store.clone(),
        sink.clone(),
        runtime.clone(),
    )
    .await?;
    config::secure_file(&config::whatsapp_db_path())?;
    let client = bot.client();
    let (send_tx, send_rx) = mpsc::channel(256);
    let send_worker = transport::spawn_send_worker(send_rx, client.clone(), store.clone(), sink);

    let app = server::router(AppState {
        send_tx,
        runtime,
        store,
        events,
        config: config.clone(),
        client: Some(client),
    });
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", config.port))
        .await
        .with_context(|| format!("bind 127.0.0.1:{}", config.port))?;
    tracing::info!(port = config.port, "Greenski listening");

    let mut server_task = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut bot_handle = bot.spawn();
    tokio::select! {
        _ = &mut bot_handle => {
            tracing::warn!("WhatsApp client stopped");
        }
        _ = whatsapp_rust::shutdown_signal() => {
            tracing::info!("shutting down");
            bot_handle.shutdown().await;
        }
        result = &mut server_task => {
            result.context("HTTP server task failed")??;
            bot_handle.shutdown().await;
        }
    }
    server_task.abort();
    send_worker.abort();
    Ok(())
}
