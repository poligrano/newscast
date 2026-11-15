use std::{env, net::SocketAddr, str::FromStr, time::Duration};
use chrono::Utc;
use crossterm::event::{Event as CEvent, EventStream, KeyEventKind};
use futures::StreamExt;
use local_ip_address::local_ip;
use ratatui::DefaultTerminal;
use tokio::io;
use tracing_appender::rolling::RollingFileAppender;
use crate::{app::{Action, App}, memory::entry::Entry, newspaper::Newspaper};

mod app;
mod ui;
pub mod newspaper;
pub mod memory;
mod message;
mod tag;

const C: usize = 20;
const CAP: usize = 1024;
type I = String;

const BOOTSTRAP_SOCKET: &str = "192.168.0.114:20209";
const BOOTSTRAP_USER: &str = "__boot";

#[tokio::main]
async fn main() -> io::Result<()>
{
    let mut args = env::args();
    let prog = args.next().unwrap_or_else(|| "newspaper".into());
    let (Some(name), None) = (args.next(), args.next()) else {
        eprintln!("Usage: {prog} <name> | \"{BOOTSTRAP_USER}\"");
        return Ok(());
    };
    let log: RollingFileAppender = tracing_appender::rolling::never(".", format!("newscast-{name}.log"));
    tracing_subscriber::fmt().with_writer(log).with_ansi(false).init();
    let n: Newspaper<C, I> = build_newspaper(name.clone()).await?;
    let mut terminal = ratatui::init();
    let res: Result<(), io::Error> = run(&mut terminal, n, name).await;
    ratatui::restore();
    res
}

async fn build_newspaper(name: String) -> io::Result<Newspaper<C, I>>
{
    let boot: SocketAddr = SocketAddr::from_str(BOOTSTRAP_SOCKET).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let (bootstraps, socket) = if name == BOOTSTRAP_USER {
        ([Entry::default()], boot)
    } else {
        ([Entry::new(boot, Utc::now().timestamp_millis(), BOOTSTRAP_USER.to_owned())], SocketAddr::new(local_ip().map_err(|e| io::Error::new(io::ErrorKind::AddrNotAvailable, e))?, 0))
    };
    Newspaper::<C, I>::bind(name, socket, bootstraps, CAP).await
}

async fn run(terminal: &mut DefaultTerminal, mut n: Newspaper<C, I>, name: String) -> io::Result<()>
{
    let mut app = App::new(name);
    let mut net = n.connect();
    let mut keys = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        terminal.draw(|f| ui::draw(f, &mut app))?;
        tokio::select! {
            Some(Ok(ev)) = keys.next() => {
                if let CEvent::Key(k) = ev && k.kind == KeyEventKind::Press {
                    match app.on_key(k) {
                        Action::Quit => break,
                        Action::Send(text) => match n.broadcast(text.clone().into_bytes()).await {
                            Ok(()) => app.push(None, text),
                            Err(e) => app.push(None, format!("[send error: {e}]")),
                        },
                        Action::None => {}
                    }
                }
            }
            mx = n.recv() => match mx {
                Some(m) => app.push(Some(m.origin()), String::from_utf8_lossy(m.content()).into_owned()),
                None => break,
            },
            _ = tick.tick() => { app.peers = n.peers().await; }
            _ = &mut net => {
                app.connected = false;
                app.push(None, "[disconnected]".into());
                terminal.draw(|f| ui::draw(f, &mut app))?;
                let _ = keys.next().await;
                break;
            }
        }
    }
    net.abort();
    Ok(())
}