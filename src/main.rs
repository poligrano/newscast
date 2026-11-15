use std::{env, net::SocketAddr, str::FromStr, io::Write};
use chrono::Utc;
use local_ip_address::local_ip;
use rustyline_async::{Readline, ReadlineEvent, SharedWriter};
use tokio::{io, task::JoinHandle};
use tracing::{error, warn};
use crate::{memory::entry::Entry, message::Message, newspaper::Newspaper};

pub mod newspaper;
pub mod memory;
mod message;
mod tag;

const C: usize = 20;
type I = String;

const CAP: usize = 1024;

const BOOTSTRAP_SOCKET: &str = "192.168.0.114:20209";
const BOOTSTRAP_USER: &str = "__boot";

#[tokio::main]
async fn main() 
{
    let (mut rl, mut out) = Readline::new("> ".to_owned()).unwrap();
    let mut it: env::Args = env::args();
    let program_name: String = it.next().unwrap_or("newspaper".to_owned());
    init_tracing(out.clone());
    if let (Some(arg), None) = (it.next(), it.next())
    {
        match app_loop(arg, &mut rl, &mut out).await {
            Ok(()) => {},
            Err(e) => error!(error = %e)
        }
    }
    else 
    {
        writeln!(out, "Usage: {program_name} <name> | \"__boot\"").ok();
    }
}

fn pretty_print_message(mx: &Message) -> String
{
    format!(
r#"
-----NEW MESSAGE-----
  id: {}
  ttl: {}
  from: {}

  {}

-----END MESSAGE-----
"#, mx.id(), mx.ttl(), mx.origin(), String::from_utf8_lossy(mx.content()))
}

async fn build_newspaper(arg: String)  -> io::Result<Newspaper<C, I>>
{
    let (bootstraps, socket) = match arg.as_str() {
        BOOTSTRAP_USER => ([Entry::default()], SocketAddr::from_str(BOOTSTRAP_SOCKET).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?),
        _ => ([Entry::new(SocketAddr::from_str(BOOTSTRAP_SOCKET).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?, Utc::now().timestamp_millis(), BOOTSTRAP_USER.to_owned())], SocketAddr::new(local_ip().map_err(|e| io::Error::new(io::ErrorKind::AddrNotAvailable, e))?, 0u16))
    };
    Newspaper::<C, I>::bind(arg, socket, bootstraps, CAP).await
}

fn init_tracing(wr: SharedWriter)
{
    tracing_subscriber::fmt().with_writer(move || wr.clone()).with_ansi(true).init();
}

async fn app_loop(arg: String, rl: &mut Readline, out: &mut SharedWriter) -> io::Result<()>
{
    let mut n: Newspaper<C, I> = build_newspaper(arg).await?;
    let mut handle: JoinHandle<()> = n.connect();
    loop {
        tokio::select! {
            mx = n.recv() => match mx {
                Some(mx) => { writeln!(out, "{}", pretty_print_message(&mx)).ok(); },
                None => break
            },
            ev = rl.readline() => match ev {
                Ok(ReadlineEvent::Line(l)) => {
                    rl.add_history_entry(l.clone());
                    if let Err(e) = n.broadcast(l.into_bytes()).await 
                    {
                        error!(error = %e);
                    }
                }
                Ok(ReadlineEvent::Eof | ReadlineEvent::Interrupted) | Err(_) => break,
            },
            _ = &mut handle => {
                warn!("DISCONNECTED");
                break;
            }
        }
    }
    rl.flush().ok();
    handle.abort();
    Ok(())
}