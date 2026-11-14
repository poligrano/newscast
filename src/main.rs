use std::{env, net::SocketAddr, str::FromStr};
use local_ip_address::local_ip;
use tokio::{io::{self, AsyncBufReadExt, BufReader}, select};
use crate::{memory::entry::Entry, newspaper::Newspaper};

pub mod newspaper;
pub mod memory;
mod message;
mod tag;

const C: usize = 20;
const CAP: usize = 1024;
const BOOTSTRAP_SOCKET: &str = "192.168.0.114:20209";
const BOOTSTRAP_USER: &str = "__boot";
type I = String;

#[tokio::main]
async fn main() 
{
    let mut it: env::Args = env::args();
    let program_name: String = it.next().unwrap();
    if let Some(arg) = it.next()
    {
        let (bootstraps, socket) = match arg.as_str() {
            BOOTSTRAP_USER => ([Entry::default()], SocketAddr::from_str(BOOTSTRAP_SOCKET).unwrap()),
            _ => ([Entry::new(SocketAddr::from_str(BOOTSTRAP_SOCKET).unwrap(), i64::default(), BOOTSTRAP_USER.to_owned())], SocketAddr::new(local_ip().unwrap(), 0u16))
        };
        let n = Newspaper::<C, I>::bind(arg, socket, bootstraps, CAP).await.unwrap();
        let _ = n.connect();
        let mut lines = BufReader::new(io::stdin()).lines();
        loop {
            select! {
                mx = n.recv() => {
                    if let Some(mx) = mx 
                    {
                        println!("\n{:#?}\n{}\n", mx, String::from_utf8_lossy(mx.content()));
                    }
                }
                line = lines.next_line() => match line {
                    Ok(Some(l)) => {
                        if let Err(e) = n.broadcast(l.into_bytes()).await 
                        { 
                            eprintln!("{e}"); 
                        }
                    }
                    _ => break
                }
            }
        }
    }
    else 
    {
        eprintln!("Usage: {program_name} <name> | \"__boot\"");
    }
}