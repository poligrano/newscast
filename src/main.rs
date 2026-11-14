use std::{env, net::SocketAddr, str::FromStr};
use local_ip_address::local_ip;
use crate::{entry::Entry, newspaper::Newspaper};

pub mod newspaper;
pub mod entry;
mod tag;

const C: usize = 20;
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
        let n: Newspaper<C, I> = Newspaper::<C, I>::bind(arg, socket, bootstraps).await.unwrap();
        n.connect().await.unwrap();
    }
    else 
    {
        eprintln!("Usage: {program_name} <name> | \"__boot\"");
    }
}