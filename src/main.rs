use std::{env, io, net::{IpAddr, Ipv4Addr, SocketAddr}};

use tokio::net::UdpSocket;

use crate::{entry::Entry, newspaper::Newspaper};

mod newspaper;
mod entry;
mod tag;
mod tagerror;

const C: usize = 20;

#[tokio::main]
async fn main() 
{
    let bootstrap = [Entry::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 114)), 20209), 0, "__bootstrap".to_owned())];
    let mut it = env::args();
    it.next();
    let arg = it.next().unwrap_or_default();
    if arg == "boot"
    {
        let n = Newspaper::<C, String>::bind("__bootstrap".to_owned(), []).unwrap();
        match n.connect("192.168.0.114:20209").await {
            Ok(Err(e)) => eprint!("{e}"),
            Err(e) => eprint!("{e}"),
            Ok(Ok(())) => {}
        }
    }
    else 
    {
        let n = Newspaper::<C, String>::bind(arg, bootstrap).unwrap();
        match n.connect(SocketAddr::new(get_ip().await.unwrap(), 0)).await {
            Ok(Err(e)) => eprint!("{e}"),
            Err(e) => eprint!("{e}"),
            Ok(Ok(())) => {}
        }
    }
}

async fn get_ip() -> io::Result<IpAddr> 
{
    let probe = UdpSocket::bind("0.0.0.0:0").await?;
    probe.connect("8.8.8.8:80").await?;
    println!("LocalIP: {}", probe.local_addr()?.ip());
    Ok(probe.local_addr()?.ip())
}