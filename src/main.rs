use std::{env, net::{IpAddr, Ipv4Addr, SocketAddr}};

use crate::{entry::Entry, newspaper::Newspaper};

mod newspaper;
mod entry;
mod tag;
mod tagerror;

#[tokio::main()]
async fn main() {
    let bootstrap = [Entry::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 20209), 0, "__bootstrap".to_owned())];
    let mut it = env::args();
    it.next();
    if let Some(arg) = it.next() && arg == "boot"
    {
        let n = Newspaper::<20, String>::bind("__bootstrap".to_owned(), bootstrap).unwrap();
        n.connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 20209));
    }
    else 
    {
        let n = Newspaper::<20, String>::bind("__bootstrap".to_owned(), bootstrap).unwrap();
        n.connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));
    }
}