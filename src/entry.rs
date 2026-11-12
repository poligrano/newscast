use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Entry<I: Default + Eq>
{
    addr: SocketAddr,
    timestamp: i64,
    item: I
}

impl<I: Default + Eq> Entry<I> 
{
    pub fn new(addr: SocketAddr, timestamp: i64, item: I) -> Self
    {
        Self { addr: addr, timestamp: timestamp, item: item }
    }
    pub fn addr(&self) -> &SocketAddr
    {
        &self.addr
    }
    pub fn timestamp(&self) -> i64
    {
        self.timestamp
    }
    pub fn item(&self) -> &I
    {
        &self.item
    }
}

impl<I: Default + Eq> Default for Entry<I> 
{
    fn default() -> Self 
    {
        Self { addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)), timestamp: i64::default(), item: I::default() }
    }   
}