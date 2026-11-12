use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(bound = "I: Default + Eq + Serialize, for<'a> I: Deserialize<'a>")]
pub struct Entry<I> where I: Default + Eq + Serialize, for<'a> I: Deserialize<'a>
{
    addr: SocketAddr,
    timestamp: i64,
    item: I
}

impl<I> Entry<I> where I: Default + Eq + Serialize, for<'a> I: Deserialize<'a>
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
    pub fn is_valid(&self) -> bool
    {
        self.addr.ip().is_unspecified()
    }
}

impl<I> Default for Entry<I> where I: Default + Eq + Serialize, for<'a> I: Deserialize<'a>
{
    fn default() -> Self 
    {
        Self { addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)), timestamp: i64::default(), item: I::default() }
    }   
}