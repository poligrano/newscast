use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

pub struct Entry<I: Default>
{
    addr: SocketAddr,
    timestamp: i64,
    item: I
}

impl<I: Default> Entry<I> 
{
    fn new(addr: SocketAddr, timestamp: i64, item: I) -> Self
    {
        Self { addr: addr, timestamp: timestamp, item: item }
    }    
}

impl<I: Default> Default for Entry<I> 
{
    fn default() -> Self 
    {
        Self { addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)), timestamp: i64::default(), item: I::default() }
    }   
}