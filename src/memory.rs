use std::{array, io, iter};

use chrono::Utc;
use itertools::Itertools;
use tokio::{net::{ToSocketAddrs, UdpSocket}, sync::RwLock};

use crate::memory::entry::Entry;

pub(crate) struct Memory<const C: usize, I: Default + Clone>
{
    id: I,
    udp: UdpSocket,
    cache: RwLock<[Entry<I>; C]>
}

impl<const C: usize, I: Default + Clone> Memory<C, I>
{
    fn init_cache<const B: usize>(bootstraps: [Entry<I>; B]) -> [Entry<I>; C]
    {
        bootstraps.into_iter().chain(iter::repeat_with(|| Entry::<I>::default())).take(C).collect_array().unwrap_or(array::from_fn(|_| Entry::<I>::default()))
    }
    pub async fn new<const B: usize, A: ToSocketAddrs>(id: I, socket: A, bootstraps: [Entry<I>; B]) -> io::Result<Self>
    {
        Ok(Self { id: id, udp: UdpSocket::bind(socket).await?, cache: RwLock::new(Self::init_cache(bootstraps)) })
    }
    pub fn build_entry(&self) -> io::Result<Entry<I>>
    {
        Ok(Entry::new(self.udp.local_addr()?, Utc::now().timestamp_millis(), self.id.clone()))
    }
    #[allow(dead_code)]
    pub fn id(&self) -> &I
    {
        &self.id
    }
    pub fn udp(&self) -> &UdpSocket
    {
        &self.udp
    }
    pub fn cache(&self) -> &RwLock<[Entry<I>; C]>
    {
        &self.cache
    }
}

pub mod entry
{
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Entry<I: Default>
    {
        addr: SocketAddr,
        timestamp: i64,
        id: I
    }

    impl<I: Default> Entry<I>
    {
        pub fn new(addr: SocketAddr, timestamp: i64, id: I) -> Self
        {
            Self { addr: addr, timestamp: timestamp, id: id }
        }
        pub fn addr(&self) -> &SocketAddr
        {
            &self.addr
        }
        pub fn timestamp(&self) -> i64
        {
            self.timestamp
        }
        pub fn id(&self) -> &I
        {
            &self.id
        }
        pub fn is_valid(&self) -> bool
        {
            !self.addr.ip().is_unspecified() && self.addr.port() != 0 && self.timestamp <= Utc::now().timestamp_millis()
        }
    }

    impl<I: Default> Default for Entry<I>
    {
        fn default() -> Self 
        {
            Self { addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0u16)), timestamp: i64::default(), id: I::default() }
        }   
    }    
}