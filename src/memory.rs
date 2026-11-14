use std::{array, io, iter, sync::Mutex};
use chrono::Utc;
use itertools::Itertools;
use tokio::{net::{ToSocketAddrs, UdpSocket}, sync::{RwLock, mpsc::{self, Receiver, Sender}}};
use crate::{memory::{entry::Entry, seen::Seen}, message::{IdType, Message}};

pub(crate) struct Memory<const C: usize, I: Default + Clone>
{
    id: I,
    udp: UdpSocket,
    cache: RwLock<[Entry<I>; C]>,
    seen: Mutex<Seen<IdType>>,
    inbox: Sender<Message>
}

impl<const C: usize, I: Default + Clone> Memory<C, I>
{
    fn init_cache(bootstraps: impl IntoIterator<Item = Entry<I>>) -> [Entry<I>; C]
    {
        bootstraps.into_iter().chain(iter::repeat_with(|| Entry::<I>::default())).take(C).collect_array().unwrap_or(array::from_fn(|_| Entry::<I>::default()))
    }
    pub async fn new(id: I, socket: impl ToSocketAddrs, bootstraps: impl IntoIterator<Item = Entry<I>>, cap: usize) -> io::Result<(Self, Receiver<Message>)>
    {
        let (inbox, rc) = mpsc::channel(cap);
        UdpSocket::bind(socket).await.map(|udp| (Self { id: id, udp: udp, cache: RwLock::new(Self::init_cache(bootstraps)), seen: Mutex::new(Seen::new(cap)), inbox: inbox }, rc))
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
    pub fn seen(&self) -> &Mutex<Seen<IdType>>
    {
        &self.seen
    }
    pub fn inbox(&self) -> &Sender<Message>
    {
        &self.inbox
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
        pub fn addr(&self) -> SocketAddr
        {
            self.addr
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

mod seen
{
    use std::{collections::{HashSet, VecDeque}, hash::Hash};

    pub struct Seen<U: Eq + Hash + Copy>
    {
        cap: usize,
        seen: HashSet<U>,
        order: VecDeque<U>
    }

    impl<U: Eq + Hash + Copy> Seen<U>
    {
        pub fn new(cap: usize) -> Self
        {
            Self { cap: cap, seen: HashSet::with_capacity(cap), order: VecDeque::with_capacity(cap) }
        }
        pub fn insert(&mut self, e: U) -> bool
        {
            if !self.seen.insert(e)
            {
                return false;
            }
            self.order.push_front(e);
            if self.order.len() > self.cap && let Some(old) = self.order.pop_back()
            {
                self.seen.remove(&old);
            }
            return true;
        }
    }
}