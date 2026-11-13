use std::{array, cmp::Reverse, collections::HashSet, error::Error, fmt::Debug, mem, net::SocketAddr, sync::Arc, time::Duration};
use itertools::Itertools;
use chrono::Utc;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use tokio::{io, net::{ToSocketAddrs, UdpSocket}, sync::RwLock, task::JoinHandle, time::{self, Interval, MissedTickBehavior}};

use crate::{entry::Entry, tag::{self, TAG_LEN, Tag::self}};

const MAX_DATAGRAM: usize = 1400;

/// # Source
/// [Newscast computing]
/// 
/// [Newscast computing]: https://www.distributed-systems.net/my-data/papers/wip-newscast.pdf
pub struct Newspaper<const C: usize, I> where I: Default + Eq + Serialize + Clone + Send + Sync + Debug + 'static, for<'a> I: Deserialize<'a>
{
    item: I,
    cache: Arc<RwLock<[Entry<I>; C]>>
}

impl<const C: usize, I> Newspaper<C, I> where I: Default + Eq + Serialize + Clone + Send + Sync + Debug + 'static, for<'a> I: Deserialize<'a>
{
    fn init_cache<const B: usize>(bootstraps: [Entry<I>; B]) -> [Entry<I>; C]
    {
        let mut cache: [Entry<I>; C] = array::from_fn(|_| Entry::<I>::default());
        for (i, e) in bootstraps.into_iter().take(C).enumerate()
        {
            cache[i] = e;
        }
        cache
    }
    pub fn bind<const B: usize>(item: I, bootstraps: [Entry<I>; B]) -> io::Result<Self>
    {
        Ok(Self { item: item, cache: Arc::new(RwLock::new(Self::init_cache(bootstraps))) })
    } 
    //TODO: enum for tags, refine send receive logic
    fn build_cache_buf(tag: Tag, own: &Entry<I>, cache: &[Entry<I>; C], to_ignore: SocketAddr, buf: &mut[u8; MAX_DATAGRAM]) -> Result<usize, postcard::Error>
    {
        let mut entries: Vec<&Entry<I>> = cache.iter().filter(|e| e.is_valid() && *e.addr() != to_ignore).collect();
        entries.sort_unstable_by_key(|e| Reverse(e.timestamp()));
        entries.insert(0, own);
        buf[..tag::TAG_LEN].copy_from_slice(&Into::<[u8; tag::TAG_LEN]>::into(tag));
        loop {
            match postcard::to_slice(&entries, &mut buf[tag::TAG_LEN..]) {
                Ok(out) => return Ok(tag::TAG_LEN + out.len()),
                Err(postcard::Error::SerializeBufferFull) if entries.len() > 1 => { entries.pop(); },
                Err(e) => return Err(e)
            }
        }
    }
    fn pick_random_valid_address(cache: &[Entry<I>; C]) -> Option<SocketAddr>
    {
        println!("{:?}", cache.iter().filter(|e| e.is_valid()).collect_vec());
        cache.iter().filter(|e| e.is_valid()).choose(&mut rand::rng()).map(|e| *e.addr())
    }
    async fn send_cache(socket: &UdpSocket, item: I, cache: &RwLock<[Entry<I>; C]>) -> io::Result<()>
    {
        let mut buf: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
        let (dest, len): (SocketAddr, usize) = {
            let guard = cache.read().await;
            let dest: SocketAddr = match Self::pick_random_valid_address(&guard) {
                Some(e) => e,
                None => return Ok(())
            };
            let len: usize = Self::build_cache_buf(Tag::Send, &Entry::new(socket.local_addr()?, Utc::now().timestamp_millis(), item), &guard, dest, &mut buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            (dest, len)
        };
        println!("Dest: {dest}");
        socket.send_to(&buf[..len], dest).await?;
        Ok(())
    }
    fn aggregate(own_cache: [Entry<I>; C], rec_cache: Vec<Entry<I>>) -> [Entry<I>; C]
    {
        let mut new_cache: [Entry<I>; C] = array::from_fn(|_| Entry::<I>::default());
        let mut seen: HashSet<SocketAddr> = HashSet::new();
        for (i, e) in own_cache.into_iter().merge_by(rec_cache.into_iter(), |x, y| x.timestamp() >= y.timestamp()).filter(|e| e.is_valid() && seen.insert(*e.addr())).take(C).enumerate()
        {
            new_cache[i] = e;
        }
        new_cache
    }
    fn decode_cache(buf: &[u8]) -> Result<Vec<Entry<I>>, postcard::Error>
    {
        let mut rec_cache: Vec<Entry<I>> = postcard::from_bytes(buf)?;
        rec_cache.sort_unstable_by_key(|e| Reverse(e.timestamp()));
        Ok(rec_cache)
    }
    async fn receive_cache(buf: &[u8], cache: &RwLock<[Entry<I>; C]>) -> Result<(), postcard::Error>
    {
        let rec_cache: Vec<Entry<I>> =  Self::decode_cache(buf)?;
        let mut guard = cache.write().await;
        let old = mem::replace(&mut *guard, array::from_fn(|_| Entry::default()));
        *guard = Self::aggregate(old, rec_cache);
        Ok(())
    }
    async fn respond_cache(socket: &UdpSocket, item: I, sender: SocketAddr, buf: &[u8], cache: &RwLock<[Entry<I>; C]>) -> io::Result<()>
    {
        let mut to_send = [0u8; MAX_DATAGRAM];
        let len = Self::build_cache_buf(Tag::Response, &Entry::new(socket.local_addr()?, Utc::now().timestamp_millis(), item), &*cache.read().await, sender, &mut to_send).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        socket.send_to(&to_send[..len], sender).await?;
        Self::receive_cache(buf, cache).await.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
    pub fn connect<A: ToSocketAddrs + Send + Sync + 'static>(&self, socket: A) -> JoinHandle<io::Result<()>>
    {
        let cache: Arc<RwLock<[Entry<I>; C]>> = self.cache.clone();
        let item: I = self.item.clone();
        tokio::spawn(async move {
            let mut ticker: Interval = time::interval(Duration::from_secs(5));
            let udp: UdpSocket = UdpSocket::bind(socket).await?;
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let mut buf: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
                tokio::select! {
                    res = udp.recv_from(&mut buf) => {
                        let (len, sender) = res?;
                        if let Err(e) = Self::decode(&udp, item.clone(), &buf[..len], &cache, sender).await
                        {
                            eprintln!("{e}")
                        }
                    }
                    _ = ticker.tick() => {
                        if let Err(e) = Self::send_cache(&udp, item.clone(), &cache).await
                        {
                            eprintln!("{e}")
                        }
                    }
                }
            }
        })
    }
    async fn decode(socket: &UdpSocket, item: I, buf: &[u8], cache: &RwLock<[Entry<I>; C]>, sender: SocketAddr) -> Result<(), Box<dyn Error>>
    {
        match Tag::try_from(buf.get(..TAG_LEN).ok_or("Datagram too short")?)? {
            Tag::Send => Self::respond_cache(socket, item, sender, &buf[tag::TAG_LEN..], cache).await?,
            Tag::Response => Self::receive_cache(&buf[tag::TAG_LEN..], cache).await?,
            Tag::Plain => todo!()
        };
        Ok(())
    }
}