use std::{array, cmp::Reverse, collections::HashSet, iter, mem, net::{Ipv4Addr, SocketAddr, SocketAddrV4}, sync::Arc, time::Duration};
use itertools::Itertools;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use tokio::{io::self, net::ToSocketAddrs, sync::RwLock, task::JoinHandle, time::{self, Interval, MissedTickBehavior}};
use crate::{memory::{entry::Entry, Memory}, tag::{self, TAG_LEN, Tag::self}};

const MAX_DATAGRAM: usize = 1400;

/// # Source
/// [Newscast computing]
/// 
/// [Newscast computing]: https://www.distributed-systems.net/my-data/papers/wip-newscast.pdf
pub struct Newspaper<const C: usize, I> where I: Default + Clone + Send + Sync + 'static + Serialize, for<'de> I: Deserialize<'de>
{
    shared: Arc<Memory<C, I>>
}

impl<const C: usize, I> Newspaper<C, I> where I: Default + Clone + Send + Sync + 'static + Serialize, for<'de> I: Deserialize<'de>
{
    pub async fn bind<const B: usize, A: ToSocketAddrs>(id: I, socket: A, bootstraps: [Entry<I>; B]) -> io::Result<Self>
    {
        Ok(Self { shared: Arc::new(Memory::new(id, socket, bootstraps).await?) })
    } 
    fn build_cache_buf(tag: Tag, own: &Entry<I>, cache: &[Entry<I>; C], skip: SocketAddr, buf: &mut [u8; MAX_DATAGRAM]) -> postcard::Result<usize>
    {
        let mut entries: Vec<&Entry<I>> = cache.iter().filter(|e| e.is_valid() && *e.addr() != skip).collect();
        entries.sort_unstable_by_key(|e| Reverse(e.timestamp()));
        entries.insert(0, own);
        buf[..tag::TAG_LEN].copy_from_slice(Into::<&[u8; tag::TAG_LEN]>::into(tag));
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
        cache.iter().filter(|e| e.is_valid()).choose(&mut rand::rng()).map(|e| *e.addr())
    }
    async fn send_cache(shared: &Arc<Memory<C, I>>) -> io::Result<()>
    {
        let mut buf: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
        let (dest, len) = {
            let guard = shared.cache().read().await;
            let dest: SocketAddr = match Self::pick_random_valid_address(&guard) {
                Some(e) => e,
                None => return Ok(())
            };
            let len: usize = Self::build_cache_buf(Tag::Send, &shared.build_entry()?, &guard, dest, &mut buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            (dest, len)
        };
        println!("Sending to {dest}");
        shared.udp().send_to(&buf[..len], dest).await?;
        Ok(())
    }
    fn aggregate(own_cache: [Entry<I>; C], rec_cache: Vec<Entry<I>>, socket: SocketAddr) -> [Entry<I>; C]
    {
        let mut seen: HashSet<SocketAddr> = HashSet::new();
        own_cache.into_iter().merge_by(rec_cache.into_iter(), |x, y| x.timestamp() >= y.timestamp()).filter(|e| e.is_valid() && *e.addr() != socket && seen.insert(*e.addr())).chain(iter::repeat_with(|| Entry::<I>::default())).take(C).collect_array().unwrap_or(array::from_fn(|_| Entry::<I>::default()))
    }
    fn decode_cache(buf: &[u8]) -> postcard::Result<Vec<Entry<I>>>
    {
        let mut rec_cache: Vec<Entry<I>> = postcard::from_bytes(buf)?;
        rec_cache.sort_unstable_by_key(|e| Reverse(e.timestamp()));
        Ok(rec_cache)
    }
    async fn receive_cache(cache: &RwLock<[Entry<I>; C]>, socket: SocketAddr, buf: &[u8]) -> postcard::Result<()>
    {
        let rec_cache: Vec<Entry<I>> =  Self::decode_cache(buf)?;
        let mut guard = cache.write().await;
        let old: [Entry<I>; C] = mem::replace(&mut *guard, array::from_fn(|_| Entry::default()));
        *guard = Self::aggregate(old, rec_cache, socket);
        Ok(())
    }
    async fn respond_cache(shared: &Arc<Memory<C, I>>, sender: SocketAddr, buf: &[u8]) -> io::Result<()>
    {
        let mut to_send: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
        let len = Self::build_cache_buf(Tag::Response, &shared.build_entry()?, &*shared.cache().read().await, sender, &mut to_send).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        shared.udp().send_to(&to_send[..len], sender).await?;
        Self::receive_cache(shared.cache(), shared.udp().local_addr()?, buf).await.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
    async fn decode(shared: &Arc<Memory<C, I>>, buf: &[u8], sender: SocketAddr) -> io::Result<()>
    {
        match Tag::try_from(buf.get(..TAG_LEN).ok_or(io::Error::new(io::ErrorKind::InvalidData, tag::Error::new("Datagram too short".to_owned())))?).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
            Tag::Send => {
                println!("Responding to {sender}");
                Self::respond_cache(shared, sender, &buf[tag::TAG_LEN..]).await?
            },
            Tag::Response => {
                println!("Received response from {sender}");
                Self::receive_cache(shared.cache(), shared.udp().local_addr()?, &buf[tag::TAG_LEN..]).await.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            },
            Tag::Plain => todo!()
        };
        Ok(())
    }
    pub fn connect(&self) -> JoinHandle<()>
    {
        let shared: Arc<Memory<C, I>> = self.shared.clone();
        tokio::spawn(async move {
            let mut ticker: Interval = time::interval(Duration::from_secs(5));
            println!("Local Addr: {}", shared.udp().local_addr().unwrap_or(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0u16))));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let mut buf: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
                if let Err(e) = tokio::select! {
                    res = shared.udp().recv_from(&mut buf) => match res {
                        Ok((len, sender)) => Self::decode(&shared, &buf[..len], sender).await,
                        Err(e) => Err(e)
                    },
                    _ = ticker.tick() => Self::send_cache(&shared).await
                }
                {
                    eprintln!("{e}");
                }
                println!("{:?}", shared.cache().read().await.iter().filter(|e| e.is_valid()).map(|e| (e.addr(), e.timestamp())).collect_vec());
            }
        })
    }
}