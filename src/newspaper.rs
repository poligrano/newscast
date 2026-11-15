use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use tokio::{io::self, net::ToSocketAddrs, task::JoinHandle};
use crate::{memory::entry::Entry, message::Message, newspaper::{input::Input, output::Output}};

pub(crate) const MAX_DATAGRAM: usize = 1400;

const TTL: u8 = 20;
const FANOUT: usize = 5;

/// # Source
/// [Newscast computing]
/// 
/// [Newscast computing]: https://www.distributed-systems.net/my-data/papers/wip-newscast.pdf
pub struct Newspaper<const C: usize, I> where I: Default + Clone + Send + Sync + 'static + Serialize, for<'de> I: Deserialize<'de>
{
    output: Output<C, I>,
    input: Input
}

impl<const C: usize, I> Newspaper<C, I> where I: Default + Clone + Send + Sync + 'static + Serialize, for<'de> I: Deserialize<'de>
{
    pub async fn bind(id: I, socket: impl ToSocketAddrs, bootstraps: impl IntoIterator<Item = Entry<I>>, cap: usize) -> io::Result<Self>
    {
        Output::new(id, socket, bootstraps, cap).await.map(|(output, input)| Self { output: output, input: Input::new(input) })
    }
    pub fn split(self) -> (Input, Output<C, I>)
    {
        (self.input, self.output)
    }
    pub fn connect(&self) -> JoinHandle<()>
    {
        self.output.connect()
    }
    pub async fn broadcast(&self, content: Vec<u8>) -> io::Result<()>
    {
        self.output.broadcast(content).await
    }
    pub async fn recv(&mut self) -> Option<Message>
    {
        self.input.recv().await
    }
    pub async fn peers(&self) -> Vec<(SocketAddr, I)>
    {
        self.output.peers().await
    }
}

pub mod input
{
    use tokio::sync::mpsc::Receiver;
    use crate::message::Message;

    pub struct Input
    {
        incoming: Receiver<Message>
    }

    impl Input
    {
        pub(super) fn new(incoming: Receiver<Message>) -> Self
        {
            Self { incoming: incoming }
        }
        pub async fn recv(&mut self) -> Option<Message>
        {
            self.incoming.recv().await
        }
    }
}

pub mod output
{
    use std::{array, cmp::Reverse, collections::HashSet, iter, mem, net::{Ipv4Addr, SocketAddr, SocketAddrV4}, sync::Arc, time::Duration};
    use itertools::Itertools;
    use rand::seq::IteratorRandom;
    use serde::{Deserialize, Serialize};
    use tokio::{io, net::ToSocketAddrs, sync::{RwLock, mpsc::Receiver}, task::JoinHandle, time::{self, Interval, MissedTickBehavior}};
    use tracing::{info, warn};
    use crate::{memory::{Memory, entry::Entry}, message::Message, newspaper::{FANOUT, MAX_DATAGRAM, TTL}, tag::{self, Tag}};

    pub struct Output<const C: usize, I> where I: Default + Clone + Send + Sync + 'static + Serialize, for<'de> I: Deserialize<'de>
    {
        shared: Arc<Memory<C, I>>
    }

    impl<const C: usize, I> Output<C, I> where I: Default + Clone + Send + Sync + 'static + Serialize, for<'de> I: Deserialize<'de>
    {
        pub(super) async fn new(id: I, socket: impl ToSocketAddrs, bootstraps: impl IntoIterator<Item = Entry<I>>, cap: usize) -> io::Result<(Self, Receiver<Message>)>
        {
            Memory::new(id, socket, bootstraps, cap).await.map(|(shared, incoming)| (Self { shared: Arc::new(shared) }, incoming))
        } 
        fn build_cache_buf(tag: Tag, own: &Entry<I>, cache: &[Entry<I>; C], skip: SocketAddr, buf: &mut [u8; MAX_DATAGRAM]) -> postcard::Result<usize>
        {
            let mut entries: Vec<&Entry<I>> = cache.iter().filter(|e| e.is_valid() && e.addr() != skip).collect();
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
            cache.iter().filter(|e| e.is_valid()).choose(&mut rand::rng()).map(|e| e.addr())
        }
        async fn send_cache(shared: &Memory<C, I>) -> io::Result<()>
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
            info!(%dest, tag = ?Tag::Send);
            shared.udp().send_to(&buf[..len], dest).await?;
            Ok(())
        }
        fn aggregate(own_cache: [Entry<I>; C], rec_cache: Vec<Entry<I>>, socket: SocketAddr) -> [Entry<I>; C]
        {
            let mut seen: HashSet<SocketAddr> = HashSet::new();
            own_cache.into_iter().merge_by(rec_cache.into_iter(), |x, y| x.timestamp() >= y.timestamp()).filter(|e| e.is_valid() && e.addr() != socket && seen.insert(e.addr())).chain(iter::repeat_with(|| Entry::<I>::default())).take(C).collect_array().unwrap_or(array::from_fn(|_| Entry::<I>::default()))
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
        async fn respond_cache(shared: &Memory<C, I>, sender: SocketAddr, buf: &[u8]) -> io::Result<()>
        {
            let mut to_send: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
            let len = Self::build_cache_buf(Tag::Response, &shared.build_entry()?, &*shared.cache().read().await, sender, &mut to_send).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            shared.udp().send_to(&to_send[..len], sender).await?;
            Self::receive_cache(shared.cache(), shared.udp().local_addr()?, buf).await.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }
        async fn send_message(shared: &Memory<C, I>, dest: SocketAddr, message: &Message) -> io::Result<()>
        {
            let mut buf: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
            buf[..tag::TAG_LEN].copy_from_slice(Into::<&[u8; tag::TAG_LEN]>::into(Tag::Broad));
            let n = postcard::to_slice(message, &mut buf[tag::TAG_LEN..]).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?.len();
            shared.udp().send_to(&buf[..tag::TAG_LEN + n], dest).await?;
            Ok(())
        }
        async fn forward(shared: &Memory<C, I>, sender: SocketAddr, message: &Message)
        {
            let peers: Vec<SocketAddr> = shared.cache().read().await.iter().filter(|e| e.is_valid() && e.addr() != sender && e.addr() != message.origin()).map(|e| e.addr()).sample(&mut rand::rng(), FANOUT);
            for dest in peers
            {
                if let Err(e) = Self::send_message(shared, dest, message).await
                {
                    warn!(warning = %e);
                }
            }
        }
        async fn handle_incoming_broad(shared: &Arc<Memory<C, I>>, sender: SocketAddr, buf: &[u8]) -> postcard::Result<()>
        {
            let message: Message = postcard::from_bytes(buf)?;
            info!(?message, %sender);
            if shared.seen().lock().unwrap_or_else(|e| e.into_inner()).insert(message.id())
            {
                if let Some(fwd) = message.as_forwarded()
                {
                    let shared: Arc<Memory<C, I>> = shared.clone();
                    tokio::spawn(async move {
                        Self::forward(&shared, sender, &fwd).await
                    });
                }
                if let Err(e) = shared.inbox().try_send(message)
                {
                    warn!(warning = %e);
                }
            }
            Ok(())
        }
        async fn decode(shared: &Arc<Memory<C, I>>, sender: SocketAddr, buf: &[u8]) -> io::Result<()>
        {
            match Tag::try_from(buf.get(..tag::TAG_LEN).ok_or(io::Error::new(io::ErrorKind::InvalidData, tag::Error::new("Datagram too short")))?).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
                Tag::Send => {
                    info!(%sender, tag = ?Tag::Send);
                    Self::respond_cache(shared, sender, &buf[tag::TAG_LEN..]).await?;
                },
                Tag::Response => {
                    info!(%sender, tag = ?Tag::Response);
                    Self::receive_cache(shared.cache(), shared.udp().local_addr()?, &buf[tag::TAG_LEN..]).await.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                },
                Tag::Broad => {
                    Self::handle_incoming_broad(shared, sender, &buf[tag::TAG_LEN..]).await.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                }
            };
            Ok(())
        }
        pub(super) fn connect(&self) -> JoinHandle<()>
        {
            let shared: Arc<Memory<C, I>> = self.shared.clone();
            tokio::spawn(async move {
                let mut ticker: Interval = time::interval(Duration::from_secs(5));
                info!(local_address = %shared.udp().local_addr().unwrap_or(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0u16))));
                ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    let mut buf: [u8; MAX_DATAGRAM] = [0u8; MAX_DATAGRAM];
                    if let Err(e) = tokio::select! {
                        res = shared.udp().recv_from(&mut buf) => match res {
                            Ok((len, sender)) => Self::decode(&shared, sender, &buf[..len]).await,
                            Err(e) => Err(e)
                        },
                        _ = ticker.tick() => Self::send_cache(&shared).await
                    }
                    {
                        warn!(warning = %e)
                    }
                }
            })
        }
        pub async fn peers(&self) -> Vec<(SocketAddr, I)>
        {
            self.shared.cache().read().await.iter().filter(|e| e.is_valid()).map(|e| (e.addr(), e.id().clone())).collect()
        }
        fn new_message(&self, content: Vec<u8>) -> io::Result<Message>
        {
            Ok(Message::new(TTL, self.shared.udp().local_addr()?, content).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?)
        }
        pub async fn broadcast(&self, content: Vec<u8>) -> io::Result<()>
        {
            let message: Message = self.new_message(content)?;
            self.shared.seen().lock().unwrap_or_else(|e| e.into_inner()).insert(message.id());
            Self::forward(&self.shared, self.shared.udp().local_addr()?, &message).await;
            Ok(())
        }
    }
}