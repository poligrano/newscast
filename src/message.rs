use std::{net::SocketAddr, result};
use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use crate::{newspaper, tag::self};

pub const MAX_CONTENT: usize = newspaper::MAX_DATAGRAM - tag::TAG_LEN - size_of::<u64>() - size_of::<u8>() - size_of::<SocketAddr>() - size_of::<usize>();
pub type Error = messageerror::MessageError;
pub type Result<T> = result::Result<T, Error>;

pub(crate) type IdType = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message
{
    id: IdType,
    ttl: u8,
    origin: SocketAddr,
    content: Bytes
}

impl Message
{
    pub fn new(ttl: u8, origin: SocketAddr, content: impl Into<Bytes>) -> Result<Self>
    {
        let content: Bytes = content.into();
        (content.len() <= MAX_CONTENT).then(|| Self { id: rand::rng().next_u64(), ttl: ttl, origin: origin, content: Bytes::from(content) }).ok_or_else(|| Error::new("Content max length exceeded"))
    }
    pub fn as_forwarded(&self) -> Option<Self>
    {
        self.ttl.checked_sub(1).map(|ttl| Self { ttl: ttl, ..self.clone() })
    }
    pub fn id(&self) -> IdType
    {
        self.id
    }
    pub fn ttl(&self) -> u8
    {
        self.ttl
    }
    pub fn origin(&self) -> SocketAddr
    {
        self.origin
    }
    pub fn content(&self) -> &[u8]
    {
        &self.content
    }
}

mod messageerror
{
    use std::{error::Error, fmt::Display};

    #[derive(Debug)]
    pub struct MessageError
    {
        desc: String
    }

    impl Display for MessageError 
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result 
        {
            write!(f, "{}", self.desc)
        }
    }

    impl Error for MessageError {}

    impl MessageError
    {
        pub fn new(desc: impl Into<String>) -> Self
        {
            Self { desc: desc.into() }
        }
    }
}