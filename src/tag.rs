use std::result;

pub const TAG_LEN: usize = 3;
pub type Error = tagerror::TagError;
pub type Result<T> = result::Result<T, Error>;

const SEND_BSTR: &[u8; TAG_LEN] = b"SND";
const RESPONSE_BSTR: &[u8; TAG_LEN] = b"RSP";
const BROAD_BSTR: &[u8; TAG_LEN] = b"BRD";

pub enum Tag
{
    Send,
    Response,
    Broad
}

impl Into<&'static[u8; TAG_LEN]> for Tag
{
    fn into(self) -> &'static[u8; TAG_LEN] 
    {
        match self {
            Self::Send => SEND_BSTR,
            Self::Response => RESPONSE_BSTR,
            Self::Broad => BROAD_BSTR
        }    
    }
}

impl TryFrom<&[u8]> for Tag
{
    type Error = Error;
    fn try_from(value: &[u8]) -> Result<Self>
    {
        let arr: &[u8; TAG_LEN] = value.try_into().map_err(|_| Self::Error::new(format!("Invalid TAG length: {:?}", value)))?;
        match arr {
            SEND_BSTR => Ok(Self::Send),
            RESPONSE_BSTR => Ok(Self::Response),
            BROAD_BSTR => Ok(Self::Broad),
            _ => Err(Self::Error::new(format!("Unrecognised TAG: {:?}", value))),
        }
    }   
}

mod tagerror
{
    use std::{error::Error, fmt::Display};

    #[derive(Debug)]
    pub struct TagError
    {
        desc: String
    }

    impl Display for TagError 
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result 
        {
            write!(f, "{}", self.desc)
        }
    }

    impl Error for TagError {}

    impl TagError
    {
        pub fn new(desc: String) -> Self
        {
            Self { desc: desc }
        }
    }
}