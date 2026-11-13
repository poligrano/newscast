use crate::tagerror::TagError;

pub const TAG_LEN: usize = 3;
pub type Error = TagError;

const SEND_BSTR: &[u8; TAG_LEN] = b"SND";
const RESPONSE_BSTR: &[u8; TAG_LEN] = b"RSP";
const PLAIN_BSTR: &[u8; TAG_LEN] = b"PLN";

pub enum Tag
{
    Send,
    Response,
    Plain
}

impl Into<&'static[u8; TAG_LEN]> for Tag
{
    fn into(self) -> &'static[u8; TAG_LEN] 
    {
        match self {
            Self::Send => SEND_BSTR,
            Self::Response => RESPONSE_BSTR,
            Self::Plain => PLAIN_BSTR
        }    
    }
}

impl TryFrom<&[u8]> for Tag
{
    type Error = TagError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> 
    {
        let arr: &[u8; TAG_LEN] = value.try_into().map_err(|_| Self::Error::new(format!("Invalid TAG length: {:?}", value)))?;
        match arr {
            SEND_BSTR => Ok(Self::Send),
            RESPONSE_BSTR => Ok(Self::Response),
            PLAIN_BSTR => Ok(Self::Plain),
            _ => Err(Self::Error::new(format!("Unrecognised TAG: {:?}", value))),
        }
    }   
}