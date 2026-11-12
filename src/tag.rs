use crate::tagerror::TagError;

pub const TAG_LEN: usize = 3;

pub enum Tag
{
    Send,
    Response,
    Plain
}

impl Into<[u8; TAG_LEN]> for Tag
{
    fn into(self) -> [u8; TAG_LEN] 
    {
        match self {
            Self::Send => b"SND".to_owned(),
            Self::Response => b"RSP".to_owned(),
            Self::Plain => b"PLN".to_owned()
        }    
    }
}

impl TryFrom<&[u8]> for Tag
{
    type Error = TagError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> 
    {
        match value {
            b"SND" => Ok(Self::Send),
            b"RSP" => Ok(Self::Response),
            b"PLN" => Ok(Self::Plain),
            _ => Err(TagError::new(format!("Unrecognised TAG: {:?}", value)))
        }
    }   
}