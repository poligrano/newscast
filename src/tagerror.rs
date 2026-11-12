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