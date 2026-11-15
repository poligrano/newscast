use std::net::SocketAddr;
use chrono::{DateTime, Local};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

pub struct ChatLine
{
    pub at: DateTime<Local>,
    pub from: Option<SocketAddr>,
    pub text: String,
}

pub enum Action 
{ 
    None, 
    Send(String), 
    Quit 
}

pub struct App
{
    pub name: String,
    pub messages: Vec<ChatLine>,
    pub input: String,
    pub cursor: usize,
    pub scroll: usize,
    pub peers: Vec<(SocketAddr, String)>,
    pub connected: bool,
}

impl App
{
    pub fn new(name: String) -> Self
    {
        Self { name, messages: Vec::new(), input: String::new(), cursor: 0, scroll: 0, peers: Vec::new(), connected: true }
    }
    pub fn push(&mut self, from: Option<SocketAddr>, text: String)
    {
        self.messages.push(ChatLine { at: Local::now(), from, text });
        self.scroll = 0;
    }
    fn byte_idx(&self) -> usize
    {
        self.input.char_indices().nth(self.cursor).map_or(self.input.len(), |(i, _)| i)
    }
    pub fn on_key(&mut self, k: KeyEvent) -> Action
    {
        let ctrl = k.modifiers.contains(KeyModifiers::CONTROL);
        match k.code {
            KeyCode::Esc => return Action::Quit,
            KeyCode::Char('c') | KeyCode::Char('d') if ctrl => return Action::Quit,
            KeyCode::Char('u') if ctrl => { self.input.clear(); self.cursor = 0; }
            KeyCode::Char(c) if !ctrl => {
                self.input.insert(self.byte_idx(), c);
                self.cursor += 1;
            }
            KeyCode::Backspace if self.cursor > 0 => {
                self.cursor -= 1;
                self.input.remove(self.byte_idx());
            }
            KeyCode::Delete if self.cursor < self.input.chars().count() => {
                self.input.remove(self.byte_idx());
            }
            KeyCode::Left => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Right => self.cursor = (self.cursor + 1).min(self.input.chars().count()),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.input.chars().count(),
            KeyCode::Up => self.scroll = self.scroll.saturating_add(5),
            KeyCode::Down => self.scroll = self.scroll.saturating_sub(5),
            KeyCode::Enter => {
                let text: String = self.input.trim().to_owned();
                self.input.clear();
                self.cursor = 0;
                if text == "/quit" 
                { 
                    return Action::Quit; 
                }
                if !text.is_empty() 
                { 
                    return Action::Send(text); 
                }
            }
            _ => {}
        }
        Action::None
    }
}