use ratatui::{prelude::*, widgets::*};
use unicode_width::UnicodeWidthStr;
use crate::app::App;

pub fn draw(f: &mut Frame, app: &mut App)
{
    let [main, input, status] = Layout::vertical([Constraint::Min(3), Constraint::Length(3), Constraint::Length(1)]).areas(f.area());
    let [chat, side] = Layout::horizontal([Constraint::Fill(3), Constraint::Max(34)]).areas(main);
    draw_chat(f, app, chat);
    draw_peers(f, app, side);
    draw_input(f, app, input);
    draw_status(f, app, status);
}

fn draw_chat(f: &mut Frame, app: &mut App, area: Rect)
{
    let block = Block::bordered().title(" Messages ");
    let inner = block.inner(area);
    let mut lines: Vec<Line> = Vec::new();
    for m in &app.messages 
    {
        let ts = m.at.format("%H:%M:%S ").to_string();
        let who = m.from.map_or("you".to_owned(), |a| a.to_string());
        let prefix = format!("{ts}{who}: ");
        let pw = prefix.width();
        let wrap_w = (inner.width as usize).saturating_sub(pw).max(1);
        let color = if m.from.is_some() { Color::Cyan } else { Color::Green };
        for (i, part) in textwrap::wrap(&m.text, wrap_w).into_iter().enumerate() 
        {
            let head = if i == 0 {
                Span::styled(prefix.clone(), Style::new().fg(color))
            } else {
                Span::raw(" ".repeat(pw))
            };
            lines.push(Line::from(vec![head, Span::raw(part.into_owned())]));
        }
    }
    let h = inner.height as usize;
    let max_scroll = lines.len().saturating_sub(h);
    app.scroll = app.scroll.min(max_scroll);
    let end = lines.len() - app.scroll;
    let start = end.saturating_sub(h);
    let title = if app.scroll > 0 { format!(" Messages (↑{}) ", app.scroll) } else { " Messages ".into() };
    f.render_widget(Paragraph::new(lines[start..end].to_vec()).block(block.title(title)), area);
}

fn draw_peers(f: &mut Frame, app: &App, area: Rect)
{
    let items: Vec<ListItem> = app.peers.iter()
        .map(|(a, n)| ListItem::new(format!("{n}  {a}"))).collect();
    let title = format!(" Peer ({}) ", app.peers.len());
    f.render_widget(List::new(items).block(Block::bordered().title(title)), area);
}

fn draw_input(f: &mut Frame, app: &App, area: Rect)
{
    f.render_widget(Paragraph::new(app.input.as_str()).block(Block::bordered().title(" > ")), area);
    let before: String = app.input.chars().take(app.cursor).collect();
    let x = (area.x + 1 + before.width() as u16).min(area.right().saturating_sub(2));
    f.set_cursor_position((x, area.y + 1));
}

fn draw_status(f: &mut Frame, app: &App, area: Rect)
{
    let st = if app.connected { "connected" } else { "DISCONNECTED" };
    let text = format!(" {} | {} | Esc/Ctrl+C exit | Up/Dn scroll ", app.name, st);
    f.render_widget(Paragraph::new(text).style(Style::new().reversed()), area);
}