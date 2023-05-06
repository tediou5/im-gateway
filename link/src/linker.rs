pub(crate) use message::{Content, Message, MessageCodec};

mod auth;
mod message;
pub(crate) mod tcp;
pub(crate) mod websocket;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum Platform {
    App(tcp::Sender),
    Pc(tcp::Sender),
    Web(websocket::Sender),
}

impl Platform {
    pub(crate) fn send_one(&self, message: std::sync::Arc<Message>) -> anyhow::Result<()> {
        match self {
            Platform::App(sender) => sender.send(tcp::Event::Write(message))?,
            Platform::Pc(sender) => sender.send(tcp::Event::Write(message))?,
            Platform::Web(sender) => sender.send(websocket::Event::Write(message))?,
        };
        Ok(())
    }

    pub(crate) fn close(&self) -> anyhow::Result<()> {
        match self {
            Platform::App(sender) => sender.send(tcp::Event::Close)?,
            Platform::Pc(sender) => sender.send(tcp::Event::Close)?,
            Platform::Web(sender) => sender.send(websocket::Event::Close)?,
        };
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
pub(crate) struct User {
    pub(crate) app: Option<tcp::Sender>,
    pub(crate) web: Option<websocket::Sender>,
    pub(crate) pc: Option<tcp::Sender>,
}

impl User {
    pub(crate) fn send(&mut self, message: std::sync::Arc<Message>) -> anyhow::Result<()> {
        if let Some(sender) = self.app.as_ref() &&
        let Err(_) = sender.send(tcp::Event::Write(message.clone())) {
            self.app = None;
        };

        if let Some(sender) = self.web.as_ref() &&
        let Err(_) = sender.send(websocket::Event::Write(message.clone())) {
            self.web = None;
        };

        if let Some(sender) = self.pc.as_ref() &&
        let Err(_) = sender.send(tcp::Event::Write(message.clone())) {
            self.pc = None;
        };

        if let None = self.app &&
        let None = self.web &&
        let None = self.pc {
            Err(anyhow::anyhow!("user all links has been disconnected"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn send_batch(
        &mut self,
        messages: std::sync::Arc<Vec<String>>,
        messages_bytes: std::sync::Arc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        if let Some(sender) = self.app.as_ref() &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(messages_bytes.clone())) {
            self.app = None;
        };

        if let Some(sender) = self.web.as_ref() &&
        let Err(_) = sender.send(websocket::Event::WriteBatch(messages)) {
            self.web = None;
        };

        if let Some(sender) = self.pc.as_ref() &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(messages_bytes.clone())) {
            self.pc = None;
        };

        if let None = self.app &&
        let None = self.web &&
        let None = self.pc {
            Err(anyhow::anyhow!("user all links has been disconnected"))
        } else {
            Ok(())
        }
    }
}
