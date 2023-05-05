pub(crate) use message::{Content, Message, MessageCodec};

mod auth;
mod message;
pub(crate) mod tcp;
pub(crate) mod websocket;

#[derive(Debug)]
pub(crate) enum Event {
    Write(std::sync::Arc<Message>),
    WriteBatch(std::sync::Arc<Vec<u8>>),
    Close,
}

#[allow(dead_code)]
pub(crate) enum Platform {
    App(crate::Sender),
    Web(crate::Sender),
    PC(crate::Sender),
}

#[allow(dead_code)]
pub(crate) struct User {
    app: Option<crate::Sender>,
    web: Option<crate::Sender>,
    pc: Option<crate::Sender>,
}
