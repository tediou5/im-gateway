pub(crate) use message::{Content, Message, MessageCodec};

mod auth;
mod message;
pub(crate) mod tcp;
pub(crate) mod websocket;

#[derive(Debug)]
pub(crate) enum Event {
    // Write(std::sync::Arc<Message>),
    WriteBatch(std::sync::Arc<Vec<Message>>),
    Close,
}
