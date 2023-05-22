pub(crate) use message::{Content, Message, MessageCodec};

mod ack_window;
mod auth;
mod control_protocol;
mod message;
pub(crate) mod tcp;
pub(crate) mod websocket;

#[derive(Debug)]
pub(crate) enum Platform {
    App(tokio::net::TcpStream),
    Pc(tokio::net::TcpStream),
    Web(axum::extract::ws::WebSocket),
}

#[derive(Debug, Clone)]
pub(crate) struct User {
    pub(crate) pin: std::rc::Rc<String>,
    pub(crate) app: std::rc::Rc<std::cell::RefCell<Option<tcp::Sender>>>,
    pub(crate) web: std::rc::Rc<std::cell::RefCell<Option<websocket::Sender>>>,
    pub(crate) pc: std::rc::Rc<std::cell::RefCell<Option<tcp::Sender>>>,
}

impl std::hash::Hash for User {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pin.hash(state);
    }
}

impl Eq for User {}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        let app_eq = match (self.app.borrow().as_ref(), other.app.borrow().as_ref()) {
            (None, None) => true,
            (Some(s), Some(o)) => s.same_channel(o),
            (None, Some(_)) | (Some(_), None) => false,
        };
        let web_eq = match (self.web.borrow().as_ref(), other.web.borrow().as_ref()) {
            (None, None) => true,
            (Some(s), Some(o)) => s.same_channel(o),
            (None, Some(_)) | (Some(_), None) => false,
        };
        let pc_eq = match (self.pc.borrow().as_ref(), other.pc.borrow().as_ref()) {
            (None, None) => true,
            (Some(s), Some(o)) => s.same_channel(o),
            (None, Some(_)) | (Some(_), None) => false,
        };
        app_eq && web_eq && pc_eq
    }
}

impl User {
    pub(crate) fn from_pin(pin: std::rc::Rc<String>) -> Self {
        Self {
            pin,
            app: std::rc::Rc::new(std::cell::RefCell::new(None)),
            web: std::rc::Rc::new(std::cell::RefCell::new(None)),
            pc: std::rc::Rc::new(std::cell::RefCell::new(None)),
        }
    }

    // TODO: handle socket stream here
    pub(crate) fn update(&self, platform: Platform) {
        match platform {
            Platform::App(stream) => {
                let sender = tcp::process(stream, self.pin.clone());
                if let Some(old) = self.app.replace(Some(sender)) {
                    tracing::error!("{}: remove old > app < connection", self.pin.as_str());
                    let _ = old.send(tcp::Event::Close);
                };
            }
            Platform::Pc(stream) => {
                let sender = tcp::process(stream, self.pin.clone());
                if let Some(old) = self.pc.replace(Some(sender)) {
                    tracing::error!("{}: remove old > pc < connection", self.pin.as_str());
                    let _ = old.send(tcp::Event::Close);
                };
            }
            Platform::Web(socket) => {
                let sender = websocket::process(socket, self.pin.clone());
                if let Some(old) = self.web.replace(Some(sender)) {
                    tracing::error!("{}: remove old > web < connection", self.pin.as_str());
                    let _ = old.send(websocket::Event::Close);
                };
            }
        }
    }

    pub(crate) fn send(
        &self,
        message_bytes: &std::rc::Rc<Vec<u8>>,
        content: &mut Option<std::rc::Rc<String>>,
        // message_bytes: &std::sync::Arc<Vec<u8>>,
        // content: &mut Option<std::sync::Arc<String>>,
    ) -> anyhow::Result<()> {
        let mut flag = 0;

        let mut app = self.app.borrow_mut();
        if let Some(sender) = app.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(message_bytes.clone())) {
            tracing::error!("{}: tcp send failed", self.pin);
            app.take();
            flag -= 1;
        };

        let mut pc = self.pc.borrow_mut();
        if let Some(sender) = pc.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(message_bytes.clone())) {
            tracing::error!("{}: tcp send failed", self.pin);
            pc.take();
            flag -= 1;
        };

        let mut web = self.web.borrow_mut();
        if let Some(sender) = web.as_ref().inspect(|_| flag += 1) {
            if content.is_none() {
                *content = Some(std::rc::Rc::new(
                    // *content = Some(std::sync::Arc::new(
                    String::from_utf8_lossy(&message_bytes.as_ref()[44..]).to_string(),
                ))
            }

            if let Some(content) = content &&
            let Err(_) = sender.send(websocket::Event::WriteBatch(content.clone())) {
                tracing::error!("{}: websocket send failed", self.pin);
                web.take();
                flag -= 1;
            }
        };

        if flag == 0 {
            Err(anyhow::anyhow!("user all links has been disconnected"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn close(&self) {
        if let Some(sender) = self.app.borrow().as_ref()  &&
        let Err(_e) = sender.send(tcp::Event::Close) {
            // TODO:
        }
        if let Some(sender) = self.pc.borrow().as_ref()  &&
        let Err(_e) = sender.send(tcp::Event::Close) {
            // TODO:
        }
        if let Some(sender) = self.web.borrow().as_ref()  &&
        let Err(_e) = sender.send(websocket::Event::Close) {
            // TODO:
        }
    }

    // pub(crate) fn flush(&mut self) -> anyhow::Result<()> {
    //     if let Some(sender) = self.app.as_ref() &&
    //     let Err(_) = sender.send(tcp::Event::Flush) {
    //         self.app = None;
    //     };

    //     if let Some(sender) = self.pc.as_ref() &&
    //     let Err(_) = sender.send(tcp::Event::Flush) {
    //         self.pc = None;
    //     };

    //     if let None = self.app &&
    //     let None = self.web &&
    //     let None = self.pc {
    //         Err(anyhow::anyhow!("user all links has been disconnected"))
    //     } else {
    //         Ok(())
    //     }
    // }
}
