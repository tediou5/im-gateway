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

    pub(crate) fn update(&self, platform: Platform) {
        match platform {
            Platform::App(sender) => {
                if let Some(old) = self.app.replace(Some(sender)) {
                    tracing::error!("remove old app connection");
                    let _ = old.send(tcp::Event::Close);
                };
            }
            Platform::Pc(sender) => {
                if let Some(old) = self.pc.replace(Some(sender)) {
                    tracing::error!("remove old pc connection");
                    let _ = old.send(tcp::Event::Close);
                };
            }
            Platform::Web(sender) => {
                if let Some(old) = self.web.replace(Some(sender)) {
                    tracing::error!("remove old web connection");
                    let _ = old.send(websocket::Event::Close);
                };
            }
        }
    }
    pub(crate) fn send(&self, message: std::sync::Arc<Message>) -> anyhow::Result<()> {
        let mut flag = 0;
        if let Some(sender) = self.app.borrow_mut().as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(message.as_ref().into())) {
            self.app.replace(None);
            flag -= 1
        };
        if let Some(sender) = self.pc.borrow_mut().as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(message.as_ref().into())) {
            self.pc.replace(None);
            flag -= 1
        };
        if let Some(sender) = self.web.borrow_mut().as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(websocket::Event::Write(message)) {
            self.web.replace(None);
            flag -= 1
        };

        if flag == 0 {
            Err(anyhow::anyhow!("user all links has been disconnected"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn send_batch(
        &self,
        messages: std::sync::Arc<Vec<String>>,
        messages_bytes: std::sync::Arc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mut flag = 0;
        if let Some(sender) = self.app.borrow_mut().as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(messages_bytes.clone())) {
            self.app.replace(None);
            flag -= 1
        };
        if let Some(sender) = self.web.borrow_mut().as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(websocket::Event::WriteBatch(messages)).inspect(|_|flag += 1) {
            self.web.replace(None);
            flag -= 1
        };
        if let Some(sender) = self.pc.borrow_mut().as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(messages_bytes)) {
            self.pc.replace(None);
            flag -= 1
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
