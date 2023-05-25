pub(crate) use protocol::content::Content;

mod ack_window;
mod auth;
mod protocol;
pub(crate) mod tcp;
pub(crate) mod websocket;

pub(crate) struct Login {
    platform: Platform,
    auth_message: crate::linker::Content,
}

#[derive(Debug)]
pub(crate) enum Platform {
    App(tokio::net::TcpStream),
    Pc(tokio::net::TcpStream),
    Web(axum::extract::ws::WebSocket),
}

#[derive(Debug, Clone)]
pub(crate) struct User {
    pub(crate) pin: std::rc::Rc<String>,
    pub(crate) pc: std::rc::Rc<std::cell::RefCell<Option<tcp::Sender>>>,
    pub(crate) app: std::rc::Rc<std::cell::RefCell<Option<tcp::Sender>>>,
    pub(crate) web: std::rc::Rc<std::cell::RefCell<Option<websocket::Sender>>>,
}

impl std::hash::Hash for User {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pin.as_str().hash(state);
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
    pub(crate) fn update(&self, login: Login) {
        let Login {
            platform,
            auth_message,
        } = login;
        match platform {
            Platform::App(stream) => {
                let sender = tcp::process(stream, self.pin.clone(), auth_message);
                if let Some(old) = self.app.replace(Some(sender)) {
                    tracing::error!("{}: remove old > app < connection", self.pin.as_str());
                    let _ = old.send(tcp::Event::Close);
                };
            }
            Platform::Pc(stream) => {
                let sender = tcp::process(stream, self.pin.clone(), auth_message);
                if let Some(old) = self.pc.replace(Some(sender)) {
                    tracing::error!("{}: remove old > pc < connection", self.pin.as_str());
                    let _ = old.send(tcp::Event::Close);
                };
            }
            Platform::Web(socket) => {
                let sender = websocket::process(socket, self.pin.clone(), auth_message);
                if let Some(old) = self.web.replace(Some(sender)) {
                    tracing::error!("{}: remove old > web < connection", self.pin.as_str());
                    let _ = old.send(websocket::Event::Close);
                };
            }
        }
    }

    pub(crate) fn send(
        &self,
        trace_id: u64,
        message_bytes: &std::rc::Rc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mut flag = 0;

        let mut app = self.app.borrow_mut();
        if let Some(sender) = app.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(trace_id, message_bytes.clone())) {
            tracing::error!("{}: app send failed", self.pin);
            app.take();
            flag -= 1;
        };

        let mut pc = self.pc.borrow_mut();
        if let Some(sender) = pc.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(tcp::Event::WriteBatch(trace_id, message_bytes.clone())) {
            tracing::error!("{}: pc send failed", self.pin);
            pc.take();
            flag -= 1;
        };

        let mut web = self.web.borrow_mut();
        if let Some(sender) = web.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(websocket::Event::WriteBatch(trace_id, message_bytes.clone())) {
            tracing::error!("{}: web send failed", self.pin);
            web.take();
            flag -= 1;
        };

        if flag == 0 {
            let pin = self.pin.clone();
            tokio::task::spawn_local(async move {
                if let Some(redis) = crate::REDIS_CLIENT.get() {
                    if let Err(e) = redis.del_heartbeat(pin.to_string()).await {
                        tracing::error!("del [{pin}] heartbeat error: {e}",)
                    };
                }
            });
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
}
