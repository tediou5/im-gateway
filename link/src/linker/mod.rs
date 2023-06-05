mod ack_window;
mod auth;
mod protocol;
pub(crate) mod tcp;
pub(crate) mod websocket;

pub(crate) use protocol::{content::Content, control::Control};

pub(crate) type Sender = local_sync::mpsc::unbounded::Tx<Event>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    WriteBatch(u64, std::rc::Rc<Vec<u8>>),
    Close(bool /* need_reconnect */, String /* reason */),
}

#[derive(Debug, Clone)]
pub(crate) enum SenderEvent {
    WriteBatch(std::rc::Rc<Vec<u8>>),
    Close(bool /* need_reconnect */, String /* reason */),
}

pub(crate) struct Login {
    platform: Platform,
    auth_message: crate::linker::Content,
}

pub(crate) enum Platform {
    App(tokio::net::TcpStream),
    Pc(tokio::net::TcpStream),
    Web(axum::extract::ws::WebSocket),
}

struct Connection {
    sender: Sender,
    read_handler: tokio::task::JoinHandle<()>,
    write_hander: tokio::task::JoinHandle<()>,
}

impl Connection {
    fn new(
        sender: Sender,
        read_handler: tokio::task::JoinHandle<()>,
        write_hander: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            sender,
            read_handler,
            write_hander,
        }
    }

    fn send(&self, event: Event) -> anyhow::Result<()> {
        self.sender
            .send(event)
            .map_err(|_e| anyhow::anyhow!("connect send err"))
    }

    fn about(&self) {
        self.read_handler.abort();
        self.write_hander.abort();
    }

    fn about_with_event(self, close_event: Event) {
        let _ = self.send(close_event);
        tokio::task::spawn_local(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            self.about()
        });
    }
}

pub(crate) struct User {
    pub(crate) pin: std::rc::Rc<String>,
    pc: Option<Connection>,
    app: Option<Connection>,
    web: Option<Connection>,
}

impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("User").field(&self.pin).finish()
    }
}

impl std::hash::Hash for User {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pin.as_str().hash(state);
    }
}

impl Eq for User {}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        self.pin.as_str() == other.pin.as_str()
    }
}

impl User {
    pub(crate) fn from_pin(pin: std::rc::Rc<String>) -> Self {
        Self {
            pin,
            app: None,
            web: None,
            pc: None,
        }
    }

    // TODO: handle socket stream here, return true is update
    pub(crate) fn update(&mut self, login: Login) -> bool {
        let Login {
            platform,
            auth_message,
        } = login;
        match platform {
            Platform::App(stream) => {
                let (sender, read_handler, write_hander) =
                    tcp::process(stream, self.pin.clone(), auth_message);
                if let Some(old) =
                    self.app
                        .replace(Connection::new(sender, read_handler, write_hander))
                {
                    tracing::error!("{}: remove old > app < connection", self.pin.as_str());
                    old.about_with_event(Event::Close(false, "other device connected".to_string()));
                    return true;
                };
            }
            Platform::Pc(stream) => {
                let (sender, read_handler, write_hander) =
                    tcp::process(stream, self.pin.clone(), auth_message);
                if let Some(old) =
                    self.pc
                        .replace(Connection::new(sender, read_handler, write_hander))
                {
                    tracing::error!("{}: remove old > pc < connection", self.pin.as_str());
                    old.about_with_event(Event::Close(false, "other device connected".to_string()));
                    return true;
                };
            }
            Platform::Web(socket) => {
                let (sender, read_handler, write_hander) =
                    websocket::process(socket, self.pin.clone(), auth_message);
                if let Some(old) =
                    self.web
                        .replace(Connection::new(sender, read_handler, write_hander))
                {
                    tracing::error!("{}: remove old > web < connection", self.pin.as_str());
                    old.about_with_event(Event::Close(false, "other device connected".to_string()));
                    return true;
                };
            }
        }
        false
    }

    pub(crate) fn send(
        &mut self,
        trace_id: u64,
        message_bytes: &std::rc::Rc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mut flag = 0;

        if let Some(sender) = self.app.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(Event::WriteBatch(trace_id, message_bytes.clone())) {
            tracing::error!("{}: app send failed", self.pin);
            sender.about();
            self.app = None;
            flag -= 1;
        };

        if let Some(sender) = self.pc.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(Event::WriteBatch(trace_id, message_bytes.clone())) {
            tracing::error!("{}: pc send failed", self.pin);
            sender.about();
            self.app = None;
            flag -= 1;
        };

        if let Some(sender) = self.web.as_ref().inspect(|_|flag += 1) &&
        let Err(_) = sender.send(Event::WriteBatch(trace_id, message_bytes.clone())) {
            tracing::error!("{}: web send failed", self.pin);
            sender.about();
            self.app = None;
            flag -= 1;
        };

        if flag == 0 {
            tracing::error!("{}: user all links has been disconnected ", self.pin);
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
}
