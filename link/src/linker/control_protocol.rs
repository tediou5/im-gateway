pub(super) struct Control<'e> {
    pub(super) bad_network: Option<()>,
    pub(super) heartbeat: Option<()>,
    pub(super) event: Event<'e>,
    pub(super) length: u8,
}

pub(super) enum Event<'e> {
    Ack(Vec<&'e [u8]>),
    Package(&'e [u8]),
    WeakAck,
    WeakPackage,
}

impl<'e> TryFrom<&'e [u8]> for Control<'e> {
    type Error = anyhow::Error;
    fn try_from(value: &'e [u8]) -> anyhow::Result<Control<'e>> {
        let flag = value[0];
        let number = flag & 0b00001111;
        let body = &value[1..];

        let ack = ((flag & 0b00100000) >> 5).eq(&1);
        let weak_net_package = ((flag & 0b00010000) >> 4).eq(&1);
        let event = match (ack, weak_net_package) {
            (true, false) => {
                if body.len() % number as usize != 0 {
                    return Err(anyhow::anyhow!("InvalidBodyLength"));
                };
                let len = body.len() / number as usize;
                Event::Ack(body.chunks_exact(len).collect())
            }
            (false, true) => Event::WeakPackage,
            (true, true) => Event::WeakAck,
            (false, false) => Event::Package(value),
        };

        let heartbeat = ((flag & 0b01000000) >> 6).eq(&1);
        let bad_network = ((flag & 0b10000000) >> 7).eq(&1);

        let bad_network: Option<()> = if bad_network { Some(()) } else { None };
        let heartbeat = if heartbeat { Some(()) } else { None };

        Ok(Self {
            bad_network,
            heartbeat,
            event,
            length: number,
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn control_from_slice() {
        let flag: u8 = 0b01110011;
        let req = vec![flag, 2, 3, 4];
    }
}
