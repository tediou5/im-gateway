#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Controls<'e>(pub(crate) Vec<Control<'e>>);

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Control<'e> {
    pub(crate) bad_network: Option<()>,
    pub(crate) heartbeat: Option<()>,
    pub(crate) event: Event<'e>,
    pub(crate) number: u8,
}

#[derive(Eq, PartialEq)]
pub(crate) enum Event<'e> {
    Ack(u64),
    Package(u16, u64, &'e [u8]),
    #[allow(dead_code)]
    WeakAck,
    #[allow(dead_code)]
    WeakPackage,
}

impl std::fmt::Debug for Event<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ack(arg0) => f.debug_tuple("Ack").field(arg0).finish(),
            Self::Package(arg0, arg1, _arg2) => {
                f.debug_tuple("Package").field(arg0).field(arg1).finish()
            }
            Self::WeakAck => write!(f, "WeakAck"),
            Self::WeakPackage => write!(f, "WeakPackage"),
        }
    }
}
impl<'e> Control<'e> {
    pub(crate) async fn process(
        pin: &str,
        message: &[u8],
        ack_window: &Option<crate::linker::ack_window::AckWindow<u64>>,
    ) -> anyhow::Result<()> {
        let len = message.len();
        tracing::info!("try into controls");
        let controls: crate::linker::protocol::Controls = message.try_into().map_err(|e| {
            anyhow::anyhow!("[{pin}]control error: try into control protocol error: {e}")
        })?;
        tracing::info!("[{pin}]process {len} len message into controls:\n{controls:?}\n--------------------------------");
        for control in controls.0.into_iter() {
            if control.bad_network.is_some() {
                // TODO: handle for bad network quality
            };

            if control.heartbeat.is_some() {
                let pin_c = pin.to_string();
                tokio::task::spawn_local(async move {
                    if let Some(redis) = crate::REDIS_CLIENT.get() {
                        if let Err(e) = redis.heartbeat(pin_c.clone()).await {
                            tracing::error!("update [{pin_c}] heartbeat error: {e}")
                        };
                    }
                });
            };

            match control.event {
                crate::linker::protocol::control::Event::Ack(trace_id) => {
                    if let Some(ref ack_window) = ack_window {
                        ack_window.ack(pin, trace_id);
                    };
                }
                crate::linker::protocol::control::Event::Package(_len, _trace_id, pkg) => {
                    // TODO: not returned ack to the front end now.
                    let kafka = crate::KAFKA_CLIENT.get().unwrap();

                    kafka
                        .produce(crate::kafka::VecValue(pkg.to_vec()))
                        .await
                        .map_err(|e| anyhow::anyhow!("[{pin}]control Error: Kafka Error: {e}"))?;
                }
                crate::linker::protocol::control::Event::WeakAck => todo!(),
                crate::linker::protocol::control::Event::WeakPackage => todo!(),
            }
        }
        Ok(())
    }
}

impl<'e> TryFrom<&'e [u8]> for Controls<'e> {
    type Error = anyhow::Error;
    fn try_from(value: &'e [u8]) -> anyhow::Result<Controls<'e>> {
        if value.is_empty() {
            return Err(anyhow::anyhow!("value is empty"));
        }
        let mut res = Vec::new();

        let mut body = value;
        let mut len;
        let mut flag = unsafe { *value.get_unchecked(0) };

        loop {
            tracing::info!("serializing next flag : {flag}");
            let number = flag & 0b00001111;

            let ack = ((flag & 0b00100000) >> 5).eq(&1);
            let weak_net_package = ((flag & 0b00010000) >> 4).eq(&1);
            let event = match (ack, weak_net_package) {
                (false, false) => {
                    if number != 1 {
                        return Err(anyhow::anyhow!("PackageNumberMustBeOne:\n{value:?}"));
                    };

                    let mut length_bytes = [0u8; 2];
                    length_bytes.copy_from_slice(&body[1..3]);
                    let length = u16::from_be_bytes(length_bytes);

                    let mut trace_id_bytes = [0u8; 8];
                    trace_id_bytes.copy_from_slice(&body[3..11]);
                    let trace_id = u64::from_be_bytes(trace_id_bytes);

                    if (body.len() as u16) < 11 + length {
                        // The full string has not yet arrived.
                        return Err(anyhow::anyhow!(
                            "Invalid Data: length must bigger than 11 + {length} bytes:\n{value:?}"
                        ));
                    }
                    len = 11 + (length as usize);
                    Event::Package(length, trace_id, &body[11..len])
                }
                (true, false) => {
                    if number != 1 {
                        return Err(anyhow::anyhow!("ACKNumberMustNotBeOne:\n{value:?}"));
                    };
                    len = 8 + 1;
                    if body.len() < len {
                        return Err(anyhow::anyhow!("InvalidBodyLength:\n{value:?}"));
                    };
                    let mut trace_bytes = [0u8; 8];
                    trace_bytes.copy_from_slice(&body[1..len]);
                    let trace_id = u64::from_be_bytes(trace_bytes);

                    Event::Ack(trace_id)
                }
                (false, true) => {
                    // Event::WeakPackage
                    // return Err(anyhow::anyhow!("Weak Package is not supported:\n{value:?}"));
                    return Ok(Controls(res));
                }
                (true, true) => {
                    // Event::WeakAck
                    // return Err(anyhow::anyhow!("WeakAck is not supported:\n{value:?}"));
                    return Ok(Controls(res));
                }
            };

            let heartbeat = ((flag & 0b01000000) >> 6).eq(&1);
            let bad_network = ((flag & 0b10000000) >> 7).eq(&1);

            let bad_network: Option<()> = if bad_network { Some(()) } else { None };
            let heartbeat = if heartbeat { Some(()) } else { None };

            res.push(Control {
                bad_network,
                heartbeat,
                event,
                number,
            });

            if body.len() < (len + 1) {
                break;
            }
            flag = body[len];
            body = &body[len..];
        }

        Ok(Controls(res))
    }
}

#[cfg(test)]
mod test {
    use bytes::BufMut;

    use super::{Control, Controls, Event};

    #[test]
    fn ack_and_package_from_slice() {
        let req = [
            33, 0, 0, 16, 135, 38, 195, 208, 1, 65, 0, 42, 23, 14, 43, 198, 251, 0, 16, 0, 123, 34,
            100, 97, 116, 97, 34, 58, 123, 34, 115, 116, 97, 116, 117, 115, 34, 58, 50, 48, 48,
            125, 44, 34, 112, 114, 111, 116, 111, 99, 111, 108, 34, 58, 34, 72, 101, 97, 114, 116,
            34, 125,
        ];
        let req: Controls = req.as_ref().try_into().unwrap();
        println!("ack: {req:?}");
        assert_eq!(req.0.len(), 2);

        let req = [
            65, 0, 42, 23, 14, 43, 198, 251, 0, 16, 0, 123, 34, 100, 97, 116, 97, 34, 58, 123, 34,
            115, 116, 97, 116, 117, 115, 34, 58, 50, 48, 48, 125, 44, 34, 112, 114, 111, 116, 111,
            99, 111, 108, 34, 58, 34, 72, 101, 97, 114, 116, 34, 125, 33, 0, 0, 16, 135, 38, 195,
            208, 1,
        ];
        let req: Controls = req.as_ref().try_into().unwrap();
        println!("ack: {req:?}");
        assert_eq!(req.0.len(), 2);
    }

    #[test]
    fn ack_from_slice() {
        let flag: u8 = 0b01100001;
        let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
        let trace_id = id_worker.next_id().unwrap();
        let mut dst = bytes::BytesMut::new();

        // Reserve space in the buffer.
        dst.reserve(1 + 8);

        // Write Message to the buffer.
        dst.put_bytes(flag, 1);
        dst.put_u64(trace_id);

        let mut req_control: Controls = dst.as_ref().try_into().unwrap();

        let control = Control {
            bad_network: None,
            heartbeat: Some(()),
            event: Event::Ack(trace_id),
            number: 1,
        };

        assert_eq!(control, req_control.0.pop().unwrap());
    }

    #[test]
    fn package_from_slice() {
        let flag: u8 = 0b00000001;
        let content = "hello world".as_bytes();
        let clen = content.len() as u16;
        let mut id_worker = crate::snowflake::SnowflakeIdWorkerInner::new(1, 1).unwrap();
        let trace_id = id_worker.next_id().unwrap();
        let mut dst = bytes::BytesMut::new();

        // Reserve space in the buffer.
        dst.reserve((11 + clen).into());

        // Write Message to the buffer.
        dst.put_bytes(flag, 1);
        dst.put_u16(clen);
        dst.put_u64(trace_id);
        dst.extend_from_slice(&content);

        let mut req_control: Controls = dst.as_ref().try_into().unwrap();

        let control = Control {
            bad_network: None,
            heartbeat: None,
            event: Event::Package(clen, trace_id, content),
            number: 1,
        };

        assert_eq!(control, req_control.0.pop().unwrap());
    }

    #[test]
    fn test_for_ack() {
        let ack = [33, 0, 0, 16, 135, 38, 195, 208, 1];
        let ack: Controls = ack.as_ref().try_into().unwrap();
        assert_eq!(ack.0.len(), 1);
        println!("ack: {ack:?}");

        let ack = [
            33, 0, 0, 16, 135, 38, 195, 208, 1, 33, 0, 0, 16, 135, 38, 195, 208, 0,
        ];
        let ack: Controls = ack.as_ref().try_into().unwrap();
        assert_eq!(ack.0.len(), 2);
        println!("ack: {ack:?}");
    }

    #[test]
    fn test_for_heartbeat() {
        let heartbeat = [
            65, 0, 42, 23, 14, 43, 198, 251, 0, 16, 0, 123, 34, 100, 97, 116, 97, 34, 58, 123, 34,
            115, 116, 97, 116, 117, 115, 34, 58, 50, 48, 48, 125, 44, 34, 112, 114, 111, 116, 111,
            99, 111, 108, 34, 58, 34, 72, 101, 97, 114, 116, 34, 125, 65, 0, 42, 23, 14, 43, 198,
            251, 0, 16, 0, 123, 34, 100, 97, 116, 97, 34, 58, 123, 34, 115, 116, 97, 116, 117, 115,
            34, 58, 50, 48, 48, 125, 44, 34, 112, 114, 111, 116, 111, 99, 111, 108, 34, 58, 34, 72,
            101, 97, 114, 116, 34, 125,
        ];
        let heartbeat: Controls = heartbeat.as_ref().try_into().unwrap();
        println!("heartbeat: {heartbeat:?}");
        assert!(heartbeat.0.len() == 2);

        let heartbeat = [
            65, 0, 42, 23, 14, 43, 198, 251, 0, 16, 0, 123, 34, 100, 97, 116, 97, 34, 58, 123, 34,
            115, 116, 97, 116, 117, 115, 34, 58, 50, 48, 48, 125, 44, 34, 112, 114, 111, 116, 111,
            99, 111, 108, 34, 58, 34, 72, 101, 97, 114, 116, 34, 125, 0b00110001,
        ];
        let heartbeat: Controls = heartbeat.as_ref().try_into().unwrap();
        assert!(heartbeat.0.len() == 1)
    }

    #[test]
    fn test_for_package() {
        let package = [
            1, 0, 42, 23, 14, 43, 198, 251, 0, 16, 0, 123, 34, 100, 97, 116, 97, 34, 58, 123, 34,
            115, 116, 97, 116, 117, 115, 34, 58, 50, 48, 48, 125, 44, 34, 112, 114, 111, 116, 111,
            99, 111, 108, 34, 58, 34, 72, 101, 97, 114, 116, 34, 125,
        ];

        let package: Controls = package.as_ref().try_into().unwrap();
        println!("package: {package:?}");
        let p2 = [
            1, 1, 163, 255, 255, 255, 255, 191, 194, 160, 0, 123, 34, 112, 114, 111, 116, 111, 99,
            111, 108, 34, 58, 34, 77, 101, 115, 115, 97, 103, 101, 34, 44, 34, 100, 97, 116, 97,
            34, 58, 123, 34, 109, 115, 103, 73, 100, 34, 58, 34, 54, 56, 49, 48, 98, 99, 49, 49,
            48, 99, 51, 98, 52, 56, 48, 99, 56, 57, 51, 48, 56, 53, 101, 102, 102, 51, 100, 100,
            53, 50, 56, 54, 34, 44, 34, 116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 58, 49, 54,
            56, 52, 57, 50, 51, 50, 49, 55, 54, 54, 50, 44, 34, 99, 104, 97, 116, 73, 100, 34, 58,
            34, 54, 52, 54, 99, 56, 49, 54, 54, 50, 52, 98, 97, 102, 48, 54, 54, 101, 49, 48, 56,
            101, 50, 97, 56, 34, 44, 34, 102, 114, 111, 109, 73, 100, 34, 58, 34, 55, 51, 101, 54,
            53, 48, 53, 48, 50, 49, 98, 54, 52, 50, 51, 56, 97, 54, 98, 52, 99, 98, 48, 99, 54,
            102, 48, 98, 54, 100, 52, 100, 34, 44, 34, 97, 112, 112, 73, 100, 34, 58, 34, 83, 86,
            79, 65, 72, 98, 108, 112, 34, 44, 34, 99, 104, 97, 116, 84, 121, 112, 101, 34, 58, 34,
            83, 117, 112, 101, 114, 71, 114, 111, 117, 112, 34, 44, 34, 102, 114, 111, 109, 34, 58,
            123, 34, 112, 105, 110, 34, 58, 34, 55, 51, 101, 54, 53, 48, 53, 48, 50, 49, 98, 54,
            52, 50, 51, 56, 97, 54, 98, 52, 99, 98, 48, 99, 54, 102, 48, 98, 54, 100, 52, 100, 34,
            44, 34, 73, 115, 66, 111, 116, 34, 58, 102, 97, 108, 115, 101, 44, 34, 110, 105, 99,
            107, 110, 97, 109, 101, 34, 58, 34, 116, 101, 115, 116, 32, 117, 115, 101, 114, 34, 44,
            34, 97, 118, 97, 116, 97, 114, 34, 58, 34, 97, 118, 97, 116, 97, 114, 34, 44, 34, 97,
            112, 112, 73, 100, 34, 58, 34, 83, 86, 79, 65, 72, 98, 108, 112, 34, 125, 44, 34, 99,
            104, 97, 116, 77, 115, 103, 84, 121, 112, 101, 34, 58, 34, 83, 101, 115, 115, 105, 111,
            110, 34, 44, 34, 109, 115, 103, 70, 111, 114, 109, 97, 116, 34, 58, 34, 84, 69, 88, 84,
            34, 44, 34, 98, 111, 100, 121, 34, 58, 123, 34, 109, 115, 103, 34, 58, 34, 104, 114,
            100, 97, 115, 104, 97, 119, 101, 92, 110, 34, 125, 125, 125, 1, 1, 163, 255, 255, 255,
            255, 191, 194, 160, 0, 123, 34, 112, 114, 111, 116, 111, 99, 111, 108, 34, 58, 34, 77,
            101, 115, 115, 97, 103, 101, 34, 44, 34, 100, 97, 116, 97, 34, 58, 123, 34, 109, 115,
            103, 73, 100, 34, 58, 34, 54, 56, 49, 48, 98, 99, 49, 49, 48, 99, 51, 98, 52, 56, 48,
            99, 56, 57, 51, 48, 56, 53, 101, 102, 102, 51, 100, 100, 53, 50, 56, 54, 34, 44, 34,
            116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 58, 49, 54, 56, 52, 57, 50, 51, 50, 49,
            55, 54, 54, 50, 44, 34, 99, 104, 97, 116, 73, 100, 34, 58, 34, 54, 52, 54, 99, 56, 49,
            54, 54, 50, 52, 98, 97, 102, 48, 54, 54, 101, 49, 48, 56, 101, 50, 97, 56, 34, 44, 34,
            102, 114, 111, 109, 73, 100, 34, 58, 34, 55, 51, 101, 54, 53, 48, 53, 48, 50, 49, 98,
            54, 52, 50, 51, 56, 97, 54, 98, 52, 99, 98, 48, 99, 54, 102, 48, 98, 54, 100, 52, 100,
            34, 44, 34, 97, 112, 112, 73, 100, 34, 58, 34, 83, 86, 79, 65, 72, 98, 108, 112, 34,
            44, 34, 99, 104, 97, 116, 84, 121, 112, 101, 34, 58, 34, 83, 117, 112, 101, 114, 71,
            114, 111, 117, 112, 34, 44, 34, 102, 114, 111, 109, 34, 58, 123, 34, 112, 105, 110, 34,
            58, 34, 55, 51, 101, 54, 53, 48, 53, 48, 50, 49, 98, 54, 52, 50, 51, 56, 97, 54, 98,
            52, 99, 98, 48, 99, 54, 102, 48, 98, 54, 100, 52, 100, 34, 44, 34, 73, 115, 66, 111,
            116, 34, 58, 102, 97, 108, 115, 101, 44, 34, 110, 105, 99, 107, 110, 97, 109, 101, 34,
            58, 34, 116, 101, 115, 116, 32, 117, 115, 101, 114, 34, 44, 34, 97, 118, 97, 116, 97,
            114, 34, 58, 34, 97, 118, 97, 116, 97, 114, 34, 44, 34, 97, 112, 112, 73, 100, 34, 58,
            34, 83, 86, 79, 65, 72, 98, 108, 112, 34, 125, 44, 34, 99, 104, 97, 116, 77, 115, 103,
            84, 121, 112, 101, 34, 58, 34, 83, 101, 115, 115, 105, 111, 110, 34, 44, 34, 109, 115,
            103, 70, 111, 114, 109, 97, 116, 34, 58, 34, 84, 69, 88, 84, 34, 44, 34, 98, 111, 100,
            121, 34, 58, 123, 34, 109, 115, 103, 34, 58, 34, 104, 114, 100, 97, 115, 104, 97, 119,
            101, 92, 110, 34, 125, 125, 125,
        ];
        println!("p2 len: {}", p2.len());
        let p2: Controls = p2.as_ref().try_into().unwrap();
        println!("package: {p2:?}");
    }
}
