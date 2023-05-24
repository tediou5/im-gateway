#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Control<'e> {
    pub(crate) bad_network: Option<()>,
    pub(crate) heartbeat: Option<()>,
    pub(crate) event: Event<'e>,
    pub(crate) number: u8,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Event<'e> {
    Ack(Vec<u64>),
    Package(u16, u64, &'e [u8]),
    WeakAck,
    WeakPackage,
}

impl<'e> Control<'e> {
    pub(crate) async fn process(
        pin: &str,
        message: &[u8],
        ack_window: &Option<crate::linker::ack_window::AckWindow<u64>>,
    ) -> anyhow::Result<()> {
        let controls: crate::linker::protocol::Controls = message.try_into().map_err(|e| {
            anyhow::anyhow!("[{pin}]control error: try into control protocol error: {e}")
        })?;
        for control in controls.0.into_iter() {
            if control.bad_network.is_some() {
                // TODO: handle for bad network quality
            };

            if control.heartbeat.is_some() || message.len() < 100 {
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
                crate::linker::protocol::control::Event::Ack(acks) => {
                    if let Some(ref ack_window) = ack_window {
                        for trace_id in acks.into_iter() {
                            let _ = ack_window.ack(trace_id);
                        }
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

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Controls<'e>(pub(crate) Vec<Control<'e>>);

impl<'e> TryFrom<&'e [u8]> for Controls<'e> {
    type Error = anyhow::Error;
    fn try_from(value: &'e [u8]) -> anyhow::Result<Controls<'e>> {
        let mut res = Vec::new();

        let mut body = value;
        let mut flag = value[0];

        loop {
            let mut len = 0usize;

            let number = flag & 0b00001111;

            let ack = ((flag & 0b00100000) >> 5).eq(&1);
            let weak_net_package = ((flag & 0b00010000) >> 4).eq(&1);
            let event = match (ack, weak_net_package) {
                (false, false) => {
                    if number == 0 {
                        return Err(anyhow::anyhow!("PackageNumberMustNotBeZero:\n{value:?}"));
                    };

                    let mut length_bytes = [0u8; 2];
                    length_bytes.copy_from_slice(&value[1..3]);
                    let length = u16::from_be_bytes(length_bytes);

                    let mut trace_id_bytes = [0u8; 8];
                    trace_id_bytes.copy_from_slice(&value[3..11]);
                    let trace_id = u64::from_be_bytes(trace_id_bytes);

                    if (value.len() as u16) < 11 + length {
                        // The full string has not yet arrived.
                        return Err(anyhow::anyhow!(
                            "Invalid Data: length must bigger than 11 + {length} bytes:\n{value:?}"
                        ));
                    }
                    len = 11 + length as usize;
                    Event::Package(length, trace_id, &value[11..(11 + length as usize)])
                }
                (true, false) => {
                    if number == 0 {
                        return Err(anyhow::anyhow!("ACKNumberMustNotBeZero:\n{value:?}"));
                    };
                    len = number as usize * 8;
                    if body.len() < len + 1 {
                        return Err(anyhow::anyhow!("InvalidBodyLength:\n{value:?}"));
                    };
                    let ack_body = &body[1..=len];
                    println!("ack body: {ack_body:?}");
                    len += 1;
                    Event::Ack(
                        ack_body
                            .chunks_exact(8)
                            .map(|bytes| {
                                let mut length_bytes = [0u8; 8];
                                length_bytes.copy_from_slice(bytes);
                                u64::from_be_bytes(length_bytes)
                            })
                            .collect(),
                    )
                }
                (false, true) => Event::WeakPackage,
                (true, true) => Event::WeakAck,
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

            if body.len() < len + 1 {
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
            event: Event::Ack(vec![trace_id]),
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
        let ack = [
            33, 0, 0, 16, 135, 38, 195, 208, 1, 33, 0, 0, 16, 135, 38, 195, 208, 0,
        ];
        let ack: Controls = ack.as_ref().try_into().unwrap();
        println!("ack: {ack:?}");
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
            100, 97, 115, 104, 97, 119, 101, 92, 110, 34, 125, 125, 125,
        ];
        println!("p2 len: {}", p2.len());
        let p2: Controls = p2.as_ref().try_into().unwrap();
        println!("package: {p2:?}");
    }
}
