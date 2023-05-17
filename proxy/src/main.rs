#![feature(async_closure)]

mod axum_handler;
mod config;
mod kafka;
mod protocol;
mod redis;
mod socket_addr;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // config path
    #[arg(short, long)]
    config: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let local_addr = socket_addr::ipv4::local_addr().await?;
    println!("local addr: {local_addr:?}");

    let args = <Args as clap::Parser>::parse();
    let config = config::Config::init(args.config);

    let kafka = kafka::Client::new(local_addr.to_string(), config.kafka, config.compress).await?;

    tokio::task::spawn(async {
        kafka
            .consume(config.redis, async move |record, redis, kafka| {
                // FIXME: handle error
                if let Ok(mut proto) = record.record.try_into() {
                    let linkers = match &mut proto {
                        protocol::LinkProtocol::Private(recvs, ref mut message) => {
                            // FIXME: handle error
                            if let Some(dict_path) = kafka
                                .compress
                                .as_ref()
                                .map(|compress| compress.dict.as_str())
                            {
                                let _old = std::mem::replace(
                                    message,
                                    compression::compress(message, dict_path).unwrap(),
                                );
                            }

                            // FIXME: select the linker service by hashring.
                            if recvs.is_empty() {
                                None
                            } else {
                                redis.get_linkers().await.ok()
                            }
                        }
                        protocol::LinkProtocol::Chat(protocol::chat::Action::Join(.., recvs)) => {
                            // FIXME: select the linker service by hashring.
                            if recvs.is_empty() {
                                None
                            } else {
                                redis.get_linkers().await.ok()
                            }
                        }
                        protocol::LinkProtocol::Group(chat, _, ref mut message) => {
                            // FIXME: handle error
                            if let Some(dict_path) = kafka
                                .compress
                                .as_ref()
                                .map(|compress| compress.dict.as_str())
                            {
                                let _old = std::mem::replace(
                                    message,
                                    compression::compress(message, dict_path).unwrap(),
                                );
                            }
                            redis.get_router(chat.as_str()).await.ok()
                        }
                        protocol::LinkProtocol::Chat(protocol::chat::Action::Notice(..)) => {
                            redis.get_linkers().await.ok()
                        }
                        protocol::LinkProtocol::Chat(protocol::chat::Action::Leave(chat, ..)) => {
                            redis.get_router(chat.as_str()).await.ok()
                        }
                    };

                    if let Some(linkers) = linkers {
                        tracing::info!("produce into: {linkers:?}\nmessage: {proto:?}");

                        for linker in linkers {
                            let _ = kafka.produce(linker, proto.clone()).await;
                        }
                    }
                }
            })
            .await;
    });

    println!("starting consume kafka");

    println!("running http server");
    axum_handler::run(config.http).await;
    tracing::error!("closed.");

    Ok(())
}

mod compression {
    pub(crate) fn compress(source: &Vec<u8>, dict_path: &str) -> anyhow::Result<Vec<u8>> {
        let len = source.len();

        let mut compressed = Vec::<u8>::new();
        let dictionary = std::fs::read(dict_path)?;
        let mut encoder = zstd::Encoder::with_dictionary(&mut compressed, 22, &dictionary)?;

        std::io::copy(&mut source.as_slice(), &mut encoder)?;
        encoder.finish()?;

        tracing::info!("origin len: {len}, compressed len: {}", compressed.len());
        Ok(compressed)
    }

    fn _train() {
        let files = vec![
            "../dicts/group1",
            "../dicts/group2",
            "../dicts/group3",
            "../dicts/group1",
            "../dicts/private1",
            "../dicts/private2",
            "../dicts/private3",
        ];
        let dict = zstd::dict::from_files(files, 2048).unwrap();
        std::fs::create_dir_all("../dicts").unwrap();
        std::fs::write("../dicts/dict", dict).unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn zstd_compression() {
        let source = r#"{"data":{"msgId":"0eaacb8c042343e9977091053bb35824","timestamp":1684291426561,"chatId":"6461a8ea1ff3b85d71c351ae","fromId":"b42dad6779074923af3d8b7fbb8db0a1","chatType":"Group","from":{"pin":"b42dad6779074923af3d8b7fbb8db0a1","uid":"","isBot":false,"nickname":"test user","avatar":"avatar","appId":"SVOAHblp","remark":""},"chatMsgType":"Session","appId":"SVOAHblp","msgFormat":"TEXT","noticeType":"","body":"{\"msg\":\"一个小消息\"}","seqId":0},"protocol":"Message"}"#.to_string();
        let source: Vec<u8> = source.as_bytes().to_vec();
        let len = source.len();

        let compressed = crate::compression::compress(&source, "../dicts/dict").unwrap();
        println!("origin len: {len}, compressed len: {}", compressed.len());
    }
}
