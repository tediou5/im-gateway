pub(crate) mod ipv4 {
    use std::net::{Ipv4Addr, SocketAddrV4};

    pub(crate) async fn local_addr() -> anyhow::Result<Ipv4Addr> {
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;

        socket.connect("1.1.1.1:80").await?;

        match socket.local_addr()? {
            std::net::SocketAddr::V4(addr) => Ok(*addr.ip()),
            _ => Err(anyhow::anyhow!("ipv6 is invalid")),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_u64(socket: SocketAddrV4) -> u64 {
        let ip = socket.ip().octets();
        let port = socket.port();

        let mut ipv4_u64 = 0u64;
        ip.into_iter().for_each(|octet| {
            ipv4_u64 = ipv4_u64 << 8 | octet as u64;
        });

        ipv4_u64 << 16 | port as u64
    }

    #[allow(dead_code)]
    pub(crate) fn from_u64(ipv4_u64: u64) -> SocketAddrV4 {
        let ip_u32 = ((ipv4_u64 & 0xffffffffffff0000u64) >> 16) as u32;
        let port = (ipv4_u64 & 0xffff) as u16;

        let ip = std::net::Ipv4Addr::new(
            ((ip_u32 & 0xff000000u32) >> 24) as u8,
            ((ip_u32 & 0xff0000u32) >> 16) as u8,
            ((ip_u32 & 0xff00u32) >> 8) as u8,
            (ip_u32 & 0xffu32) as u8,
        );
        SocketAddrV4::new(ip, port)
    }

    #[cfg(test)]
    mod test {
        use super::{from_u64, into_u64};

        #[test]
        fn ipv4_to_u64() {
            use std::net::{Ipv4Addr, SocketAddrV4};
            let socket = SocketAddrV4::new(Ipv4Addr::new(255, 254, 253, 252), 65535);
            let ipv4_u64 = into_u64(socket);
            let ip = from_u64(ipv4_u64);

            assert_eq!(ip, socket)
        }
    }
}
