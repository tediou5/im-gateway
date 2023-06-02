#[cfg(test)]
mod test {
    use ahash::HashMap;
    use josekit::{
        jws::{JwsHeader, HS256},
        jwt::{self, JwtPayload},
        JoseError,
    };

    #[test]
    fn parse_token() {
        use base64::{engine::general_purpose, Engine as _};

        let key: [u8; 32] = [
            142, 41, 224, 201, 169, 225, 121, 200, 110, 106, 120, 34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];

        // Verifing JWT
        let verifier = HS256.verifier_from_bytes(key).unwrap();
        let (payload, header) = jwt::decode_with_verifier("eyJhbGciOiJIUzI1NiJ9.eyJzYWx0Ijoid1M5aUhkMmUiLCJwaW4iOiIxMDAwMDAxIiwiYXBwSWQiOiJqTFVUeWlMcCJ9.9AzF2fKc1EY90vrF_qeqgKqhDTX3TmWBgXSdnipCX8s", &verifier).unwrap();
        println!("payload: {:?}", payload);
    }
}
