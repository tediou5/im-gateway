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
    }
}
