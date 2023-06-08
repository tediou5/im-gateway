const SECRET_KEY: [u8; 32] = [
    142, 41, 224, 201, 169, 225, 121, 200, 110, 106, 120, 34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
];

const APPID: &'static str = "appId";
const PIN: &'static str = "pin";

pub(crate) fn parse_user_token(token: &str) -> anyhow::Result<String> {
    let verifier = josekit::jws::HS256.verifier_from_bytes(SECRET_KEY).unwrap();
    let (payload, _) = josekit::jwt::decode_with_verifier(token, &verifier).unwrap();
    let appid = payload
        .claim(APPID)
        .ok_or(anyhow::anyhow!("parse token error: appid not exist"))?
        .as_str()
        .unwrap();
    let pin = payload
        .claim(PIN)
        .ok_or(anyhow::anyhow!("parse token error: pin not exist"))?
        .as_str()
        .unwrap();

    Ok(format!("{appid}:{pin}"))
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_token() {
        let uid = super::parse_user_token("eyJhbGciOiJIUzI1NiJ9.eyJzYWx0Ijoid1M5aUhkMmUiLCJwaW4iOiIxMDAwMDAxIiwiYXBwSWQiOiJqTFVUeWlMcCJ9.9AzF2fKc1EY90vrF_qeqgKqhDTX3TmWBgXSdnipCX8s").unwrap();
        let res = "jLUTyiLp:1000001".to_string();
        assert_eq!(uid, res);
    }
}
