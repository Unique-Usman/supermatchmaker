// JWT authentication using the `jsonwebtoken` crate (HS256).

use jsonwebtoken::{
    decode, encode, errors::ErrorKind, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: i64, // subject: user id
    pub iat: u64, // issued-at (unix seconds)
    pub exp: u64, // expiry (unix seconds)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug, PartialEq)]
pub enum AuthError {
    Malformed,
    BadSignature,
    Expired,
}

// Issue an HS256 token for `user_id`, valid for `ttl_secs`.
pub fn issue(user_id: i64, secret: &[u8], ttl_secs: u64) -> String {
    let iat = now_secs();
    let claims = Claims {
        sub: user_id,
        iat,
        exp: iat + ttl_secs,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .expect("token encoding never fails for valid claims")
}

// Verify a token's signature and expiry; return the claims if valid.
pub fn verify(token: &str, secret: &[u8]) -> Result<Claims, AuthError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.required_spec_claims.clear();

    match decode::<Claims>(token, &DecodingKey::from_secret(secret), &validation) {
        Ok(data) => Ok(data.claims),
        Err(e) => Err(match e.kind() {
            ErrorKind::ExpiredSignature => AuthError::Expired,
            ErrorKind::InvalidSignature => AuthError::BadSignature,
            _ => AuthError::Malformed,
        }),
    }
}

// Extract a bearer token from an Authorization header value.
pub fn bearer(header_value: &str) -> Option<&str> {
    header_value.strip_prefix("Bearer ").map(|s| s.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let secret = b"test-secret";
        let tok = issue(42, secret, 60);
        assert_eq!(verify(&tok, secret).unwrap().sub, 42);
    }

    #[test]
    fn rejects_tampering() {
        let secret = b"test-secret";
        let tok = issue(42, secret, 60);
        let mut bad = tok;
        bad.pop();
        bad.push('x');
        assert!(verify(&bad, secret).is_err());
    }

    #[test]
    fn rejects_wrong_secret() {
        let tok = issue(42, b"secret-one", 60);
        assert!(matches!(
            verify(&tok, b"secret-two"),
            Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn rejects_expired() {
        let secret = b"test-secret";
        let tok = issue(42, secret, 0);
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert!(matches!(verify(&tok, secret), Err(AuthError::Expired)));
    }
}
