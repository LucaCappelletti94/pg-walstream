//! Crate-local `ToSql` for `exec_with_params`.
//!
//! Parameters are sent in PostgreSQL text format with no declared type, so the
//! server infers each parameter's type from the query. This keeps parameterized
//! queries dependency-free (no `postgres-types`) and identical across backends.

use std::fmt::Write as _;

/// A value bindable as a text-format query parameter for
/// [`exec_with_params`](crate::PgReplicationConnection::exec_with_params).
pub trait ToSql {
    /// The PostgreSQL text representation to bind, or `None` for SQL `NULL`
    /// (use `Option<T>`).
    fn to_sql_text(&self) -> Option<String>;
}

macro_rules! impl_display_to_sql {
    ($($t:ty),*) => {$(
        impl ToSql for $t {
            fn to_sql_text(&self) -> Option<String> {
                Some(self.to_string())
            }
        }
    )*};
}

impl_display_to_sql!(i16, i32, i64, u32);

macro_rules! impl_float_to_sql {
    ($($t:ty),*) => {$(
        impl ToSql for $t {
            fn to_sql_text(&self) -> Option<String> {
                Some(if self.is_nan() {
                    "NaN".to_string()
                } else if self.is_infinite() {
                    if *self > 0.0 { "Infinity".to_string() } else { "-Infinity".to_string() }
                } else {
                    self.to_string()
                })
            }
        }
    )*};
}

impl_float_to_sql!(f32, f64);

impl ToSql for bool {
    fn to_sql_text(&self) -> Option<String> {
        Some(if *self { "true" } else { "false" }.to_string())
    }
}

impl ToSql for &str {
    fn to_sql_text(&self) -> Option<String> {
        Some((*self).to_string())
    }
}

impl ToSql for String {
    fn to_sql_text(&self) -> Option<String> {
        Some(self.clone())
    }
}

impl ToSql for &[u8] {
    fn to_sql_text(&self) -> Option<String> {
        Some(bytea_text(self))
    }
}

impl ToSql for Vec<u8> {
    fn to_sql_text(&self) -> Option<String> {
        Some(bytea_text(self))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql_text(&self) -> Option<String> {
        match self {
            Some(value) => value.to_sql_text(),
            None => None,
        }
    }
}

/// PostgreSQL `bytea` text input format: `\x` followed by lowercase hex.
fn bytea_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::ToSql;

    #[test]
    fn scalars_render_as_text() {
        assert_eq!(42i32.to_sql_text().as_deref(), Some("42"));
        assert_eq!(i32::MIN.to_sql_text().as_deref(), Some("-2147483648"));
        assert_eq!(
            9_000_000_000i64.to_sql_text().as_deref(),
            Some("9000000000")
        );
        assert_eq!(true.to_sql_text().as_deref(), Some("true"));
        assert_eq!(false.to_sql_text().as_deref(), Some("false"));
        assert_eq!("héllo".to_sql_text().as_deref(), Some("héllo"));
        assert_eq!(String::new().to_sql_text().as_deref(), Some(""));
    }

    #[test]
    fn floats_handle_specials() {
        assert_eq!(1.5f64.to_sql_text().as_deref(), Some("1.5"));
        assert_eq!(f64::NAN.to_sql_text().as_deref(), Some("NaN"));
        assert_eq!(f64::INFINITY.to_sql_text().as_deref(), Some("Infinity"));
        assert_eq!(
            f64::NEG_INFINITY.to_sql_text().as_deref(),
            Some("-Infinity")
        );
    }

    #[test]
    fn bytea_uses_hex_input_format() {
        let bytes: &[u8] = &[0x00, 0x01, 0xde, 0xad, 0xbe, 0xef];
        assert_eq!(bytes.to_sql_text().as_deref(), Some("\\x0001deadbeef"));
        assert_eq!((&[] as &[u8]).to_sql_text().as_deref(), Some("\\x"));
    }

    #[test]
    fn option_binds_null() {
        assert_eq!(None::<i32>.to_sql_text(), None);
        assert_eq!(Some(7i32).to_sql_text().as_deref(), Some("7"));
    }
}
