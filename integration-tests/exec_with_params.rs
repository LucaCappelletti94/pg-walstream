#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Live check for `PgReplicationConnection::exec_with_params`: parameter
//! round-trips and that a payload is bound, not interpreted as SQL.

use pg_walstream::PgReplicationConnection;
use proptest::prelude::*;
use std::sync::{Mutex, OnceLock};

fn conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/postgres?sslmode=disable".to_string()
        })
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn exec_with_params_round_trips_scalars() {
    let mut conn = PgReplicationConnection::connect(&conn_string()).expect("connect");

    let r = conn.exec_with_params("SELECT $1::int4", &[&42i32]).unwrap();
    assert_eq!(r.get_value(0, 0), Some("42".to_string()));
    let r = conn
        .exec_with_params("SELECT $1::int4", &[&i32::MIN])
        .unwrap();
    assert_eq!(r.get_value(0, 0), Some(i32::MIN.to_string()));

    let r = conn
        .exec_with_params("SELECT $1::int8", &[&9_000_000_000i64])
        .unwrap();
    assert_eq!(r.get_value(0, 0), Some("9000000000".to_string()));
    let r = conn.exec_with_params("SELECT $1::bool", &[&true]).unwrap();
    assert_eq!(r.get_value(0, 0), Some("t".to_string()));

    let r = conn
        .exec_with_params("SELECT $1::text", &[&"héllo"])
        .unwrap();
    assert_eq!(r.get_value(0, 0), Some("héllo".to_string()));
    let r = conn.exec_with_params("SELECT $1::text", &[&""]).unwrap();
    assert_eq!(r.get_value(0, 0), Some(String::new()));

    // NULL via Option.
    let r = conn
        .exec_with_params("SELECT $1::int4", &[&None::<i32>])
        .unwrap();
    assert_eq!(r.get_bytes(0, 0), None);

    // No parameters.
    let r = conn.exec_with_params("SELECT 1", &[]).unwrap();
    assert_eq!(r.get_value(0, 0), Some("1".to_string()));

    // Wrong parameter count is an error, not a crash.
    assert!(conn.exec_with_params("SELECT $1::int4", &[]).is_err());
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn exec_with_params_binds_not_interprets() {
    let mut conn = PgReplicationConnection::connect(&conn_string()).expect("connect");
    conn.exec("DROP TABLE IF EXISTS ewp_inj").unwrap();
    conn.exec("CREATE TABLE ewp_inj (id int)").unwrap();

    let payload = "'; DROP TABLE ewp_inj; --";
    let r = conn
        .exec_with_params("SELECT $1::text", &[&payload])
        .unwrap();
    assert_eq!(r.get_value(0, 0), Some(payload.to_string()));

    // The payload was bound, not executed, so the table still exists.
    let r = conn
        .exec("SELECT count(*) FROM information_schema.tables WHERE table_name = 'ewp_inj'")
        .unwrap();
    assert_eq!(r.get_value(0, 0), Some("1".to_string()));

    conn.exec("DROP TABLE ewp_inj").unwrap();
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn exec_with_params_command_returns_no_rows() {
    let mut conn = PgReplicationConnection::connect(&conn_string()).expect("connect");
    conn.exec("DROP TABLE IF EXISTS ewp_cmd").unwrap();
    conn.exec("CREATE TABLE ewp_cmd (id int)").unwrap();

    // Parameterized command with no result rows (the CommandComplete path).
    let r = conn
        .exec_with_params("INSERT INTO ewp_cmd (id) VALUES ($1)", &[&7i32])
        .unwrap();
    assert_eq!(r.ntuples(), 0);

    let r = conn
        .exec_with_params("SELECT id FROM ewp_cmd WHERE id = $1", &[&7i32])
        .unwrap();
    assert_eq!(r.get_value(0, 0), Some("7".to_string()));

    conn.exec("DROP TABLE ewp_cmd").unwrap();
}

/// One connection shared across all proptest cases (reconnecting per case would
/// dominate the runtime).
fn shared_conn() -> &'static Mutex<PgReplicationConnection> {
    static CONN: OnceLock<Mutex<PgReplicationConnection>> = OnceLock::new();
    CONN.get_or_init(|| {
        Mutex::new(PgReplicationConnection::connect(&conn_string()).expect("connect"))
    })
}

/// Decode PostgreSQL `bytea` text output (`\x` followed by lowercase hex).
fn decode_bytea_hex(text: &str) -> Vec<u8> {
    let hex = text
        .strip_prefix("\\x")
        .expect("bytea output starts with \\x");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Arbitrary values bound and read back must equal the input, for the whole
    /// value space, proving the text encoding is a faithful inverse of the
    /// server's text input across each type.
    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_i32(v in any::<i32>()) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::int4", &[&v]).unwrap();
        prop_assert_eq!(r.get_value(0, 0), Some(v.to_string()));
    }

    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_i64(v in any::<i64>()) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::int8", &[&v]).unwrap();
        prop_assert_eq!(r.get_value(0, 0), Some(v.to_string()));
    }

    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_f64(v in any::<f64>()) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::float8", &[&v]).unwrap();
        let back: f64 = r.get_value(0, 0).expect("non-null").parse().expect("parse float8");
        if v.is_nan() {
            prop_assert!(back.is_nan());
        } else {
            prop_assert_eq!(back, v);
        }
    }

    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_bool(v in any::<bool>()) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::bool", &[&v]).unwrap();
        prop_assert_eq!(r.get_value(0, 0), Some((if v { "t" } else { "f" }).to_string()));
    }

    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_text(s in any::<String>().prop_filter("no interior NUL", |s| !s.contains('\0'))) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::text", &[&s]).unwrap();
        prop_assert_eq!(r.get_value(0, 0), Some(s.clone()));
    }

    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_bytea(bytes in any::<Vec<u8>>()) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::bytea", &[&bytes]).unwrap();
        let text = r.get_value(0, 0).expect("non-null");
        prop_assert_eq!(decode_bytea_hex(&text), bytes);
    }

    #[test]
    #[ignore = "requires live PostgreSQL"]
    fn prop_roundtrip_option(v in any::<Option<i32>>()) {
        let mut conn = shared_conn().lock().unwrap();
        let r = conn.exec_with_params("SELECT $1::int4", &[&v]).unwrap();
        match v {
            Some(n) => prop_assert_eq!(r.get_value(0, 0), Some(n.to_string())),
            None => prop_assert!(r.get_bytes(0, 0).is_none()),
        }
    }
}
