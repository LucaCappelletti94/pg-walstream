#![cfg(all(feature = "libpq", not(feature = "rustls-tls")))]

//! The libpq backend's non-blocking connect against a live server.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! cargo test --no-default-features --features libpq --test libpq_async_connect -- --ignored --test-threads=1
//! ```

use pg_walstream::PgReplicationConnection;

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    })
}

/// The connection `connect_async` returns serves commands, on a current-thread runtime.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn connect_async_returns_a_usable_connection() {
    let mut conn = PgReplicationConnection::connect_async(&replication_conn_string())
        .await
        .expect("connection");
    let identity = conn.exec("IDENTIFY_SYSTEM").expect("IDENTIFY_SYSTEM");
    assert!(identity.get_value(0, 0).is_some());
    let encoding = conn.exec("SHOW client_encoding").expect("SHOW");
    assert_eq!(encoding.get_value(0, 0).as_deref(), Some("UTF8"));
}

/// A database name that looks like a connection string is a name, not a nested conninfo.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn connect_async_keeps_a_conninfo_shaped_database_name_literal() {
    let url = replication_conn_string();
    let (base, _) = url.split_once('?').unwrap_or((url.as_str(), ""));
    let (server, _) = base.rsplit_once('/').expect("URI connection string");
    let literal = format!("{server}/host%3Dnowhere?replication=database");
    let err = match PgReplicationConnection::connect_async(&literal).await {
        Ok(_) => panic!("the database \"host=nowhere\" does not exist"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("host=nowhere"),
        "the server must have been asked for the literal database: {err}"
    );
}
