#![cfg(all(feature = "libpq", not(feature = "rustls-tls")))]

//! The libpq backend's non-blocking connect against a live server.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! cargo test --no-default-features --features libpq --test libpq_async_connect -- --ignored --test-threads=1
//! ```

use pg_walstream::{LogicalReplicationStream, PgReplicationConnection, ReplicationStreamConfig};
use std::time::{Duration, Instant};

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

/// `DATABASE_URL` with a host that accepts and never answers listed first, and a 2-second `connect_timeout`.
async fn behind_a_silent_host() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let silent = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            held.push(sock);
        }
    });
    let url = replication_conn_string();
    let (scheme, rest) = url.split_once("://").expect("URI connection string");
    let path_at = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(path_at);
    let (userinfo, server) = authority.rsplit_once('@').expect("user in the URI");
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let query: Vec<&str> = query
        .split('&')
        .filter(|p| !p.is_empty() && !p.starts_with("connect_timeout="))
        .chain(["connect_timeout=2"])
        .collect();
    let url = format!(
        "{scheme}://{userinfo}@127.0.0.1:{port},{server}{path}?{}",
        query.join("&")
    );
    (url, silent)
}

/// A first host that accepts and stays silent costs its own `connect_timeout`, then the live server is reached.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn connect_async_fails_over_from_a_silent_host_to_the_live_server() {
    let (url, silent) = behind_a_silent_host().await;
    let started = Instant::now();
    let mut conn = tokio::time::timeout(
        Duration::from_secs(20),
        PgReplicationConnection::connect_async(&url),
    )
    .await
    .expect("the silent host's connect_timeout must end its attempt")
    .expect("the live server is the second host");
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "{:?}",
        started.elapsed()
    );
    let identity = conn.exec("IDENTIFY_SYSTEM").expect("IDENTIFY_SYSTEM");
    assert!(identity.get_value(0, 0).is_some());
    silent.abort();
}

/// The stream's own connect fails over too, though the silent host uses up more than `connection_timeout`.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn stream_startup_fails_over_from_a_silent_host_to_the_live_server() {
    let (url, silent) = behind_a_silent_host().await;
    let config = ReplicationStreamConfig::builder("unused_slot", "unused_publication")
        .with_connection_timeout(Duration::from_secs(1));
    let stream = tokio::time::timeout(
        Duration::from_secs(20),
        LogicalReplicationStream::new(&url, config),
    )
    .await
    .expect("the silent host's connect_timeout must end its attempt")
    .expect("the live server is the second host");
    assert_ne!(stream.server_identity().system_id(), 0);
    silent.abort();
}
