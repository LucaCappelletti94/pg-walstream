#![cfg(all(feature = "libpq", not(feature = "rustls-tls")))]

//! The libpq backend's non-blocking commands against a live server.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! cargo test --no-default-features --features libpq --test libpq_async_commands -- --ignored --test-threads=1
//! ```

use pg_walstream::{PgReplicationConnection, SlotType};
use std::time::Duration;

fn connection() -> PgReplicationConnection {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    });
    PgReplicationConnection::connect(&url).expect("connection")
}

/// A reply larger than libpq's first socket read must complete without further server traffic.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn exec_async_returns_a_reply_spanning_several_reads() {
    let mut conn = connection();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        conn.exec_async("SELECT repeat('x', 1000) FROM generate_series(1, 200)"),
    )
    .await
    .expect("the whole reply is already on the wire, so nothing may wait for more")
    .expect("query");
    assert_eq!(result.ntuples(), 200);
    assert_eq!(result.get_value(199, 0).map(|v| v.len()), Some(1000));
}

/// The commands the stream issues at startup, in order, on one connection.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn startup_commands_run_on_one_connection() {
    let slot = "it_libpq_async_slot";
    let mut conn = connection();
    let identity = conn.identify_system_async().await.expect("IDENTIFY_SYSTEM");
    assert!(identity.get_value(0, 0).is_some());

    let _ = conn.drop_replication_slot_async(slot, false).await;
    conn.create_replication_slot_with_options_async(
        slot,
        SlotType::Logical,
        Some("pgoutput"),
        &Default::default(),
    )
    .await
    .expect("CREATE_REPLICATION_SLOT");
    conn.drop_replication_slot_async(slot, false)
        .await
        .expect("DROP_REPLICATION_SLOT");
}
