#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Startup metadata read on the stream's own connection.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test startup_metadata -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, Lsn, PgReplicationConnection,
    ReplicationStreamConfig, RetryConfig,
};
use std::time::Duration;

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string())
}

fn regular() -> PgReplicationConnection {
    PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection")
}

fn scalar(conn: &mut PgReplicationConnection, sql: &str) -> Option<String> {
    conn.exec(sql).expect(sql).get_value(0, 0)
}

fn drop_slot(conn: &mut PgReplicationConnection, slot: &str) {
    let _ = conn.exec(&format!(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = '{slot}'"
    ));
}

fn config(slot: &str, publication: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::builder(slot, publication).with_retry_config(RetryConfig {
        max_attempts: 1,
        ..RetryConfig::default()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn server_identity_describes_the_streaming_server() {
    let mut regular = regular();
    // `system_identifier` is a signed bigint holding the unsigned control-file value.
    let expected: u64 = scalar(
        &mut regular,
        "SELECT (system_identifier::numeric + CASE WHEN system_identifier < 0 \
         THEN 18446744073709551616 ELSE 0 END)::text FROM pg_control_system()",
    )
    .unwrap()
    .parse()
    .unwrap();
    let timeline: u32 = scalar(
        &mut regular,
        "SELECT timeline_id FROM pg_control_checkpoint()",
    )
    .unwrap()
    .parse()
    .unwrap();

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        config("it_startup_identity", "it_startup_identity"),
    )
    .await
    .expect("stream");

    assert_eq!(stream.server_identity().system_id(), expected);
    assert_eq!(stream.server_identity().timeline(), timeline);
    let history = stream.timeline_history().await.expect("history");
    assert_eq!(history.is_empty(), timeline == 1, "{history:?}");
    assert!(
        history.iter().all(|switch| switch.timeline() < timeline),
        "{history:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn slot_position_is_read_before_start_and_refused_after() {
    let (slot, publication, table) = ("it_startup_slot", "it_startup_slot", "it_startup_slot");
    let mut regular = regular();
    drop_slot(&mut regular, slot);
    regular
        .exec(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY)"
        ))
        .expect("table");
    regular
        .exec(&format!("DROP PUBLICATION IF EXISTS {publication}"))
        .expect("drop publication");
    regular
        .exec(&format!(
            "CREATE PUBLICATION {publication} FOR TABLE {table}"
        ))
        .expect("publication");

    let mut missing =
        LogicalReplicationStream::new(&replication_conn_string(), config(slot, publication))
            .await
            .expect("stream");
    assert_eq!(missing.slot_confirmed_flush_lsn().await.unwrap(), None);
    drop(missing);

    regular
        .exec(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ))
        .expect("slot");
    let expected = scalar(
        &mut regular,
        &format!(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ),
    )
    .unwrap();

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), config(slot, publication))
            .await
            .expect("stream");
    let position = stream
        .slot_confirmed_flush_lsn()
        .await
        .unwrap()
        .expect("the slot exists");
    assert_eq!(
        position,
        Lsn::new(pg_walstream::parse_lsn(&expected).unwrap())
    );

    stream.start(None).await.expect("start");
    assert!(
        stream.slot_confirmed_flush_lsn().await.is_err(),
        "a streaming connection cannot run the slot query"
    );

    regular
        .exec(&format!("INSERT INTO {table} VALUES (1)"))
        .expect("insert");
    let cancel = CancellationToken::new();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next_event(&cancel))
        .await
        .expect("an event arrives")
        .expect("event");
    assert!(
        matches!(first.event_type, EventType::Begin { .. }),
        "{first:?}"
    );

    drop(stream);
    drop_slot(&mut regular, slot);
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = regular.exec(&format!("DROP TABLE IF EXISTS {table}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn slot_position_of_a_physical_slot_is_an_error() {
    let slot = "it_startup_physical";
    let mut regular = regular();
    drop_slot(&mut regular, slot);
    regular
        .exec(&format!(
            "SELECT pg_create_physical_replication_slot('{slot}')"
        ))
        .expect("slot");

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), config(slot, "unused"))
            .await
            .expect("stream");
    let err = stream.slot_confirmed_flush_lsn().await.unwrap_err();
    assert!(err.to_string().contains("not a logical slot"), "{err}");

    drop(stream);
    drop_slot(&mut regular, slot);
}

/// The blocking commands kept next to their `_async` siblings still reach the server: identify it, create a temporary slot and start streaming from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn blocking_commands_identify_create_and_start() {
    let slot = "it_startup_blocking";
    let mut regular = regular();
    drop_slot(&mut regular, slot);
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {slot}"));
    regular
        .exec(&format!("CREATE PUBLICATION {slot}"))
        .expect("publication");

    let stream = LogicalReplicationStream::new(&replication_conn_string(), config(slot, slot))
        .await
        .expect("stream");
    let expected = *stream.server_identity();

    let mut conn =
        PgReplicationConnection::connect(&replication_conn_string()).expect("blocking connect");
    let identified = conn.identify_system().expect("IDENTIFY_SYSTEM");
    assert_eq!(
        identified
            .get_value(0, 0)
            .and_then(|v| v.parse::<u64>().ok()),
        Some(expected.system_id())
    );
    assert_eq!(
        identified
            .get_value(0, 1)
            .and_then(|v| v.parse::<u32>().ok()),
        Some(expected.timeline())
    );

    let created = conn
        .create_replication_slot_with_options(
            slot,
            pg_walstream::SlotType::Logical,
            Some("pgoutput"),
            &pg_walstream::ReplicationSlotOptions {
                temporary: true,
                ..Default::default()
            },
        )
        .expect("CREATE_REPLICATION_SLOT");
    assert_eq!(created.get_value(0, 0).as_deref(), Some(slot));

    conn.start_replication(
        slot,
        0,
        &[
            ("proto_version", "1"),
            ("publication_names", &format!("\"{slot}\"")),
        ],
    )
    .expect("START_REPLICATION");
    assert!(
        conn.exec("SELECT 1").is_err(),
        "a streaming connection must refuse queries"
    );

    drop(conn);
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {slot}"));
}
