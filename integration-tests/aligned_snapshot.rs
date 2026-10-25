#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for the aligned snapshot: a copy of extra tables taken
//! while a stream is already running, merged at the side slot's consistent
//! point.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 15+ with `wal_level = logical`
//! - `DATABASE_URL` — a replication connection string
//! - `DATABASE_URL_REGULAR` — a regular connection to the same database
//!
//! ```bash
//! cargo test --test aligned_snapshot -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    AlignedSnapshot, CancellationToken, ColumnValue, EventType, LogicalReplicationStream, Lsn,
    PgReplicationConnection, PgResult, ReplicationError, ReplicationStreamConfig, RowData,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        replication_conn_string()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn regular_conn() -> PgReplicationConnection {
    PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection")
}

fn exec(conn: &mut PgReplicationConnection, sql: &str) -> PgResult {
    conn.exec(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn count(result: &PgResult) -> i64 {
    result.get_value(0, 0).unwrap().parse().unwrap()
}

/// Drop `slot`, terminating a walsender that still holds it.
fn drop_slot(slot: &str) {
    let mut conn = regular_conn();
    for _ in 0..50 {
        let _ = conn.exec(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{slot}' AND active_pid IS NOT NULL"
        ));
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
        ));
        if count(&exec(
            &mut conn,
            &format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'"),
        )) == 0
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not drop replication slot {slot}");
}

/// Drops its slot however the test leaves; declare it before the stream.
struct SlotGuard(&'static str);

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = std::panic::catch_unwind(|| drop_slot(self.0));
        } else {
            drop_slot(self.0);
        }
    }
}

fn slot_guard(slot: &'static str) -> SlotGuard {
    drop_slot(slot);
    SlotGuard(slot)
}

async fn running_stream(slot: &str, publication: &str) -> LogicalReplicationStream {
    let config = ReplicationStreamConfig::builder(slot, publication);
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
        .await
        .expect("stream");
    stream.ensure_replication_slot().await.expect("slot");
    stream.start(None).await.expect("start");
    stream
}

fn text(data: &RowData, column: &str) -> String {
    data.get(column)
        .and_then(ColumnValue::as_str)
        .unwrap_or_else(|| panic!("column {column} missing or not text"))
        .to_string()
}

fn id(data: &RowData) -> i64 {
    text(data, "id").parse().unwrap()
}

/// The table's rows as `id -> v`.
fn table_state(conn: &mut PgReplicationConnection, table: &str) -> BTreeMap<i64, String> {
    let result = exec(conn, &format!("SELECT id, v FROM {table} ORDER BY id"));
    (0..result.ntuples())
        .map(|r| {
            (
                result.get_value(r, 0).unwrap().parse().unwrap(),
                result.get_value(r, 1).unwrap(),
            )
        })
        .collect()
}

fn no_aligned_slot_left() -> bool {
    let mut conn = regular_conn();
    (0..50).any(|_| {
        let left = count(&exec(
            &mut conn,
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pg_walstream_aligned_%'",
        ));
        if left > 0 {
            std::thread::sleep(Duration::from_millis(100));
        }
        left == 0
    })
}

/// A running stream at `B`, a writer committing inserts, updates and deletes to
/// `{prefix}_new` and `{prefix}_old` across a capture of `{prefix}_new`, and a
/// sentinel row in `{prefix}_old` after the writer stopped.
struct Race {
    _guard: SlotGuard,
    admin: PgReplicationConnection,
    stream: LogicalReplicationStream,
    cancel: CancellationToken,
    prefix: &'static str,
    c: Lsn,
    copy: BTreeMap<i64, String>,
    /// The rows at `B`, before any change the writer made.
    at_b: BTreeMap<i64, String>,
}

async fn race(prefix: &'static str, slot: &'static str) -> Race {
    let guard = slot_guard(slot);
    let mut admin = regular_conn();
    for sql in [
        format!("DROP PUBLICATION IF EXISTS {prefix}_pub"),
        format!("DROP TABLE IF EXISTS {prefix}_old, {prefix}_new"),
        format!("CREATE TABLE {prefix}_old (id bigint PRIMARY KEY, v text NOT NULL)"),
        format!("CREATE TABLE {prefix}_new (id bigint PRIMARY KEY, v text NOT NULL)"),
        format!("CREATE PUBLICATION {prefix}_pub FOR TABLE {prefix}_old, {prefix}_new"),
    ] {
        exec(&mut admin, &sql);
    }

    let mut stream = running_stream(slot, &format!("{prefix}_pub")).await;
    let cancel = CancellationToken::new();

    for id in 1..=20 {
        exec(
            &mut admin,
            &format!("INSERT INTO {prefix}_new VALUES ({id}, 'b')"),
        );
    }
    let mut delivered = 0;
    while delivered < 20 {
        if let EventType::Insert { .. } = stream.next_event(&cancel).await.unwrap().event_type {
            delivered += 1;
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut w = regular_conn();
            let mut id = 1000_i64;
            while !stop.load(Ordering::Relaxed) {
                exec(
                    &mut w,
                    &format!("INSERT INTO {prefix}_new VALUES ({id}, 'w')"),
                );
                exec(
                    &mut w,
                    &format!("INSERT INTO {prefix}_old VALUES ({id}, 'w')"),
                );
                if id % 3 == 0 {
                    exec(
                        &mut w,
                        &format!("UPDATE {prefix}_new SET v = v || 'u' WHERE id = {}", id - 1),
                    );
                }
                if id % 5 == 0 {
                    exec(
                        &mut w,
                        &format!("DELETE FROM {prefix}_new WHERE id = {}", id - 3),
                    );
                }
                id += 1;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut snapshot = stream
        .capture_aligned(&[("public", &format!("{prefix}_new"))])
        .await
        .expect("capture");
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    exec(
        &mut admin,
        &format!("INSERT INTO {prefix}_old VALUES (-1, 'sentinel')"),
    );

    let c = snapshot.consistent_point();
    assert_eq!(snapshot.tables().len(), 1);
    let mut copy = BTreeMap::new();
    while let Some(row) = snapshot.next_row().await.unwrap() {
        assert_eq!(row.lsn, c);
        assert!(copy.insert(id(&row.data), text(&row.data, "v")).is_none());
    }
    snapshot.finish().unwrap();
    assert!(
        no_aligned_slot_left(),
        "the temporary side slot outlived the capture"
    );

    Race {
        _guard: guard,
        admin,
        stream,
        cancel,
        prefix,
        c,
        copy,
        at_b: (1..=20).map(|id| (id, "b".to_string())).collect(),
    }
}

/// What one stream event means for the race.
enum Step {
    /// A change to `{prefix}_new`: row, new value or `None` for a delete, and whether the row must already exist.
    Change(i64, Option<String>, bool),
    Sentinel,
    Other,
}

fn step(event: EventType, prefix: &str) -> Step {
    let (old, new) = (format!("{prefix}_old"), format!("{prefix}_new"));
    match event {
        EventType::Insert { table, data, .. } if *table == *old && id(&data) == -1 => {
            Step::Sentinel
        }
        EventType::Insert { table, data, .. } if *table == *new => {
            Step::Change(id(&data), Some(text(&data, "v")), false)
        }
        EventType::Update {
            table, new_data, ..
        } if *table == *new => Step::Change(id(&new_data), Some(text(&new_data, "v")), true),
        EventType::Delete {
            table, old_data, ..
        } if *table == *new => Step::Change(id(&old_data), None, true),
        _ => Step::Other,
    }
}

fn apply(state: &mut BTreeMap<i64, String>, row: i64, value: &Option<String>, must_exist: bool) {
    let previous = match value {
        Some(v) => state.insert(row, v.clone()),
        None => state.remove(&row),
    };
    assert_eq!(
        previous.is_some(),
        must_exist,
        "row {row} applied out of step"
    );
}

/// With changes committing on both sides of `C`, the stream replayed to `C`
/// reproduces the copy, and the copy plus every transaction whose
/// `commit_lsn()` is at or past `C` reproduces the table, each change once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn copy_and_stream_split_at_the_consistent_point_apply_every_change_once() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let Race {
            _guard,
            mut admin,
            mut stream,
            cancel,
            prefix,
            c,
            mut copy,
            mut at_b,
        } = race("aligned_split", "aligned_split_main").await;

        let mut commit_lsn = Lsn::new(0);
        let (mut below, mut at_or_after) = (0, 0);
        let mut crossed = false;
        loop {
            let event = stream.next_event(&cancel).await.unwrap().event_type;
            if let Some(commit) = event.commit_lsn() {
                commit_lsn = commit;
                if commit >= c && !crossed {
                    crossed = true;
                    assert_eq!(at_b, copy, "the copy differs from the stream replayed to C");
                }
                continue;
            }
            match step(event, prefix) {
                Step::Sentinel => break,
                Step::Other => {}
                Step::Change(row, value, must_exist) => {
                    apply(&mut at_b, row, &value, must_exist);
                    if commit_lsn < c {
                        below += 1;
                    } else {
                        at_or_after += 1;
                        apply(&mut copy, row, &value, must_exist);
                    }
                }
            }
        }

        assert!(
            below > 0,
            "no change committed between B and C, the overlap was not exercised"
        );
        assert!(crossed && at_or_after > 0, "no change committed after C");
        assert_eq!(copy, table_state(&mut admin, &format!("{prefix}_new")));
    })
    .await
    .expect("test timed out");
}

/// `next_event_before(C)` delivers exactly the transactions committed below
/// `C`, and the following reads deliver the rest, so applying the copy at the
/// `None` reproduces the table with each change once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn next_event_before_stops_the_stream_where_the_copy_applies() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let Race {
            _guard,
            mut admin,
            mut stream,
            cancel,
            prefix,
            c,
            mut copy,
            mut at_b,
        } = race("aligned_barrier", "aligned_barrier_main").await;

        let mut below = 0;
        while let Some(event) = stream.next_event_before(c, &cancel).await.unwrap() {
            if let Some(commit) = event.event_type.commit_lsn() {
                assert!(commit < c, "a commit at or past C crossed the barrier");
            }
            if let Step::Change(row, value, must_exist) = step(event.event_type, prefix) {
                apply(&mut at_b, row, &value, must_exist);
                below += 1;
            }
        }
        assert!(
            below > 0,
            "no change committed between B and C, the overlap was not exercised"
        );
        assert_eq!(
            at_b, copy,
            "the stream stopped somewhere other than the copy's point"
        );

        let mut first_commit = None;
        let mut after = 0;
        loop {
            let event = stream.next_event(&cancel).await.unwrap().event_type;
            if let Some(commit) = event.commit_lsn() {
                first_commit.get_or_insert(commit);
                continue;
            }
            match step(event, prefix) {
                Step::Sentinel => break,
                Step::Other => {}
                Step::Change(row, value, must_exist) => {
                    apply(&mut copy, row, &value, must_exist);
                    after += 1;
                }
            }
        }
        assert!(first_commit.is_some_and(|commit| commit >= c));
        assert!(after > 0, "no change committed after C");
        assert_eq!(copy, table_state(&mut admin, &format!("{prefix}_new")));
    })
    .await
    .expect("test timed out");
}

/// With nothing published after `C`, the walsender's keepalive ends the wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn a_keepalive_ends_the_wait_when_no_published_commit_follows() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let _guard = slot_guard("aligned_idle_main");
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_idle_pub",
            "DROP TABLE IF EXISTS aligned_idle, aligned_idle_other",
            "CREATE TABLE aligned_idle (id bigint PRIMARY KEY, v text NOT NULL)",
            "CREATE TABLE aligned_idle_other (id bigint PRIMARY KEY)",
            "INSERT INTO aligned_idle VALUES (1, 'a')",
            "CREATE PUBLICATION aligned_idle_pub FOR TABLE aligned_idle",
        ] {
            exec(&mut admin, sql);
        }

        let stream = running_stream("aligned_idle_main", "aligned_idle_pub").await;
        let mut copy = stream
            .capture_aligned(&[("public", "aligned_idle")])
            .await
            .expect("capture");
        let c = copy.consistent_point();
        exec(&mut admin, "INSERT INTO aligned_idle_other VALUES (1)");

        let mut events = stream.into_stream(CancellationToken::new());
        let reached = tokio::time::timeout(Duration::from_secs(30), events.next_event_before(c))
            .await
            .expect("the boundary was never reported");
        assert!(reached.unwrap().is_none());

        let mut rows = 0;
        while copy.next_row().await.unwrap().is_some() {
            rows += 1;
        }
        copy.finish().unwrap();
        assert_eq!(rows, 1);
    })
    .await
    .expect("test timed out");
}

/// The copy reads each table through its publication's column list and row
/// filter, so its rows have the shape the stream delivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn the_copy_applies_the_publication_column_list_and_row_filter() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_shape_pub",
            "DROP TABLE IF EXISTS aligned_shape",
            "CREATE TABLE aligned_shape (id bigint PRIMARY KEY, v text NOT NULL, secret text)",
            "INSERT INTO aligned_shape SELECT g, 'v' || g, 's' FROM generate_series(1, 10) g",
            "CREATE PUBLICATION aligned_shape_pub FOR TABLE aligned_shape (id, v) WHERE (id > 5)",
        ] {
            exec(&mut admin, sql);
        }

        // A plain connection string: capture derives the replication session itself.
        let mut snapshot = AlignedSnapshot::capture(
            &regular_conn_string(),
            &["aligned_shape_pub"],
            &[("public", "aligned_shape")],
        )
        .await
        .expect("capture");
        let mut ids = Vec::new();
        while let Some(row) = snapshot.next_row().await.unwrap() {
            let columns: Vec<&str> = row.data.iter().map(|(name, _)| &**name).collect();
            assert_eq!(columns, ["id", "v"]);
            ids.push(id(&row.data));
        }
        snapshot.finish().unwrap();
        ids.sort_unstable();
        assert_eq!(ids, [6, 7, 8, 9, 10]);
    })
    .await
    .expect("test timed out");
}

/// A table the publication does not carry is rejected by name, and the side
/// slot does not survive the failed capture.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn an_unpublished_table_is_rejected_by_name() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_missing_pub",
            "DROP TABLE IF EXISTS aligned_missing_in, aligned_missing_out",
            "CREATE TABLE aligned_missing_in (id bigint PRIMARY KEY, v text)",
            "CREATE TABLE aligned_missing_out (id bigint PRIMARY KEY, v text)",
            "CREATE PUBLICATION aligned_missing_pub FOR TABLE aligned_missing_in",
        ] {
            exec(&mut admin, sql);
        }

        let err = match AlignedSnapshot::capture(
            &replication_conn_string(),
            &["aligned_missing_pub"],
            &[
                ("public", "aligned_missing_in"),
                ("public", "aligned_missing_out"),
            ],
        )
        .await
        {
            Ok(_) => panic!("an unpublished table must be rejected"),
            Err(e) => e,
        };
        assert!(matches!(err, ReplicationError::Config(_)), "{err:?}");
        assert!(
            err.to_string().contains("public.aligned_missing_out"),
            "{err}"
        );
        assert!(
            no_aligned_slot_left(),
            "the temporary side slot outlived the failed capture"
        );
    })
    .await
    .expect("test timed out");
}

/// Slot creation waits for every open transaction, and that wait must not
/// block a current-thread runtime that also drives the running stream.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn capture_waits_for_open_transactions_without_stalling_the_runtime() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut admin = regular_conn();
        for sql in [
            "DROP PUBLICATION IF EXISTS aligned_wait_pub",
            "DROP TABLE IF EXISTS aligned_wait",
            "CREATE TABLE aligned_wait (id bigint PRIMARY KEY, v text NOT NULL)",
            "INSERT INTO aligned_wait VALUES (1, 'a')",
            "CREATE PUBLICATION aligned_wait_pub FOR TABLE aligned_wait",
        ] {
            exec(&mut admin, sql);
        }

        let mut blocker = regular_conn();
        exec(&mut blocker, "BEGIN");
        exec(&mut blocker, "SELECT txid_current()");

        let conninfo = replication_conn_string();
        let capture = tokio::spawn(async move {
            AlignedSnapshot::capture(
                &conninfo,
                &["aligned_wait_pub"],
                &[("public", "aligned_wait")],
            )
            .await
        });
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !capture.is_finished(),
            "slot creation did not wait for the open transaction"
        );

        exec(&mut blocker, "COMMIT");
        let mut copy = capture.await.unwrap().expect("capture");
        let mut rows = 0;
        while copy.next_row().await.unwrap().is_some() {
            rows += 1;
        }
        copy.finish().unwrap();
        assert_eq!(rows, 1);
    })
    .await
    .expect("test timed out");
}
