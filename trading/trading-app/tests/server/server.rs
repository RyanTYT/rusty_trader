//! `init_server` / `ServerHandle` lifecycle and the HTTP surface that is
//! reachable without IB or Postgres.
//!
//! Each test starts its own server on `127.0.0.1:0` (OS-assigned ephemeral
//! port) so tests can run in parallel without port collisions. Requests go
//! over a plain `std::net::TcpStream` with `Connection: close`; no HTTP client
//! dependency is added.
//!
//! Handler coverage in this file:
//!   - `/check-health` — the `None` (200 ok) and dead-`Weak` (500) branches.
//!     The "connected to IBKR" branch needs a live IB client
//!     (`src/server/server.rs:393`) and is NOT covered.
//!   - `/contract/price`, `/contracts/stock`, `/exchange_rate`,
//!     `/strategy/capital` — exactly the part before
//!     `extract_application_state` (`src/server/server.rs:77`): query
//!     rejection (400) and "not running" (500). Happy paths need a live
//!     `Consolidator`/IB `Client`; see the blockers listed there.
//!   - routing table: 404 for unknown paths, 405 for wrong methods.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Weak;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use trading_app::init_app::ApplicationState;
use trading_app::server::server::{ServerHandle, init_server};

const HEALTH: &str = "/check-health";
const NOT_RUNNING: &str = "Trading App is currently not running!";
const DEAD_STATE: &str =
    "Trading app is currently not running BUT app_state still has Strong Reference to it";

// ───────────────────────────── HTTP client ─────────────────────────────

struct RawResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl RawResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn content_type(&self) -> &str {
        self.header("content-type").unwrap_or("<missing>")
    }
}

fn request(addr: SocketAddr, method: &str, path: &str) -> RawResponse {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .unwrap_or_else(|e| panic!("connect to {addr} failed: {e}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    stream.flush().unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read HTTP response");
    let text = String::from_utf8(raw).expect("HTTP response is valid UTF-8");

    let (head, body) = text
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {text:?}"));
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_else(|| panic!("malformed status line: {status_line:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("non-numeric status: {status_line:?}"));

    let headers = lines
        .map(|line| {
            let (name, value) = line
                .split_once(':')
                .unwrap_or_else(|| panic!("malformed header line: {line:?}"));
            (name.trim().to_ascii_lowercase(), value.trim().to_string())
        })
        .collect();

    RawResponse {
        status,
        headers,
        body: body.to_string(),
    }
}

fn get(addr: SocketAddr, path: &str) -> RawResponse {
    request(addr, "GET", path)
}

// ───────────────────────────── test server ─────────────────────────────

/// A running server plus the channel used to publish `ApplicationState`.
struct TestServer {
    handle: ServerHandle,
    addr: SocketAddr,
    state_tx: mpsc::Sender<Option<Weak<ApplicationState>>>,
}

impl TestServer {
    fn start() -> Self {
        let (state_tx, state_rx) = mpsc::channel(8);
        let handle = init_server("127.0.0.1:0", state_rx)
            .expect("binding 127.0.0.1:0 must always succeed");
        let addr = handle.local_addr();
        Self {
            handle,
            addr,
            state_tx,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Publish a new application state to the server's background receiver.
    /// `try_send` only buffers, so this works before the worker runtime polls.
    fn publish_state(&self, state: Option<Weak<ApplicationState>>) {
        self.state_tx
            .try_send(state)
            .expect("server state channel must be open");
    }

    /// Poll until the route answers with `status` (the state receiver runs on
    /// the server's own thread, so a freshly published state is not visible
    /// instantly).
    fn wait_for(&self, path: &str, status: u16) -> RawResponse {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let response = get(self.addr, path);
            if response.status == status {
                return response;
            }
            if Instant::now() >= deadline {
                panic!(
                    "{path} never returned {status}; last response was {} {:?}",
                    response.status, response.body
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

// ───────────────────────────── lifecycle ─────────────────────────────

#[test]
fn local_addr_reports_the_os_assigned_ephemeral_port() {
    let server = TestServer::start();
    assert_eq!(server.addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_ne!(server.addr().port(), 0, "port 0 must be resolved");
    assert!(
        server.addr().port() >= 1024,
        "expected an ephemeral port, got {}",
        server.addr().port()
    );
}

#[test]
fn health_is_ok_before_any_application_state_is_published() {
    let server = TestServer::start();

    for _ in 0..3 {
        let response = get(server.addr(), HEALTH);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_type(), "application/json");
        assert_eq!(
            response.header("content-length"),
            Some("15"),
            "must be a fixed-length body"
        );
        // Exact body: no trailing newline, exact key/value.
        assert_eq!(response.body, r#"{"status":"ok"}"#);
    }
}

#[test]
fn health_is_500_when_the_published_weak_reference_is_dead() {
    let server = TestServer::start();
    server.publish_state(Some(Weak::<ApplicationState>::new()));

    let response = server.wait_for(HEALTH, 500);
    assert_eq!(response.content_type(), "text/plain; charset=utf-8");
    assert_eq!(response.body, DEAD_STATE);
}

#[test]
fn health_recovers_to_200_after_the_state_is_cleared() {
    let server = TestServer::start();

    server.publish_state(Some(Weak::<ApplicationState>::new()));
    assert_eq!(server.wait_for(HEALTH, 500).body, DEAD_STATE);

    // `None` on the channel means `.take()` — back to the unstarted state.
    server.publish_state(None);
    let response = server.wait_for(HEALTH, 200);
    assert_eq!(response.body, r#"{"status":"ok"}"#);
}

#[test]
fn bind_failure_is_reported_synchronously_instead_of_panicking() {
    // Occupy an ephemeral port, then ask init_server to bind the same address.
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap();
    let (_state_tx, state_rx) = mpsc::channel(1);

    match init_server(&addr.to_string(), state_rx) {
        Ok(handle) => {
            drop(handle);
            panic!("init_server unexpectedly bound already-occupied {addr}");
        }
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse, "{error}"),
    }
}

#[test]
fn dropping_the_handle_releases_the_bound_port() {
    let server = TestServer::start();
    let addr = server.addr();
    drop(server);

    // If the worker thread had not been joined, the listener would still hold
    // the port and this bind would fail with AddrInUse.
    TcpListener::bind(addr).expect("port must be released after ServerHandle::drop");
}

#[test]
fn explicit_shutdown_releases_the_bound_port() {
    let server = TestServer::start();
    let addr = server.addr();
    server.handle.shutdown();

    TcpListener::bind(addr).expect("port must be released after ServerHandle::shutdown");
}

#[test]
fn two_servers_run_concurrently_on_distinct_ephemeral_ports() {
    let first = TestServer::start();
    let second = TestServer::start();

    assert_ne!(first.addr(), second.addr());
    assert_eq!(get(first.addr(), HEALTH).status, 200);
    assert_eq!(get(second.addr(), HEALTH).status, 200);
}

// ───────────────────────────── routing ─────────────────────────────

#[test]
fn unknown_paths_return_404() {
    let server = TestServer::start();

    // Note the hyphen: `/check_health` is NOT a route.
    for path in ["/", "/nope", "/check_health", "/check-health/", "/Check-Health"] {
        let response = get(server.addr(), path);
        assert_eq!(response.status, 404, "path {path} should be 404");
        assert_eq!(response.body, "", "404 body should be empty for {path}");
    }
}

#[test]
fn wrong_method_on_health_returns_405() {
    let server = TestServer::start();

    let response = request(server.addr(), "POST", HEALTH);
    assert_eq!(response.status, 405);
    let allow = response.header("allow").unwrap_or("<missing>");
    assert!(
        allow.split(',').any(|m| m.trim() == "GET"),
        "Allow header {allow:?} should include GET"
    );
}

// ─────────────────── state-independent handler responses ───────────────────

#[test]
fn missing_query_params_are_rejected_before_state_is_needed() {
    let server = TestServer::start();

    let cases = [
        // ContractDetails field names, in declaration order.
        ("/contract/price", "missing field `stock`"),
        ("/contract/price?stock=AAPL", "missing field `primary_exchange`"),
        (
            "/contract/price?stock=AAPL&primary_exchange=NASDAQ",
            "missing field `currency`",
        ),
        ("/contracts/stock", "missing field `stock`"),
        (
            "/contracts/stock?stock=AAPL&primary_exchange=",
            "missing field `currency`",
        ),
        // Currencies field names.
        ("/exchange_rate?currency=SGD", "missing field `quote`"),
        ("/exchange_rate?quote=USD", "missing field `currency`"),
        // StrategyQuery field name.
        ("/strategy/capital", "missing field `strategy`"),
    ];

    for (path, fragment) in cases {
        let response = get(server.addr(), path);
        assert_eq!(response.status, 400, "path {path}");
        assert_eq!(
            response.content_type(),
            "text/plain; charset=utf-8",
            "path {path}"
        );
        assert!(
            response
                .body
                .starts_with("Failed to deserialize query string:"),
            "path {path} body {:?}",
            response.body
        );
        assert!(
            response.body.contains(fragment),
            "path {path} body {:?} does not contain {fragment:?}",
            response.body
        );
    }
}

#[test]
fn camel_case_query_param_is_not_an_alias_for_primary_exchange() {
    let server = TestServer::start();

    let response = get(
        server.addr(),
        "/contract/price?stock=AAPL&primaryExchange=NASDAQ&currency=USD",
    );
    assert_eq!(response.status, 400);
    assert!(
        response.body.contains("missing field `primary_exchange`"),
        "{:?}",
        response.body
    );
}

#[test]
fn handlers_return_the_not_running_error_when_no_state_is_published() {
    let server = TestServer::start();

    // Every one of these passes query extraction, then hits
    // `extract_application_state` (`src/server/server.rs:77-102`) which fails
    // fast because no `ApplicationState` was published. Crucially this proves
    // no IB/DB call happens before that check.
    let paths = [
        "/contract/price?stock=AAPL&primary_exchange=NASDAQ&currency=USD",
        "/contracts/stock?stock=AAPL&primary_exchange=&currency=",
        "/exchange_rate?currency=SGD&quote=USD",
        "/strategy/capital?strategy=manual",
    ];

    for path in paths {
        let response = get(server.addr(), path);
        assert_eq!(response.status, 500, "path {path}");
        assert_eq!(
            response.content_type(),
            "text/plain; charset=utf-8",
            "path {path}"
        );
        assert_eq!(response.body, NOT_RUNNING, "path {path}");
    }
}

#[test]
fn empty_strategy_is_accepted_by_extraction_and_fails_later() {
    let server = TestServer::start();

    // `String` accepts "", so this is a 500 (no app state), not a 400.
    let response = get(server.addr(), "/strategy/capital?strategy=");
    assert_eq!(response.status, 500);
    assert_eq!(response.body, NOT_RUNNING);
}

#[test]
fn unknown_query_params_do_not_prevent_extraction() {
    let server = TestServer::start();

    // Unknown params are ignored by serde; the handler still reaches the
    // not-running check.
    let response = get(
        server.addr(),
        "/exchange_rate?currency=SGD&quote=USD&cache=false",
    );
    assert_eq!(response.status, 500);
    assert_eq!(response.body, NOT_RUNNING);
}
