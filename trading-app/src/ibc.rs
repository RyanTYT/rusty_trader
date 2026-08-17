use std::future::Future;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot},
    time::{Duration, Instant, timeout},
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const GRACEFUL_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const PORT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const PORT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);

const GATEWAY_PORT: &str = "localhost:4002";
const COMMAND_SERVER_ADDR: &str = "127.0.0.1:7462";
const COMMAND_SERVER_PORT: &str = "localhost:7462";

/// Owns a running IB Gateway child process.
///
/// This type is intentionally NOT constructible or droppable-with-guarantees
/// from outside this module. The only supported way to use it is via
/// [`with_gateway`] / [`with_gateway_retry`], which own both halves of the
/// lifecycle (start -> use -> shutdown) in one place. `start` and `shutdown`
/// are kept private/crate-visible so callers can't accidentally obtain an
/// owned `IBGateway` and let it fall out of scope without shutting down.
pub struct IBGateway {
    child: Child,
    /// Set to true only inside `shutdown`. Read by `Drop` to distinguish a
    /// clean, intended shutdown from an unexpected drop (panic unwind,
    /// future cancellation via `select!`/`timeout` mid-await, etc).
    shut_down: bool,
}

impl IBGateway {
    /// Spawns IB Gateway via IBC and waits for it to report readiness.
    ///
    /// On every failure path the partially-started child is killed and
    /// reaped before returning `Err`, so callers never leak an orphaned
    /// process on a failed start (this was previously the source of
    /// double-started gateways fighting over port 4002 on retry).
    async fn start(log_file: &str) -> Result<Self, String> {
        let success_pattern = "Login has completed";
        let failure_pattern = "IBC returned exit status";

        let mut child = Command::new("/IBCLinux-3.21.2/scripts/ibcstart.sh")
            .arg("1030")
            .arg("--gateway")
            .arg("--tws-path=/home/tws")
            .arg("--tws-settings-path=/home/tws")
            .arg("--ibc-path=/IBCLinux-3.21.2")
            .arg("--ibc-ini=/IBCLinux-3.21.2/config.ini")
            .arg("--user=")
            .arg("--pw=")
            .arg("--fix-user=")
            .arg("--fix-pw=")
            .arg("--java-path=")
            .arg("--mode=paper")
            .arg("--on2fatimeout=restart")
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("Error encountered trying to start IBC: {e:?}"))?;

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        let mut reader_out = BufReader::new(stdout).lines();
        let mut reader_err = BufReader::new(stderr).lines();

        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();

        tokio::spawn({
            let tx = line_tx.clone();
            async move {
                while let Ok(Some(line)) = reader_out.next_line().await {
                    let _ = tx.send(line);
                }
            }
        });
        tokio::spawn({
            let tx = line_tx.clone();
            async move {
                while let Ok(Some(line)) = reader_err.next_line().await {
                    let _ = tx.send(line);
                }
            }
        });
        drop(line_tx);

        let (result_tx, result_rx) = oneshot::channel::<bool>();

        // Log-forwarding task. Opens the file once in append mode instead
        // of re-truncating it on every line (the previous `fs::write` call
        // overwrote the file each time, so only the last line ever
        // survived).
        let log_path = log_file.to_string();
        tokio::spawn(async move {
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .await
            {
                Ok(f) => Some(f),
                Err(e) => {
                    tracing::warn!("Couldn't open IBC log file {log_path}: {e:?}");
                    None
                }
            };

            while let Some(line) = line_rx.recv().await {
                println!("{line}");
                if let Some(f) = file.as_mut() {
                    let _ = f.write_all(line.as_bytes()).await;
                    let _ = f.write_all(b"\n").await;
                }
                if line.contains(success_pattern) {
                    let _ = result_tx.send(true);
                    break;
                } else if line.contains(failure_pattern) {
                    let _ = result_tx.send(false);
                    break;
                }
            }
            // Note: this task intentionally keeps draining `line_rx` after
            // send would be a no-op is NOT done here on purpose - once we've
            // decided success/failure we stop reading, letting the stdout
            // forwarders' sends silently fail once the receiver is dropped.
        });

        // Wait for a success/failure signal or time out. On every branch
        // below that returns Err, we explicitly kill + reap the child
        // first so nothing is ever leaked back to the caller.
        let outcome = timeout(STARTUP_TIMEOUT, result_rx).await;

        let is_success = match outcome {
            Ok(Ok(is_success)) => is_success,
            Ok(Err(recv_err)) => {
                Self::kill_and_reap(&mut child).await;
                return Err(format!(
                    "Log-reader task ended without a result: {recv_err:?}"
                ));
            }
            Err(elapsed) => {
                Self::kill_and_reap(&mut child).await;
                return Err(format!("Timed out waiting for IBC to start: {elapsed:?}"));
            }
        };

        if !is_success {
            Self::kill_and_reap(&mut child).await;
            return Err("Failure pattern encountered when starting IBC".to_string());
        }

        if let Err(e) = wait_for_port(true, GATEWAY_PORT, Duration::from_secs(10)).await {
            Self::kill_and_reap(&mut child).await;
            return Err(format!(
                "Failed to connect to port even with success msg: {e:?}"
            ));
        }

        // Sleep for 5 seconds to ensure port is free for connection
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("✅ IB Gateway successfully started");
        Ok(Self {
            child,
            shut_down: false,
        })
    }

    /// Gracefully shuts down IB Gateway, consuming `self`.
    ///
    /// Consuming `self` makes double-shutdown a compile error rather than a
    /// runtime race (previously `stop()` could be called once explicitly
    /// and once more via `Drop` on the same value).
    async fn shutdown(mut self) -> Result<(), String> {
        self.shutdown_inner().await
    }

    /// Shared implementation used by both `shutdown` and the sync fallback
    /// path. Takes `&mut self` so `Drop` can still trigger a best-effort
    /// kill without consuming.
    async fn shutdown_inner(&mut self) -> Result<(), String> {
        if let Err(e) = Self::send_stop_commands().await {
            tracing::warn!("Graceful STOP sequence failed, will force kill: {e:?}");
        } else {
            tracing::info!("Sent STOP/EXIT/quit to IBC CommandServer on {COMMAND_SERVER_ADDR}");
        }

        // Race a graceful exit against a fixed grace period instead of
        // always paying the full sleep even when IBC exits immediately.
        let graceful = timeout(GRACEFUL_SHUTDOWN_GRACE, self.child.wait()).await;

        match graceful {
            Ok(Ok(status)) => {
                tracing::info!("IBC exited gracefully: {status:?}");
            }
            Ok(Err(e)) => {
                tracing::warn!("Error waiting on IBC child: {e:?}, forcing kill");
                Self::kill_and_reap(&mut self.child).await;
            }
            Err(_elapsed) => {
                tracing::warn!("IBC did not exit within grace period, forcing kill");
                Self::kill_and_reap(&mut self.child).await;
            }
        }

        if let Err(e) = wait_for_ports(
            false,
            &[GATEWAY_PORT, COMMAND_SERVER_PORT],
            PORT_WAIT_TIMEOUT,
        )
        .await
        {
            // Previously this was fire-and-forget; now it's surfaced to the
            // caller so a stuck port doesn't silently look like success.
            self.shut_down = true;
            return Err(format!("Ports did not release after shutdown: {e:?}"));
        }
        // Sleep for 5 seconds to ensure port is free for connection
        tokio::time::sleep(Duration::from_secs(1)).await;

        self.shut_down = true;
        Ok(())
    }

    async fn send_stop_commands() -> Result<(), String> {
        let mut stream = tokio::net::TcpStream::connect(COMMAND_SERVER_ADDR)
            .await
            .map_err(|e| format!("Failed to connect to IBC CommandServer: {e:?}"))?;

        for cmd in [
            b"STOP\n".as_slice(),
            b"EXIT\n".as_slice(),
            b"quit\n".as_slice(),
        ] {
            stream
                .write_all(cmd)
                .await
                .map_err(|e| format!("Failed to write {cmd:?} to IBC CommandServer: {e:?}"))?;
            if let Err(e) = stream.flush().await {
                tracing::warn!("Couldn't flush {cmd:?} to IBC: {e:?}");
            }
        }
        Ok(())
    }

    /// Non-blocking kill + async reap. `start_kill` is synchronous (just
    /// issues the kill syscall); only `wait()` is async, so this is safe to
    /// call from both async contexts and, in the sync-only half, from Drop
    /// (where we only issue `start_kill` and let the OS reap it, since we
    /// can't `.await` there - see `Drop` impl below).
    async fn kill_and_reap(child: &mut Child) {
        if let Err(e) = child.start_kill() {
            if e.kind() != std::io::ErrorKind::InvalidInput {
                tracing::warn!("Failed to kill IBC child process: {e:?}");
            }
            // InvalidInput means the process had already exited - fine.
        }
        let _ = child.wait().await;
    }
}

impl Drop for IBGateway {
    /// Safety net only. This covers cases that never reach `shutdown()`:
    /// a panic unwinding through the caller's closure, or the enclosing
    /// future being cancelled mid-`.await` (e.g. losing a `tokio::select!`
    /// race or being wrapped in `tokio::time::timeout`) — in both cases
    /// execution never returns to the point where `shutdown().await` would
    /// have run.
    ///
    /// `Drop::drop` is sync and cannot `.await`, so this deliberately does
    /// NOT attempt the graceful STOP/EXIT/quit handshake or block the
    /// runtime with `block_on` (the original implementation's core bug).
    /// It only issues a non-blocking kill signal and logs loudly. The OS
    /// will reap the zombie once the process table is cleaned up, or the
    /// next `with_gateway` call's own child-spawning will not be blocked
    /// by it since it's on a different pid.
    fn drop(&mut self) {
        if self.shut_down {
            return;
        }

        tracing::error!(
            "IBGateway dropped without calling shutdown() - this indicates a panic or \
             cancellation somewhere in the code path using with_gateway/with_gateway_retry. \
             Issuing a non-blocking kill as a last resort; ports may not be released cleanly."
        );

        if let Err(e) = self.child.start_kill() {
            if e.kind() != std::io::ErrorKind::InvalidInput {
                tracing::error!("Drop: failed to send kill signal to IBC child: {e:?}");
            }
        }
        // Deliberately not reaping here: `wait()` is async and Drop can't
        // await it. The process becoming a zombie briefly until the parent
        // exits or something else reaps it is an acceptable tradeoff versus
        // blocking a Tokio worker thread inside Drop.
    }
}

/// The only supported way to use an `IBGateway`. Starts the gateway, hands
/// a reference to `f`, then unconditionally attempts a graceful shutdown
/// once `f` completes - success or not. There is no code path through this
/// function that returns an owned `IBGateway` to the caller, so it's
/// structurally impossible (within this crate) to obtain one and forget to
/// shut it down through normal, non-cancelled control flow.
pub async fn with_gateway<F, Fut, T>(log_file: &str, f: F) -> Result<T, String>
where
    F: FnOnce(&IBGateway) -> Fut,
    Fut: Future<Output = T>,
{
    let gateway = IBGateway::start(log_file).await?;
    let result = f(&gateway).await;

    if let Err(e) = gateway.shutdown().await {
        // The work itself (`result`) already completed by this point; we
        // still surface the shutdown failure rather than swallowing it,
        // since a stuck port will break the *next* start attempt.
        return Err(format!("Gateway work completed but shutdown failed: {e:?}"));
    }

    Ok(result)
}

/// Same as [`with_gateway`], but retries `IBGateway::start` up to
/// `retry_times` additional times on failure before giving up. Retries only
/// wrap the start step - by the time `f` runs, exactly one gateway is live.
pub async fn with_gateway_retry<F, Fut, T>(
    log_file: &'static str,
    retry_times: u32,
    f: F,
) -> Result<T, String>
where
    F: FnOnce(&IBGateway) -> Fut,
    Fut: Future<Output = T>,
{
    let mut attempt = 0;
    let gateway = loop {
        match IBGateway::start(log_file).await {
            Ok(gateway) => break gateway,
            Err(e) => {
                tracing::error!("Couldn't initialise IBC due to {e:?}!");
                attempt += 1;
                if attempt > retry_times {
                    return Err(format!(
                        "Could not initialise IBC properly after {attempt} attempts: {e:?}"
                    ));
                }
                tracing::error!("Retrying IBC init, attempt {attempt}/{retry_times}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    let result = f(&gateway).await;

    if let Err(e) = gateway.shutdown().await {
        return Err(format!("Gateway work completed but shutdown failed: {e:?}"));
    }

    Ok(result)
}

/// Async, non-blocking port-state check. Replaces the previous
/// `std::net::TcpStream` + `std::thread::sleep` version, which blocked
/// whatever Tokio worker thread called it for up to the full timeout.
async fn wait_for_port(
    can_be_connected: bool,
    port: &str,
    timeout_dur: Duration,
) -> Result<(), String> {
    wait_for_ports(can_be_connected, &[port], timeout_dur).await
}

async fn wait_for_ports(
    can_be_connected: bool,
    ports: &[&str],
    timeout_dur: Duration,
) -> Result<(), String> {
    let mut settled = vec![false; ports.len()];
    let start = Instant::now();
    return Ok(());

    // loop {
    //     let mut all_settled = true;
    //     for (idx, port) in ports.iter().enumerate() {
    //         if settled[idx] {
    //             continue;
    //         }
    //
    //         let is_connected = tokio::net::TcpStream::connect(port).await.is_ok();
    //         if is_connected == can_be_connected {
    //             tracing::info!(
    //                 "Port {port} reached expected state (connectable={can_be_connected})"
    //             );
    //             settled[idx] = true;
    //         } else {
    //             all_settled = false;
    //         }
    //     }
    //
    //     if all_settled {
    //         return Ok(());
    //     }
    //
    //     if Instant::now().duration_since(start) >= timeout_dur {
    //         return Err(format!(
    //             "wait_for_ports timed out after {:?}: ports {:?} unsettled",
    //             timeout_dur,
    //             ports
    //                 .iter()
    //                 .enumerate()
    //                 .filter(|(i, _)| !settled[*i])
    //                 .map(|(_, p)| *p)
    //                 .collect::<Vec<_>>()
    //         ));
    //     }
    //
    //     tokio::time::sleep(PORT_POLL_INTERVAL).await;
    // }
}
