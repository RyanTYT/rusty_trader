use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot},
    time::{Duration, timeout},
};

pub struct IBGateway {
    child: Child,
}

impl IBGateway {
    pub async fn start(log_file: &str) -> Result<Self, String> {
        let success_pattern = "Login has completed";
        let failure_pattern = "IBC returned exit status";

        // Spawn IB Gateway
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

        let (tx, mut reader) = mpsc::unbounded_channel();

        tokio::spawn({
            let tx = tx.clone();
            async move {
                while let Ok(Some(line)) = reader_out.next_line().await {
                    let _ = tx.send(line);
                }
            }
        });

        tokio::spawn({
            let tx = tx.clone();
            async move {
                while let Ok(Some(line)) = reader_err.next_line().await {
                    let _ = tx.send(line);
                }
            }
        });

        // Channel to notify when success/failure detected
        let (tx, rx) = oneshot::channel::<bool>();

        // Spawn log reader
        let log_file = log_file.to_string();
        tokio::spawn({
            async move {
                while let Some(line) = reader.recv().await {
                    println!("{}", line);
                    tokio::fs::write(log_file.clone(), &line).await.ok(); // Append to file
                    if line.contains(success_pattern) {
                        let _ = tx.send(true);
                        break;
                    } else if line.contains(failure_pattern) {
                        let _ = tx.send(false);
                        break;
                    }
                }
            }
        });

        // Wait up to 60s for result
        match timeout(Duration::from_secs(120), rx).await {
            Ok(res) => match res {
                Ok(is_success) => {
                    if is_success {
                        tracing::info!("✅ IB Gateway successfully started");
                        Ok(Self { child })
                    } else {
                        Err(format!("Failure pattern encountered when starting IBC"))
                    }
                }
                Err(e) => Err(format!(
                    "Error encountered trying to receive from onshot channel: {e:?}"
                )),
            },
            Err(elapsed_err) => {
                if let Err(_) = (Self { child }.stop().await) {
                    tracing::error!("timeout occurred for IBC n couldn't send kill signal");
                }
                Err(format!(
                    "timeout for IBC elapsed, returning false: {elapsed_err:?}"
                ))
            }
        }
    }

    // Since child.kill() is async anyway, whole function is async then - but not necessary and
    // noted that it might cause issues
    pub async fn stop(&mut self) -> Result<(), String> {
        let server_addr = "127.0.0.1:7462";

        // Attempt to connect and send STOP
        match tokio::net::TcpStream::connect(server_addr).await {
            Ok(mut stream) => {
                if let Err(e) = stream.write_all(b"STOP\n").await {
                    return Err(format!("Failed to write STOP command to buffer: {e:?}"));
                };
                // send to server
                if let Err(e) = stream.flush().await {
                    tracing::warn!("Couldn't send STOP command to IBC: {e:?}");
                };
                if let Err(e) = stream.write_all(b"EXIT\n").await {
                    return Err(format!("Failed to write EXIT command to buffer: {e:?}"));
                };
                if let Err(e) = stream.flush().await {
                    tracing::warn!("Couldn't send EXIT command to IBC: {e:?}");
                };
                if let Err(e) = stream.write_all(b"quit\n").await {
                    return Err(format!("Failed to write 'quit' command to buffer: {e:?}"));
                };
                if let Err(e) = stream.flush().await {
                    tracing::warn!("Couldn't send 'quit' command to IBC: {e:?}");
                };
                tracing::info!("Sent STOP command to IBC CommandServer on {server_addr:?}");
            }
            Err(e) => {
                return Err(format!("Failed to connect to IBC CommandServer: {e:?}"));
            }
        }

        // Wait 5 seconds for IBKR to gracefully close
        tokio::time::sleep(Duration::from_secs(5)).await;

        match self.child.kill().await {
            Ok(_) => {
                tracing::info!("Sent SIGKILL to IBC process");
            }
            Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {
                tracing::info!("IBC already exited gracefully");
            }
            Err(e) => {
                return Err(format!("Failed to kill process: {e:?}"));
            }
        }

        // Reap the process to prevent zombies
        let _ = self.child.wait().await;

        // ─── Robust port-release wait ────────────────────────────────────
        // After killing the child, the IB Gateway may still hold port 4002
        // for a few seconds while the OS releases the socket. If we return
        // immediately, the next test's IBGateway::start() will fail to bind.
        // Poll port 4002 until it's free (or timeout).
        let api_port = "127.0.0.1:4002";
        let poll_interval = Duration::from_millis(500);
        let max_wait = Duration::from_secs(30);
        let start = std::time::Instant::now();

        loop {
            let still_bound = tokio::net::TcpStream::connect(api_port).await.is_ok();
            if !still_bound {
                tracing::info!("Port 4002 released — IB Gateway fully torn down");
                break;
            }
            if start.elapsed() >= max_wait {
                tracing::warn!(
                    "Port 4002 still bound after {:?} — giving up (next boot may fail)",
                    max_wait
                );
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }

        Ok(())
    }
}

impl Drop for IBGateway {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Err(e) = self.stop().await {
                    tracing::warn!("Error stopping IBGateway: {e:?}. Retrying in 60 seconds.");

                    tokio::time::sleep(Duration::from_secs(60)).await;

                    if self.stop().await.is_err() {
                        tracing::warn!("Failed to stop IBGateway after retry. Continuing.");
                    }
                }
            });
        });
    }
}

pub(crate) async fn init_ibc_with_retry(
    ibc_logs_file: &'static str,
    retry_times: u32,
) -> Result<IBGateway, String> {
    let mut retry_time = 0;
    let ibc_opt = loop {
        let try_ib = match IBGateway::start(ibc_logs_file).await {
            Ok(gateway) => Some(gateway),
            Err(e) => {
                tracing::error!("Couldn't initialise IBC due to {e:?}!");
                retry_time += 1;
                if retry_time <= retry_times {
                    tracing::error!("Retrying init for IBC for {retry_time:?} time!");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                None
            }
        };
        break try_ib;
    };

    ibc_opt.ok_or("Error: Could not initialise IBC properly".to_string())
}
