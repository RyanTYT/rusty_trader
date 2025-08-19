use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot},
    time::{Duration, timeout},
};

pub struct IBGateway {
    child: Child,
}

impl IBGateway {
    pub async fn start(log_file: String) -> anyhow::Result<(Self, bool)> {
        let success_pattern = "IBC: Click button: OK";
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
            .spawn()?;

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
        let (tx, rx) = oneshot::channel::<Result<bool, anyhow::Error>>();

        // Spawn log reader
        let log_file = log_file.clone();
        tokio::spawn({
            async move {
                while let Some(line) = reader.recv().await {
                    tokio::fs::write(log_file.clone(), &line).await.ok(); // Append to file
                    if line.contains(success_pattern) {
                        let _ = tx.send(Ok(true));
                        break;
                    } else if line.contains(failure_pattern) {
                        let _ = tx.send(Ok(false));
                        break;
                    }
                }
            }
        });

        // Wait up to 60s for result
        let result = timeout(Duration::from_secs(120), rx).await???;

        Ok((Self { child }, result))
    }

    pub async fn stop(mut self) -> anyhow::Result<()> {
        self.child.kill().await?;
        Ok(())
    }
}
