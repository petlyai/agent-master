use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use agent_master_client::{AgentMaster, ProcessSpec, State, WindowSize};

fn lately_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("bound address").port()
}

async fn answering(address: &str) -> Option<AgentMaster> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(mut master) = AgentMaster::connect(address).await {
            if master.state().await.is_ok() {
                return Some(master);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

enum Told {
    Flag,
    Environment,
    Both,
}

const UNBINDABLE: &str = "127.0.0.1:1";

async fn started(told: Told) -> (Daemon, AgentMaster) {
    for _ in 0..5 {
        let address = format!("127.0.0.1:{}", lately_free_port());
        let mut command = Command::new(env!("CARGO_BIN_EXE_svora-am"));
        match told {
            Told::Flag => {
                command.args(["--listen", &address]);
            }
            Told::Environment => {
                command.env("AGENT_MASTER_LISTEN", &address);
            }
            Told::Both => {
                command.env("AGENT_MASTER_LISTEN", UNBINDABLE).args(["--listen", &address]);
            }
        }
        let daemon = Daemon(command.spawn().expect("run agent-master"));
        if let Some(master) = answering(&address).await {
            return (daemon, master);
        }
    }
    panic!("agent-master never came up");
}

struct Daemon(std::process::Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn the_program_serves_the_address_it_was_given() {
    let (_daemon, mut master) = started(Told::Flag).await;
    assert_eq!(master.state().await.expect("state"), State::NotStarted);
}

#[tokio::test]
async fn the_address_may_come_from_the_environment_instead() {
    let (_daemon, mut master) = started(Told::Environment).await;
    assert_eq!(master.state().await.expect("state"), State::NotStarted);
}

#[tokio::test]
async fn the_given_address_wins_over_the_one_in_the_environment() {
    let (_daemon, mut master) = started(Told::Both).await;
    assert_eq!(master.state().await.expect("state"), State::NotStarted);
}

#[tokio::test]
async fn the_program_says_which_address_it_ended_up_on() {
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_svora-am"))
        .args(["--listen", "127.0.0.1:0"])
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("run agent-master");

    let said = daemon.stderr.take().expect("stderr");
    let mut lines = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(said));
    let announcement = loop {
        let line = lines.next_line().await.expect("read").expect("a line");
        if line.contains("listening on") {
            break line;
        }
    };
    let address = announcement
        .rsplit_once("listening on ")
        .map(|(_, address)| address.trim().to_string())
        .expect("an address in the announcement");
    assert!(!address.ends_with(":0"), "the announced address is the asked-for one: {announcement}");

    let mut master = answering(&address).await.expect("the announced address answers");
    assert_eq!(master.state().await.expect("state"), State::NotStarted);
}

#[test]
fn an_address_it_cannot_have_is_reported_and_ends_the_program() {
    let taken = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = taken.local_addr().expect("bound address").to_string();

    let run = Command::new(env!("CARGO_BIN_EXE_svora-am"))
        .args(["--listen", &address])
        .stderr(Stdio::piped())
        .output()
        .expect("run agent-master");

    assert!(!run.status.success(), "taking a busy address should have failed");
    let complaint = String::from_utf8_lossy(&run.stderr);
    assert!(complaint.contains(&address), "{complaint}");
}

#[test]
fn an_address_it_was_never_given_is_reported() {
    let run = Command::new(env!("CARGO_BIN_EXE_svora-am"))
        .env_remove("AGENT_MASTER_LISTEN")
        .stderr(Stdio::piped())
        .output()
        .expect("run agent-master");

    assert!(!run.status.success(), "running with no address should have failed");
    let complaint = String::from_utf8_lossy(&run.stderr);
    assert!(complaint.contains("--listen"), "{complaint}");
}

fn sh(script: String, terminal: Option<WindowSize>) -> ProcessSpec {
    ProcessSpec {
        command: "sh".to_string(),
        args: vec!["-c".to_string(), script],
        cwd: std::env::temp_dir(),
        env: Default::default(),
        terminal,
        output: None,
        message_socket: None,
    }
}

async fn pid_in(path: &std::path::Path) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let read = std::fs::read_to_string(path).ok();
        if let Some(pid) = read.and_then(|read| read.trim().parse().ok()) {
            return pid;
        }
        assert!(Instant::now() < deadline, "{} was never written", path.display());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn gone(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Err(_) => true,
        Ok(stat) => stat.rsplit(") ").next().is_some_and(|rest| rest.starts_with('Z')),
    }
}

async fn every_process_goes_down_with_agent_master_on(signal: i32) {
    let (mut daemon, mut master) = started(Told::Flag).await;
    let dir = tempfile::tempdir().expect("a directory");
    let detached = dir.path().join("detached");
    let attached = dir.path().join("attached");
    let without = sh(format!("echo $$ > {}; exec sleep 30", detached.display()), None);
    master.process("job").start(without).await.expect("start without a terminal");
    let size = Some(WindowSize { rows: 24, cols: 80 });
    let under = sh(format!("echo $$ > {}; exec sleep 30", attached.display()), size);
    master.start(under).await.expect("start under a terminal");
    let pids = [pid_in(&detached).await, pid_in(&attached).await];

    unsafe {
        libc::kill(daemon.0.id() as i32, signal);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let ended = loop {
        if let Some(status) = daemon.0.try_wait().expect("wait") {
            break status;
        }
        assert!(Instant::now() < deadline, "agent-master never ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(ended.success(), "{ended:?}");
    for pid in pids {
        assert!(gone(pid), "{pid} outlived agent-master");
    }
}

#[tokio::test]
async fn asked_to_end_it_takes_every_process_down_with_it() {
    every_process_goes_down_with_agent_master_on(libc::SIGTERM).await;
}

#[tokio::test]
async fn interrupted_it_takes_every_process_down_with_it() {
    every_process_goes_down_with_agent_master_on(libc::SIGINT).await;
}

#[tokio::test]
async fn a_start_that_comes_while_it_ends_is_refused() {
    let (mut daemon, mut master) = started(Told::Flag).await;
    let dir = tempfile::tempdir().expect("a directory");
    let ready = dir.path().join("ready");
    let stubborn =
        format!("trap '' TERM; echo up > {}; while :; do sleep 1; done", ready.display());
    master.process("stubborn").start(sh(stubborn, None)).await.expect("start");
    pid_in_or_text(&ready).await;

    unsafe {
        libc::kill(daemon.0.id() as i32, libc::SIGTERM);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let late = dir.path().join("late");
    let refused = master
        .process("late")
        .start(sh(format!("echo $$ > {}; exec sleep 30", late.display()), None))
        .await
        .expect_err("a start while ending");
    assert!(refused.to_string().contains("ending"), "{refused}");
    let _ = daemon.0.kill();
    let _ = daemon.0.wait();
    assert!(!late.exists(), "the late process ran after all");
}

#[tokio::test]
async fn the_grace_it_gives_its_processes_when_ending_is_its_to_be_told() {
    for told in [&["--stop-grace-ms", "200"][..], &[]] {
        let address = format!("127.0.0.1:{}", lately_free_port());
        let mut command = Command::new(env!("CARGO_BIN_EXE_svora-am"));
        command.args(["--listen", &address]).args(told);
        if told.is_empty() {
            command.env("AGENT_MASTER_STOP_GRACE_MS", "200");
        }
        let mut daemon = Daemon(command.spawn().expect("run agent-master"));
        let mut master = answering(&address).await.expect("agent-master came up");
        let dir = tempfile::tempdir().expect("a directory");
        let ready = dir.path().join("ready");
        let stubborn =
            format!("trap '' TERM; echo up > {}; while :; do sleep 1; done", ready.display());
        master.process("stubborn").start(sh(stubborn, None)).await.expect("start");
        pid_in_or_text(&ready).await;

        let asked = Instant::now();
        unsafe {
            libc::kill(daemon.0.id() as i32, libc::SIGTERM);
        }
        while daemon.0.try_wait().expect("wait").is_none() {
            assert!(
                asked.elapsed() < Duration::from_secs(3),
                "the default grace was given: {told:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

#[tokio::test]
async fn a_failure_to_take_connections_ends_it_and_its_processes_with_it() {
    use std::os::unix::process::CommandExt;

    let address = format!("127.0.0.1:{}", lately_free_port());
    let mut command = Command::new(env!("CARGO_BIN_EXE_svora-am"));
    command.args(["--listen", &address]);
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit { rlim_cur: 24, rlim_max: 24 };
            match libc::setrlimit(libc::RLIMIT_NOFILE, &limit) {
                0 => Ok(()),
                _ => Err(std::io::Error::last_os_error()),
            }
        });
    }
    let mut daemon = Daemon(command.spawn().expect("run agent-master"));
    let mut master = answering(&address).await.expect("agent-master came up");
    let dir = tempfile::tempdir().expect("a directory");
    let pid_file = dir.path().join("pid");
    let job = sh(format!("echo $$ > {}; exec sleep 30", pid_file.display()), None);
    master.process("job").start(job).await.expect("start");
    let pid = pid_in(&pid_file).await;

    let mut held = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let ended = loop {
        if let Some(status) = daemon.0.try_wait().expect("wait") {
            break status;
        }
        assert!(Instant::now() < deadline, "agent-master took connections past its limit");
        if let Ok(connection) = std::net::TcpStream::connect(&address) {
            held.push(connection);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(!ended.success(), "{ended:?}");
    assert!(gone(pid), "{pid} outlived agent-master");
}

async fn pid_in_or_text(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !std::fs::read_to_string(path).is_ok_and(|read| !read.is_empty()) {
        assert!(Instant::now() < deadline, "{} was never written", path.display());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
