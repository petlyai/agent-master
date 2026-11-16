#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_master::{serve, Processes};
use agent_master_client::{AgentMaster, Error, ProcessSpec, Refusal, State, Window, WindowSize};
use base64::Engine;
use tokio::net::TcpListener;

pub const PATIENCE: Duration = Duration::from_secs(5);

pub async fn listening() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("bound address");
    tokio::spawn(serve(listener, Arc::new(Processes::new())));
    address
}

pub async fn master() -> AgentMaster {
    AgentMaster::connect(listening().await).await.expect("connect")
}

pub fn spec(argv: &[&str]) -> ProcessSpec {
    ProcessSpec {
        command: argv[0].to_string(),
        args: argv[1..].iter().map(|arg| (*arg).to_string()).collect(),
        cwd: std::env::temp_dir(),
        env: BTreeMap::new(),
        terminal: Some(WindowSize { rows: 24, cols: 80 }),
        output: None,
        message_socket: None,
    }
}

pub const BELL: &str = "\\007";
pub const STRING_TERMINATOR: &str = "\\033\\\\";

pub fn copying(text: &str) -> String {
    copying_to("c", text, BELL)
}

pub fn copying_to(selection: &str, text: &str, terminator: &str) -> String {
    let payload = base64::engine::general_purpose::STANDARD.encode(text);
    format!("printf '\\033]52;{selection};{payload}{terminator}'")
}

pub fn shell(script: &str) -> ProcessSpec {
    spec(&["sh", "-c", script])
}

pub fn text(window: &Window) -> String {
    let mut parser = vt100::Parser::new(window.size.rows, window.size.cols, 0);
    parser.process(&window.contents);
    parser.screen().contents()
}

pub fn refusal(error: Error) -> Refusal {
    match error {
        Error::Refused(refusal) => refusal,
        other => panic!("expected a refusal, got {other}"),
    }
}

pub async fn window_showing(master: &mut AgentMaster, needle: &str) -> String {
    let deadline = Instant::now() + PATIENCE;
    let mut seen = String::new();
    while Instant::now() < deadline {
        seen = text(&master.window().await.expect("window"));
        if seen.contains(needle) {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the window never showed {needle:?}; last window:\n{seen}");
}

pub async fn ended(master: &mut AgentMaster) -> State {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let state = master.state().await.expect("state");
        if !matches!(state, State::Running) {
            return state;
        }
        assert!(Instant::now() < deadline, "the process never ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn exit_code(state: &State) -> Option<i32> {
    match state {
        State::Exited { code, .. } => *code,
        other => panic!("expected an ended process, got {other:?}"),
    }
}

pub fn last_window(state: &State) -> &Window {
    match state {
        State::Exited { window: Some(window), .. } => window,
        other => panic!("expected an ended process, got {other:?}"),
    }
}

pub fn without_a_terminal(script: &str) -> ProcessSpec {
    ProcessSpec { terminal: None, ..shell(script) }
}

pub async fn ended_as(master: &mut AgentMaster, name: &str) -> State {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let state = master.process(name).state().await.expect("state");
        if !matches!(state, State::Running) {
            return state;
        }
        assert!(Instant::now() < deadline, "{name} never ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn written(path: &std::path::Path) -> String {
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let Ok(read) = std::fs::read_to_string(path) {
            if !read.is_empty() {
                return read;
            }
        }
        assert!(Instant::now() < deadline, "{} never came", path.display());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether the process is gone, or left at most a zombie for whoever inherited it.
pub async fn gone(pid: i32) -> bool {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        let status = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        match status {
            Err(_) => return true,
            Ok(stat) if stat.rsplit(") ").next().is_some_and(|rest| rest.starts_with('Z')) => {
                return true
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    false
}
