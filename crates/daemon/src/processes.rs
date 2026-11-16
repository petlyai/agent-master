use std::collections::BTreeMap;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, MutexGuard, PoisonError};
use std::time::Duration;

use agent_master_protocol::{Messages, ProcessSpec, Refusal, State, Window, WindowSize};
use anyhow::Context;
use base64::Engine;
use pty_process::{OwnedReadPty, OwnedWritePty};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex};
use tokio::task::{AbortHandle, JoinHandle};

use crate::messages::MessageSocket;

/// The emulator panics on a grid smaller than this.
pub const LEAST_WINDOW: WindowSize = WindowSize { rows: 2, cols: 2 };

const LAST_FRAME_WAIT: Duration = Duration::from_millis(200);

type Screen = SyncMutex<vt100::Parser<Clipboard>>;
pub(crate) type Outcome<T> = std::result::Result<T, Refusal>;

#[derive(Default)]
struct Clipboard(Option<Vec<u8>>);

impl vt100::Callbacks for Clipboard {
    fn copy_to_clipboard(&mut self, _: &mut vt100::Screen, _selection: &[u8], data: &[u8]) {
        match base64::engine::general_purpose::STANDARD.decode(data) {
            Ok(copied) => self.0 = Some(copied),
            Err(error) => log::warn!("the process copied something unreadable: {error}"),
        }
    }
}

#[derive(Debug, Clone)]
struct Exit {
    code: Option<i32>,
    signal: Option<i32>,
    window: Option<Window>,
}

struct Terminal {
    pty: Mutex<OwnedWritePty>,
    screen: Screen,
}

impl Terminal {
    fn open(size: WindowSize) -> Outcome<(Arc<Self>, pty_process::Pts, JoinHandle<()>)> {
        let (pty, pts) = pty_process::open().context("opening a terminal").map_err(failed)?;
        let (read, write) = pty.into_split();
        let mut terminal = Self {
            pty: Mutex::new(write),
            screen: SyncMutex::new(vt100::Parser::new_with_callbacks(
                LEAST_WINDOW.rows,
                LEAST_WINDOW.cols,
                0,
                Clipboard::default(),
            )),
        };
        Self::fit(terminal.pty.get_mut(), &terminal.screen, size)?;

        let terminal = Arc::new(terminal);
        let painting = tokio::spawn(terminal.clone().paint(read));
        Ok((terminal, pts, painting))
    }

    async fn type_in(&self, data: &[u8]) -> Outcome<()> {
        self.pty
            .lock()
            .await
            .write_all(data)
            .await
            .context("typing into the terminal")
            .map_err(failed)
    }

    async fn resize(&self, size: WindowSize) -> Outcome<()> {
        let pty = self.pty.lock().await;
        Self::fit(&pty, &self.screen, size)
    }

    fn window(&self) -> Window {
        let parser = painted(&self.screen);
        let (rows, cols) = parser.screen().size();
        Window { size: WindowSize { rows, cols }, contents: parser.screen().contents_formatted() }
    }

    fn clipboard(&self) -> Option<Vec<u8>> {
        painted(&self.screen).callbacks().0.clone()
    }

    async fn paint(self: Arc<Self>, mut pty: OwnedReadPty) {
        let mut buf = [0u8; 4096];
        loop {
            match pty.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(read) => {
                    // The emulator is the only reader of the terminal. If a byte
                    // sequence it cannot digest took the reader down with it, the
                    // process would fill the terminal and hang; losing the frame
                    // is the lesser harm.
                    let painting = std::panic::AssertUnwindSafe(|| {
                        painted(&self.screen).process(&buf[..read])
                    });
                    if std::panic::catch_unwind(painting).is_err() {
                        log::error!(
                            "the emulator could not take {read} bytes of what the process printed"
                        );
                    }
                }
            }
        }
    }

    fn fit(pty: &OwnedWritePty, screen: &Screen, size: WindowSize) -> Outcome<()> {
        if size.rows < LEAST_WINDOW.rows || size.cols < LEAST_WINDOW.cols {
            return Err(Refusal::UnusableWindowSize { least: LEAST_WINDOW });
        }
        painted(screen).screen_mut().set_size(size.rows, size.cols);
        pty.resize(pty_process::Size::new(size.rows, size.cols))
            .context("sizing the terminal")
            .map_err(failed)
    }
}

struct Process {
    terminal: Option<Arc<Terminal>>,
    leader: i32,
    pid_released: Arc<AtomicBool>,
    exit: watch::Receiver<Option<Exit>>,
    painting: Option<AbortHandle>,
    message_socket: Option<MessageSocket>,
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(painting) = &self.painting {
            painting.abort();
        }
    }
}

impl Process {
    fn running(&self) -> bool {
        self.exit.borrow().is_none()
    }

    fn terminal(&self) -> Outcome<&Terminal> {
        self.terminal.as_deref().ok_or(Refusal::NoTerminal)
    }

    fn window(&self) -> Outcome<Window> {
        let terminal = self.terminal()?;
        Ok(match &*self.exit.borrow() {
            Some(Exit { window: Some(last), .. }) => last.clone(),
            _ => terminal.window(),
        })
    }

    fn state(&self) -> State {
        match &*self.exit.borrow() {
            None => State::Running,
            Some(exit) => {
                State::Exited { code: exit.code, signal: exit.signal, window: exit.window.clone() }
            }
        }
    }

    async fn input(&self, data: &[u8]) -> Outcome<()> {
        let terminal = self.terminal()?;
        if !self.running() {
            return Err(Refusal::AlreadyExited);
        }
        terminal.type_in(data).await
    }

    async fn resize(&self, size: WindowSize) -> Outcome<()> {
        let terminal = self.terminal()?;
        if !self.running() {
            return Err(Refusal::AlreadyExited);
        }
        terminal.resize(size).await
    }

    fn clipboard(&self) -> Outcome<Option<Vec<u8>>> {
        Ok(self.terminal()?.clipboard())
    }

    fn message_socket(&self) -> Outcome<&MessageSocket> {
        self.message_socket.as_ref().ok_or(Refusal::NoMessageSocket)
    }

    async fn send(&self, message: String) -> Outcome<()> {
        self.message_socket()?.send(message, self.running()).await
    }

    async fn messages(&self, last_taken: u64) -> Outcome<Messages> {
        self.message_socket()?.messages(last_taken, self.running()).await
    }

    async fn stop(&self, grace: Duration) -> State {
        if self.running() {
            let mut exit = self.exit.clone();
            self.signal_group(libc::SIGTERM);
            if tokio::time::timeout(grace, ended(&mut exit)).await.is_err() {
                self.signal_group(libc::SIGKILL);
                ended(&mut exit).await;
            }
        }
        self.state()
    }

    fn signal_group(&self, signal: i32) {
        if self.pid_released.load(Ordering::SeqCst) {
            return;
        }
        unsafe {
            libc::kill(-self.leader, signal);
        }
    }
}

#[derive(Default)]
pub struct Processes {
    known: Mutex<Known>,
}

#[derive(Default)]
struct Known {
    processes: BTreeMap<Option<String>, Arc<Process>>,
    closed: bool,
}

impl Processes {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn start(&self, name: Option<&str>, spec: ProcessSpec) -> Outcome<State> {
        let mut known = self.known.lock().await;
        if known.closed {
            return Err(Refusal::Failed { message: "agent-master is ending".to_string() });
        }
        let name = name.map(str::to_string);
        if known.processes.get(&name).is_some_and(|earlier| earlier.running()) {
            return Err(Refusal::AlreadyRunning);
        }
        let started = spawn(spec)?;
        let state = started.state();
        known.processes.insert(name, Arc::new(started));
        Ok(state)
    }

    pub async fn window(&self, name: Option<&str>) -> Outcome<Window> {
        self.process(name).await?.window()
    }

    pub async fn clipboard(&self, name: Option<&str>) -> Outcome<Option<Vec<u8>>> {
        self.process(name).await?.clipboard()
    }

    pub async fn input(&self, name: Option<&str>, data: &[u8]) -> Outcome<()> {
        self.process(name).await?.input(data).await
    }

    pub async fn resize(&self, name: Option<&str>, size: WindowSize) -> Outcome<()> {
        self.process(name).await?.resize(size).await
    }

    pub async fn stop(&self, name: Option<&str>, grace: Duration) -> Outcome<State> {
        Ok(self.process(name).await?.stop(grace).await)
    }

    pub async fn state(&self, name: Option<&str>) -> State {
        let known = self.known.lock().await;
        let process = known.processes.get(&name.map(str::to_string));
        process.map_or(State::NotStarted, |process| process.state())
    }

    pub async fn send(&self, name: Option<&str>, message: String) -> Outcome<()> {
        self.process(name).await?.send(message).await
    }

    pub async fn messages(&self, name: Option<&str>, last_taken: u64) -> Outcome<Messages> {
        self.process(name).await?.messages(last_taken).await
    }

    pub async fn close(&self, grace: Duration) {
        let all: Vec<Arc<Process>> = {
            let mut known = self.known.lock().await;
            known.closed = true;
            known.processes.values().cloned().collect()
        };
        let stopping = all.iter().map(|process| process.stop(grace));
        futures_util::future::join_all(stopping).await;
    }

    async fn process(&self, name: Option<&str>) -> Outcome<Arc<Process>> {
        let known = self.known.lock().await;
        known.processes.get(&name.map(str::to_string)).cloned().ok_or(Refusal::NotStarted)
    }
}

fn spawn(spec: ProcessSpec) -> Outcome<Process> {
    match spec.terminal {
        Some(size) => spawn_under_a_terminal(spec, size),
        None => spawn_without_a_terminal(spec),
    }
}

fn spawn_under_a_terminal(spec: ProcessSpec, size: WindowSize) -> Outcome<Process> {
    if let Some(output) = &spec.output {
        return Err(Refusal::Failed {
            message: format!(
                "a process under a terminal prints to its terminal, not to {}",
                output.display()
            ),
        });
    }
    let (terminal, pts, painting) = Terminal::open(size)?;

    let child = pty_process::Command::new(&spec.command)
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .envs(spec.env)
        .spawn(pts)
        .with_context(|| format!("spawning {}", spec.command))
        .map_err(failed)?;
    let leader = pid_of(&child)?;

    let (exit_tx, exit_rx) = watch::channel(None);
    let abandon = painting.abort_handle();
    let pid_released = Arc::new(AtomicBool::new(false));
    let last_window = {
        let terminal = terminal.clone();
        async move {
            let _ = tokio::time::timeout(LAST_FRAME_WAIT, painting).await;
            Some(terminal.window())
        }
    };
    tokio::spawn(reap(child, pid_released.clone(), exit_tx, last_window));

    Ok(Process {
        terminal: Some(terminal),
        leader,
        pid_released,
        exit: exit_rx,
        painting: Some(abandon),
        message_socket: spec.message_socket.map(MessageSocket::at),
    })
}

fn spawn_without_a_terminal(spec: ProcessSpec) -> Outcome<Process> {
    let (stdout, stderr) = match &spec.output {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("opening {}", path.display()))
                .map_err(failed)?;
            let shared = file.try_clone().context("sharing the output").map_err(failed)?;
            (Stdio::from(file), Stdio::from(shared))
        }
        None => (Stdio::null(), Stdio::null()),
    };
    let child = tokio::process::Command::new(&spec.command)
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .envs(spec.env)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .process_group(0)
        .spawn()
        .with_context(|| format!("spawning {}", spec.command))
        .map_err(failed)?;
    let leader = pid_of(&child)?;

    let (exit_tx, exit_rx) = watch::channel(None);
    let pid_released = Arc::new(AtomicBool::new(false));
    tokio::spawn(reap(child, pid_released.clone(), exit_tx, async { None }));

    Ok(Process {
        terminal: None,
        leader,
        pid_released,
        exit: exit_rx,
        painting: None,
        message_socket: spec.message_socket.map(MessageSocket::at),
    })
}

fn pid_of(child: &tokio::process::Child) -> Outcome<i32> {
    child
        .id()
        .context("the spawned process has no pid")
        .and_then(|pid| i32::try_from(pid).context("the spawned process has an unreadable pid"))
        .map_err(failed)
}

fn painted(screen: &Screen) -> MutexGuard<'_, vt100::Parser<Clipboard>> {
    screen.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn failed(error: anyhow::Error) -> Refusal {
    Refusal::Failed { message: format!("{error:#}") }
}

async fn ended(exit: &mut watch::Receiver<Option<Exit>>) {
    while exit.borrow().is_none() {
        if exit.changed().await.is_err() {
            return;
        }
    }
}

async fn reap(
    mut child: tokio::process::Child,
    pid_released: Arc<AtomicBool>,
    exit: watch::Sender<Option<Exit>>,
    last_window: impl std::future::Future<Output = Option<Window>>,
) {
    let status = child.wait().await;
    pid_released.store(true, Ordering::SeqCst);
    let window = last_window.await;
    let (code, signal) = match status {
        Ok(status) => (status.code(), status.signal()),
        Err(error) => {
            log::error!("cannot tell how the process ended: {error}");
            (None, None)
        }
    };
    let _ = exit.send(Some(Exit { code, signal, window }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_terminal_left_poisoned_is_still_readable() {
        let (terminal, _pts, _painting) = Terminal::open(LEAST_WINDOW).expect("open a terminal");
        std::thread::scope(|threads| {
            threads
                .spawn(|| {
                    let _guard = terminal.screen.lock().expect("lock");
                    panic!("something went wrong while painting");
                })
                .join()
                .expect_err("the painting thread should have panicked");
        });

        assert!(terminal.screen.lock().is_err(), "the screen should be poisoned by now");
        assert_eq!(terminal.window().size, LEAST_WINDOW);
    }
}
