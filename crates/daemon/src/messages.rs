use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as SyncMutex, MutexGuard, PoisonError};
use std::time::Duration;

use agent_master_protocol::{Messages, Refusal};
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

use crate::processes::Outcome;

const HOLD_LIMIT: usize = 10_000;

/// A socket that accepts and never answers the handshake would otherwise hold
/// every message to and from its process for good.
const HANDSHAKE_PATIENCE: Duration = Duration::from_secs(5);

/// The connection to a process's message socket, made the first time a message
/// is sent or asked for — a process takes a while to start listening — and kept
/// for as long as the process holds it open. A connection that ended is never
/// made again: a new one would be a new conversation, which the controller
/// would not know it is in.
pub struct MessageSocket {
    path: PathBuf,
    hold_limit: usize,
    link: Mutex<Option<Arc<Link>>>,
}

struct Link {
    outgoing: mpsc::UnboundedSender<String>,
    arrived: Arc<SyncMutex<Arrived>>,
}

/// Held until the controller says it has them, so that an answer it never got
/// costs it nothing: it asks again after the same number.
struct Arrived {
    first: u64,
    held: VecDeque<String>,
    ended: bool,
}

impl Arrived {
    fn new() -> Self {
        Self { first: 1, held: VecDeque::new(), ended: false }
    }

    fn hold(&mut self, message: String, hold_limit: usize) -> Option<String> {
        self.held.push_back(message);
        if self.held.len() > hold_limit {
            return self.let_go_of_the_oldest();
        }
        None
    }

    fn let_go_up_to(&mut self, last_taken: u64) {
        while self.first <= last_taken && !self.held.is_empty() {
            self.let_go_of_the_oldest();
        }
    }

    fn let_go_of_the_oldest(&mut self) -> Option<String> {
        let oldest = self.held.pop_front();
        self.first += 1;
        oldest
    }
}

impl MessageSocket {
    pub fn at(path: PathBuf) -> Self {
        Self { path, hold_limit: HOLD_LIMIT, link: Mutex::new(None) }
    }

    pub async fn send(&self, message: String, process_runs: bool) -> Outcome<()> {
        let link = self.link(process_runs).await?;
        if lock(&link.arrived).ended || link.outgoing.send(message).is_err() {
            return Err(Refusal::MessagesEnded);
        }
        Ok(())
    }

    pub async fn messages(&self, last_taken: u64, process_runs: bool) -> Outcome<Messages> {
        let link = self.link(process_runs).await?;
        let mut arrived = lock(&link.arrived);
        arrived.let_go_up_to(last_taken);
        if arrived.held.is_empty() && arrived.ended {
            return Err(Refusal::MessagesEnded);
        }
        Ok(Messages { first: arrived.first, messages: arrived.held.iter().cloned().collect() })
    }

    async fn link(&self, process_runs: bool) -> Outcome<Arc<Link>> {
        let mut link = self.link.lock().await;
        if let Some(made) = &*link {
            return Ok(made.clone());
        }
        if !process_runs {
            return Err(Refusal::MessagesEnded);
        }
        let made = Arc::new(connect(&self.path, self.hold_limit).await?);
        *link = Some(made.clone());
        Ok(made)
    }
}

async fn connect(path: &Path, hold_limit: usize) -> Outcome<Link> {
    let stream = UnixStream::connect(path).await.map_err(|error| Refusal::NotListening {
        message: format!("{}: {error}", path.display()),
    })?;
    let handshake = tokio_tungstenite::client_async("ws://localhost/", stream);
    let (websocket, _) = match tokio::time::timeout(HANDSHAKE_PATIENCE, handshake).await {
        Ok(Ok(opened)) => opened,
        Ok(Err(error)) => {
            return Err(Refusal::Failed {
                message: format!("opening a websocket on {}: {error}", path.display()),
            })
        }
        Err(_) => {
            return Err(Refusal::Failed {
                message: format!(
                    "{} did not answer a websocket handshake in {HANDSHAKE_PATIENCE:?}",
                    path.display()
                ),
            })
        }
    };
    let (mut sink, mut source) = websocket.split();
    let (outgoing, mut to_send) = mpsc::unbounded_channel::<String>();
    let arrived = Arc::new(SyncMutex::new(Arrived::new()));

    tokio::spawn(async move {
        while let Some(message) = to_send.recv().await {
            if let Err(error) = sink.send(Message::text(message)).await {
                log::warn!("writing to the process's message socket failed: {error}");
                return;
            }
        }
        let _ = sink.close().await;
    });

    let reading = arrived.clone();
    let socket = path.display().to_string();
    tokio::spawn(async move {
        while let Some(read) = source.next().await {
            match read {
                Ok(Message::Text(text)) => {
                    if lock(&reading).hold(text.to_string(), hold_limit).is_some() {
                        log::warn!(
                            "{socket}: {hold_limit} messages are held untaken, dropping the oldest"
                        );
                    }
                }
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(error) => {
                    log::warn!("{socket}: reading the process's messages failed: {error}");
                    break;
                }
            }
        }
        lock(&reading).ended = true;
    });

    Ok(Link { outgoing, arrived })
}

fn lock(arrived: &SyncMutex<Arrived>) -> MutexGuard<'_, Arrived> {
    arrived.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    async fn far_end_sending(
        path: &Path,
        sent: &'static [&'static str],
    ) -> tokio::task::JoinHandle<tokio_tungstenite::WebSocketStream<UnixStream>> {
        let listener = UnixListener::bind(path).expect("bind");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut far_end = tokio_tungstenite::accept_async(stream).await.expect("a websocket");
            for message in sent {
                far_end.send(Message::text(*message)).await.expect("send");
            }
            far_end
        })
    }

    async fn until_the_last_is(link: &Link, last: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while lock(&link.arrived).held.back().map(String::as_str) != Some(last) {
            assert!(tokio::time::Instant::now() < deadline, "the messages never all arrived");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn messages(first: u64, texts: &[&str]) -> Messages {
        Messages { first, messages: texts.iter().map(|text| text.to_string()).collect() }
    }

    #[tokio::test]
    async fn messages_nobody_reads_are_held_up_to_a_limit_dropping_the_oldest() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("messages.sock");
        let far_end = far_end_sending(&path, &["one", "two", "three", "four", "five"]).await;
        let socket = MessageSocket { hold_limit: 3, ..MessageSocket::at(path) };
        let link = socket.link(true).await.expect("connected");
        let _far_end = far_end.await.expect("the far end");
        until_the_last_is(&link, "five").await;

        let held = socket.messages(0, true).await.expect("messages");
        assert_eq!(held, messages(3, &["three", "four", "five"]));
    }

    #[tokio::test]
    async fn messages_are_let_go_of_only_once_the_controller_says_it_has_them() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("messages.sock");
        let far_end = far_end_sending(&path, &["one", "two", "three"]).await;
        let socket = MessageSocket::at(path);
        let link = socket.link(true).await.expect("connected");
        let _far_end = far_end.await.expect("the far end");
        until_the_last_is(&link, "three").await;

        let all = messages(1, &["one", "two", "three"]);
        assert_eq!(socket.messages(0, true).await.expect("messages"), all);
        assert_eq!(socket.messages(0, true).await.expect("asked again"), all);
        assert_eq!(socket.messages(2, true).await.expect("after two"), messages(3, &["three"]));
        assert_eq!(socket.messages(9, true).await.expect("past all"), messages(4, &[]));
    }

    #[tokio::test]
    async fn a_message_the_writer_can_no_longer_take_is_refused_as_ended() {
        let (outgoing, to_send) = mpsc::unbounded_channel();
        drop(to_send);
        let socket = MessageSocket::at(PathBuf::from("/never/connected"));
        let link = Link { outgoing, arrived: Arc::new(SyncMutex::new(Arrived::new())) };
        *socket.link.lock().await = Some(Arc::new(link));
        assert_eq!(socket.send("x".to_string(), true).await, Err(Refusal::MessagesEnded));
    }

    #[tokio::test]
    async fn what_arrived_is_still_handed_over_after_a_panic_poisoned_it() {
        let arrived = Arc::new(SyncMutex::new(Arrived::new()));
        lock(&arrived).hold("kept".to_string(), HOLD_LIMIT);
        let poisoning = arrived.clone();
        std::thread::spawn(move || {
            let _held = poisoning.lock().expect("lock");
            panic!("something went wrong while holding the messages");
        })
        .join()
        .expect_err("the thread should have panicked");
        assert!(arrived.lock().is_err(), "the messages should be poisoned by now");

        let (outgoing, _to_send) = mpsc::unbounded_channel();
        let socket = MessageSocket::at(PathBuf::from("/never/connected"));
        *socket.link.lock().await = Some(Arc::new(Link { outgoing, arrived }));
        assert_eq!(socket.messages(0, true).await.expect("messages"), messages(1, &["kept"]));
    }
}
