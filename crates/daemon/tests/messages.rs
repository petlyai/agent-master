use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use agent_master_client::{AgentMaster, Messages, ProcessSpec, Refusal, State};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

mod harness;
use harness::*;

type FarEnd = WebSocketStream<UnixStream>;

struct Socket {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

fn a_socket() -> Socket {
    let dir = tempfile::tempdir().expect("a directory");
    let path = dir.path().join("messages.sock");
    Socket { _dir: dir, path }
}

fn serving(path: &Path) -> UnixListener {
    UnixListener::bind(path).expect("bind the message socket")
}

/// The far end accepts on its own, for agent-master's handshake waits on it.
fn accepting(listener: UnixListener) -> JoinHandle<(FarEnd, UnixListener)> {
    tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(PATIENCE, listener.accept())
            .await
            .expect("in time")
            .expect("accept");
        (tokio_tungstenite::accept_async(stream).await.expect("a websocket"), listener)
    })
}

async fn heard(far_end: &mut FarEnd) -> String {
    loop {
        match tokio::time::timeout(PATIENCE, far_end.next()).await.expect("in time") {
            Some(Ok(Message::Text(text))) => return text.to_string(),
            Some(Ok(_)) => continue,
            other => panic!("nothing more was sent: {other:?}"),
        }
    }
}

fn with_socket(mut spec: ProcessSpec, socket: &Path) -> ProcessSpec {
    spec.message_socket = Some(socket.to_path_buf());
    spec
}

async fn started_with(master: &mut AgentMaster, socket: &Path) {
    let spec = with_socket(without_a_terminal("sleep 30"), socket);
    master.process("server").start(spec).await.expect("start");
}

async fn taken(master: &mut AgentMaster, count: usize) -> Vec<String> {
    let deadline = Instant::now() + PATIENCE;
    let mut taken: Vec<String> = Vec::new();
    while taken.len() < count {
        let last_taken = taken.len() as u64;
        let Messages { first, messages } =
            master.process("server").messages(last_taken).await.expect("messages");
        assert_eq!(first, last_taken + 1, "messages went missing");
        taken.extend(messages);
        assert!(Instant::now() < deadline, "only {taken:?} came");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    taken
}

#[tokio::test]
async fn messages_pass_both_ways_between_the_controller_and_the_process() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;

    master.process("server").send("hello".to_string()).await.expect("send");
    let (mut far_end, _) = far_end.await.expect("the far end");
    assert_eq!(heard(&mut far_end).await, "hello");

    far_end.send(Message::text("back")).await.expect("answer");
    assert_eq!(taken(&mut master, 1).await, vec!["back"]);
}

#[tokio::test]
async fn messages_come_numbered_oldest_first_and_are_let_go_of_once_taken() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    let nothing = master.process("server").messages(0).await.expect("messages");
    assert_eq!(nothing, Messages { first: 1, messages: Vec::new() });
    let (mut far_end, _) = far_end.await.expect("the far end");

    for message in ["one", "two", "three"] {
        far_end.send(Message::text(message)).await.expect("send");
    }
    assert_eq!(taken(&mut master, 3).await, vec!["one", "two", "three"]);
    let again = master.process("server").messages(1).await.expect("asked again after one");
    assert_eq!(again, Messages { first: 2, messages: vec!["two".into(), "three".into()] });
    let done = master.process("server").messages(3).await.expect("after all three");
    assert_eq!(done, Messages { first: 4, messages: Vec::new() });
    let behind = master.process("server").messages(1).await.expect("asked from behind");
    assert_eq!(behind, Messages { first: 4, messages: Vec::new() });
}

#[tokio::test]
async fn a_message_socket_nobody_listens_on_yet_is_tried_again_later() {
    let socket = a_socket();
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;

    let refused = master.process("server").send("early".to_string()).await.expect_err("send");
    let path = socket.path.display().to_string();
    assert!(
        matches!(refusal(refused), Refusal::NotListening { message } if message.contains(&path))
    );
    let refused = master.process("server").messages(0).await.expect_err("messages");
    assert!(matches!(refusal(refused), Refusal::NotListening { .. }));

    let far_end = accepting(serving(&socket.path));
    master.process("server").send("in time".to_string()).await.expect("send");
    let (mut far_end, _) = far_end.await.expect("the far end");
    assert_eq!(heard(&mut far_end).await, "in time");
}

async fn ended_after(master: &mut AgentMaster) -> Vec<String> {
    let deadline = Instant::now() + PATIENCE;
    let mut taken: Vec<String> = Vec::new();
    loop {
        match master.process("server").messages(taken.len() as u64).await {
            Ok(Messages { messages, .. }) => taken.extend(messages),
            Err(error) => {
                assert_eq!(refusal(error), Refusal::MessagesEnded);
                return taken;
            }
        }
        assert!(Instant::now() < deadline, "the messages never ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_closed_message_socket_ends_the_messages_once_the_last_ones_are_taken() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    master.process("server").messages(0).await.expect("messages");
    let (mut far_end, _) = far_end.await.expect("the far end");

    far_end.send(Message::text("last")).await.expect("send");
    far_end.close(None).await.expect("close");
    drop(far_end);

    assert_eq!(ended_after(&mut master).await, vec!["last"]);
    let refused = master.process("server").send("too late".to_string()).await.expect_err("send");
    assert_eq!(refusal(refused), Refusal::MessagesEnded);
}

#[tokio::test]
async fn a_far_end_gone_without_a_word_ends_the_messages_too() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    master.process("server").messages(0).await.expect("messages");
    let (mut far_end, _) = far_end.await.expect("the far end");

    far_end.send(Message::text("bye")).await.expect("send");
    drop(far_end);

    assert_eq!(ended_after(&mut master).await, vec!["bye"]);
    let refused = master.process("server").send("too late".to_string()).await.expect_err("send");
    assert_eq!(refusal(refused), Refusal::MessagesEnded);
}

#[tokio::test]
async fn frames_that_are_not_text_are_passed_over() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    master.process("server").messages(0).await.expect("messages");
    let (mut far_end, _) = far_end.await.expect("the far end");

    far_end.send(Message::binary(vec![0u8, 1, 2])).await.expect("binary");
    far_end.send(Message::Ping(vec![9u8].into())).await.expect("ping");
    far_end.send(Message::text("after")).await.expect("text");
    assert_eq!(taken(&mut master, 1).await, vec!["after"]);
    master.process("server").send("still here".to_string()).await.expect("send");
    assert_eq!(heard(&mut far_end).await, "still here");
}

#[tokio::test]
async fn what_arrived_before_the_process_ended_is_still_handed_over() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    master.process("server").messages(0).await.expect("messages");
    let (mut far_end, _) = far_end.await.expect("the far end");
    far_end.send(Message::text("last words")).await.expect("send");
    tokio::time::sleep(Duration::from_millis(100)).await;

    master.process("server").stop(Duration::from_secs(1)).await.expect("stop");
    let held = master.process("server").messages(0).await.expect("messages");
    assert_eq!(held.messages, vec!["last words"]);
}

#[tokio::test]
async fn a_process_started_with_no_message_socket_takes_no_messages() {
    let mut master = master().await;
    master.process("server").start(without_a_terminal("sleep 30")).await.expect("start");
    let refused = master.process("server").send("x".to_string()).await.expect_err("send");
    assert_eq!(refusal(refused), Refusal::NoMessageSocket);
    let refused = master.process("server").messages(0).await.expect_err("messages");
    assert_eq!(refusal(refused), Refusal::NoMessageSocket);
}

#[tokio::test]
async fn a_process_that_ended_before_its_socket_was_reached_neither_gives_nor_takes() {
    let socket = a_socket();
    let _listener = serving(&socket.path);
    let mut master = master().await;
    let spec = with_socket(without_a_terminal("exit 0"), &socket.path);
    master.process("server").start(spec).await.expect("start");
    assert!(matches!(ended_as(&mut master, "server").await, State::Exited { .. }));

    let asked = Instant::now();
    let refused = master.process("server").send("x".to_string()).await.expect_err("send");
    assert_eq!(refusal(refused), Refusal::MessagesEnded);
    let refused = master.process("server").messages(0).await.expect_err("messages");
    assert_eq!(refusal(refused), Refusal::MessagesEnded);
    assert!(asked.elapsed() < Duration::from_secs(1), "a connection was tried after all");
}

#[tokio::test]
async fn a_name_started_again_gets_a_connection_of_its_own() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    master.process("server").send("first".to_string()).await.expect("send");
    let (mut first, listener) = far_end.await.expect("the first far end");
    assert_eq!(heard(&mut first).await, "first");

    first.send(Message::text("a")).await.expect("send");
    first.send(Message::text("b")).await.expect("send");
    assert_eq!(taken(&mut master, 2).await, vec!["a", "b"]);
    master.process("server").messages(2).await.expect("let a and b go");

    master.process("server").stop(Duration::from_secs(1)).await.expect("stop");
    let second = accepting(listener);
    started_with(&mut master, &socket.path).await;
    master.process("server").send("second".to_string()).await.expect("send");
    let (mut second, _) = second.await.expect("the second far end");
    assert_eq!(heard(&mut second).await, "second");
    second.send(Message::text("c")).await.expect("send");
    assert_eq!(taken(&mut master, 1).await, vec!["c"], "the numbering starts again at 1");

    let closing = tokio::time::timeout(PATIENCE, first.next()).await.expect("in time");
    assert!(matches!(closing, Some(Ok(Message::Close(_))) | None), "{closing:?}");
}

#[tokio::test]
async fn a_process_under_a_terminal_may_have_a_message_socket_too() {
    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    master.start(with_socket(shell("sleep 30"), &socket.path)).await.expect("start");

    master.send("to the terminal one".to_string()).await.expect("send");
    let (mut far_end, _) = far_end.await.expect("the far end");
    assert_eq!(heard(&mut far_end).await, "to the terminal one");
    far_end.send(Message::text("from it")).await.expect("answer");

    let deadline = Instant::now() + PATIENCE;
    loop {
        let Messages { messages, .. } = master.messages(0).await.expect("messages");
        if !messages.is_empty() {
            assert_eq!(messages, vec!["from it"]);
            break;
        }
        assert!(Instant::now() < deadline, "nothing came back");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    master.stop(Duration::from_secs(1)).await.expect("stop");
}

#[tokio::test]
async fn a_message_socket_that_speaks_no_websocket_fails_the_connection() {
    let socket = a_socket();
    let listener = serving(&socket.path);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    let refused = master.process("server").send("x".to_string()).await.expect_err("send");
    let path = socket.path.display().to_string();
    assert!(matches!(
        refusal(refused),
        Refusal::Failed { message } if message.contains("opening a websocket") && message.contains(&path)
    ));
}

#[tokio::test]
async fn a_handshake_that_never_comes_fails_in_time_and_holds_up_nothing_else() {
    let socket = a_socket();
    let listener = serving(&socket.path);
    let held = tokio::spawn(async move { listener.accept().await.map(|(stream, _)| stream) });
    let address = listening().await;
    let mut master = AgentMaster::connect(address).await.expect("connect");
    started_with(&mut master, &socket.path).await;
    master.process("other").start(without_a_terminal("sleep 30")).await.expect("start other");

    let asked = Instant::now();
    let sending = tokio::spawn(async move {
        let refused = master.process("server").send("x".to_string()).await.expect_err("send");
        (refusal(refused), master)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut meanwhile = AgentMaster::connect(address).await.expect("a second connection");
    let answered = tokio::time::timeout(Duration::from_secs(1), async {
        assert_eq!(meanwhile.process("server").state().await.expect("state"), State::Running);
        assert_eq!(meanwhile.process("other").state().await.expect("state"), State::Running);
        meanwhile.process("other").stop(Duration::from_secs(1)).await.expect("stop other");
    })
    .await;
    assert!(answered.is_ok(), "the hung handshake held up requests that need no message");

    let (refused, _master) = sending.await.expect("the send");
    assert!(matches!(&refused, Refusal::Failed { message } if message.contains("handshake")));
    assert!(asked.elapsed() < Duration::from_secs(10));
    drop(held);
}

#[tokio::test]
async fn two_first_messages_at_once_make_one_connection() {
    let socket = a_socket();
    let listener = serving(&socket.path);
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counting = accepted.clone();
    let (heard_tx, mut heard_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counting.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let heard_tx = heard_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let mut far_end =
                    tokio_tungstenite::accept_async(stream).await.expect("a websocket");
                while let Some(Ok(Message::Text(text))) = far_end.next().await {
                    let _ = heard_tx.send(text.to_string());
                }
            });
        }
    });
    let address = listening().await;
    let mut master = AgentMaster::connect(address).await.expect("connect");
    started_with(&mut master, &socket.path).await;
    let mut other = AgentMaster::connect(address).await.expect("a second connection");

    let (a, b) = tokio::join!(
        master.process("server").send("a".to_string()),
        other.process("server").send("b".to_string()),
    );
    a.expect("send a");
    b.expect("send b");

    let mut heard = Vec::new();
    while heard.len() < 2 {
        heard.push(
            tokio::time::timeout(PATIENCE, heard_rx.recv()).await.expect("in time").expect("heard"),
        );
    }
    heard.sort();
    assert_eq!(heard, vec!["a", "b"]);
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_message_the_process_no_longer_reads_ends_the_sending_but_not_what_it_says() {
    use std::os::fd::AsRawFd;

    let socket = a_socket();
    let far_end = accepting(serving(&socket.path));
    let mut master = master().await;
    started_with(&mut master, &socket.path).await;
    master.process("server").send("first".to_string()).await.expect("send");
    let (mut far_end, _) = far_end.await.expect("the far end");
    assert_eq!(heard(&mut far_end).await, "first");
    unsafe {
        libc::shutdown(far_end.get_ref().as_raw_fd(), libc::SHUT_RD);
    }

    let deadline = Instant::now() + PATIENCE;
    loop {
        match master.process("server").send("x".repeat(1 << 16)).await {
            Ok(()) => {}
            Err(error) => {
                assert_eq!(refusal(error), Refusal::MessagesEnded);
                break;
            }
        }
        assert!(Instant::now() < deadline, "sending never ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    far_end.send(Message::text("still talking")).await.expect("send");
    assert_eq!(taken(&mut master, 1).await, vec!["still talking"]);
}
