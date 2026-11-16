use std::collections::BTreeMap;
use std::path::PathBuf;

use base64::Engine;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowSize {
    pub rows: u16,
    pub cols: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    #[serde(rename = "size", default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<WindowSize>,
    /// Where a process without a terminal prints, both of its outputs appended;
    /// given none, what it prints is dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<PathBuf>,
    /// A unix socket the process serves WebSocket text messages on, for the
    /// messages to be passed between it and whoever controls it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_socket: Option<PathBuf>,
}

/// `contents` is a byte stream that repaints the window in full — colours,
/// attributes and cursor included — when fed to a terminal.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Window {
    pub size: WindowSize,
    #[serde(with = "base64_bytes")]
    pub contents: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum State {
    NotStarted,
    Running,
    /// `window` is the last one a process under a terminal had.
    Exited {
        code: Option<i32>,
        signal: Option<i32>,
        window: Option<Window>,
    },
}

/// A request about the process started without a name, except for
/// [`Request::About`], which carries one about a process started under a name.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Start(ProcessSpec),
    Window,
    Input {
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    Resize {
        size: WindowSize,
    },
    Stop {
        grace_ms: u64,
    },
    State,
    Clipboard,
    Send {
        message: String,
    },
    /// The messages the process sent on its message socket after the one
    /// numbered `last_taken`, which lets go of it and of every one before it —
    /// for whoever else asks too: one controller takes a process's messages.
    Messages {
        last_taken: u64,
    },
    /// An op of its own rather than a name on every request, so that an
    /// agent-master from before names refuses it as unreadable instead of
    /// applying it to its one process.
    About {
        name: String,
        request: Box<Request>,
    },
}

/// What a process sent on its message socket, numbered from 1 in the order it
/// arrived. `first` is the number of the first of `messages`, or of the next to
/// come when there are none; past `last_taken` + 1, the ones between were
/// dropped before they were taken.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Messages {
    pub first: u64,
    pub messages: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub enum Response {
    Done,
    Window(Window),
    State(State),
    Clipboard {
        #[serde(with = "base64_bytes_or_nothing")]
        data: Option<Vec<u8>>,
    },
    Messages(Messages),
    Refused(Refusal),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "fault", rename_all = "snake_case")]
pub enum Refusal {
    #[serde(rename = "nothing_started")]
    NotStarted,
    AlreadyRunning,
    AlreadyExited,
    UnusableWindowSize {
        least: WindowSize,
    },
    NoTerminal,
    NoMessageSocket,
    NotListening {
        message: String,
    },
    /// The process closed its message socket, or it went away.
    MessagesEnded,
    Failed {
        message: String,
    },
    Unreadable {
        message: String,
    },
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted => write!(out, "the process has not been started"),
            Self::AlreadyRunning => write!(out, "the process is already running"),
            Self::AlreadyExited => write!(out, "the process has already exited"),
            Self::UnusableWindowSize { least } => {
                write!(out, "a window smaller than {}x{} cannot be drawn", least.rows, least.cols)
            }
            Self::NoTerminal => write!(out, "the process runs without a terminal"),
            Self::NoMessageSocket => write!(out, "the process was started with no message socket"),
            Self::NotListening { message } => {
                write!(out, "nothing listens on the process's message socket yet: {message}")
            }
            Self::MessagesEnded => write!(out, "the process's message socket is closed"),
            Self::Failed { message } | Self::Unreadable { message } => write!(out, "{message}"),
        }
    }
}

pub enum Incoming<T> {
    Message(T),
    Unreadable(String),
    Ended,
}

pub async fn write_message<W, T>(sink: &mut W, message: &T) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
    T: Serialize,
{
    use tokio::io::AsyncWriteExt;

    let mut line = serde_json::to_string(message).expect("wire messages are always serializable");
    line.push('\n');
    sink.write_all(line.as_bytes()).await
}

pub async fn read_message<R, T>(source: &mut R) -> std::io::Result<Incoming<T>>
where
    R: tokio::io::AsyncBufRead + Unpin,
    T: DeserializeOwned,
{
    use tokio::io::AsyncBufReadExt;

    let mut line = String::new();
    if source.read_line(&mut line).await? == 0 {
        return Ok(Incoming::Ended);
    }
    let line = line.trim_end_matches(['\r', '\n']);
    Ok(match serde_json::from_str(line) {
        Ok(message) => Incoming::Message(message),
        Err(error) => Incoming::Unreadable(format!("{error}: {line}")),
    })
}

fn to_text(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn from_text<E: serde::de::Error>(text: &str) -> Result<Vec<u8>, E> {
    base64::engine::general_purpose::STANDARD.decode(text).map_err(E::custom)
}

mod base64_bytes_or_nothing {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        bytes: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match bytes {
            Some(bytes) => serializer.serialize_some(&super::to_text(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|encoded| super::from_text(&encoded))
            .transpose()
    }
}

mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::to_text(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        super::from_text(&String::deserialize(deserializer)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window() -> Window {
        Window { size: WindowSize { rows: 24, cols: 80 }, contents: vec![0x1b, b'[', b'2', b'J'] }
    }

    async fn wire<T: Serialize>(message: &T) -> Vec<u8> {
        let mut wire = Vec::new();
        write_message(&mut wire, message).await.expect("write");
        wire
    }

    async fn round_trip<T: Serialize + DeserializeOwned>(message: &T) -> T {
        let wire = wire(message).await;
        assert!(wire.ends_with(b"\n"), "{:?}", String::from_utf8_lossy(&wire));
        match read_message(&mut wire.as_slice()).await.expect("read") {
            Incoming::Message(back) => back,
            Incoming::Unreadable(complaint) => panic!("unreadable: {complaint}"),
            Incoming::Ended => panic!("nothing came back"),
        }
    }

    fn spec() -> ProcessSpec {
        ProcessSpec {
            command: "claude".to_string(),
            args: vec!["--model".to_string(), "opus".to_string()],
            cwd: PathBuf::from("/workdir"),
            env: BTreeMap::from([("TERM".to_string(), "xterm-256color".to_string())]),
            terminal: Some(WindowSize { rows: 80, cols: 100 }),
            output: None,
            message_socket: None,
        }
    }

    fn spec_without_a_terminal() -> ProcessSpec {
        ProcessSpec {
            command: "codex".to_string(),
            args: vec!["app-server".to_string()],
            cwd: PathBuf::from("/workdir"),
            env: BTreeMap::new(),
            terminal: None,
            output: Some(PathBuf::from("/state/app-server.log")),
            message_socket: Some(PathBuf::from("/state/app-server.sock")),
        }
    }

    fn about(name: &str, request: Request) -> Request {
        Request::About { name: name.to_string(), request: Box::new(request) }
    }

    #[tokio::test]
    async fn every_request_survives_the_wire() {
        let requests = vec![
            Request::Start(spec()),
            Request::Start(spec_without_a_terminal()),
            Request::Window,
            Request::Input { data: vec![0x1b, b'[', b'A', 0x0d] },
            Request::Resize { size: WindowSize { rows: 30, cols: 120 } },
            Request::Stop { grace_ms: 5000 },
            Request::State,
            Request::Clipboard,
            Request::Send { message: r#"{"id":1}"#.to_string() },
            Request::Messages { last_taken: 7 },
            about("app-server", Request::Start(spec_without_a_terminal())),
            about("app-server", Request::Stop { grace_ms: 0 }),
            about("app-server", Request::Messages { last_taken: 0 }),
        ];
        for request in requests {
            assert_eq!(round_trip(&request).await, request);
        }
    }

    #[tokio::test]
    async fn every_response_survives_the_wire() {
        let responses = vec![
            Response::Done,
            Response::Window(window()),
            Response::State(State::NotStarted),
            Response::State(State::Running),
            Response::State(State::Exited { code: Some(7), signal: None, window: Some(window()) }),
            Response::State(State::Exited { code: None, signal: Some(9), window: Some(window()) }),
            Response::State(State::Exited { code: Some(0), signal: None, window: None }),
            Response::Clipboard { data: None },
            Response::Clipboard { data: Some(Vec::new()) },
            Response::Clipboard { data: Some(vec![0x00, 0xff, b'h', b'i']) },
            Response::Messages(Messages { first: 1, messages: Vec::new() }),
            Response::Messages(Messages {
                first: 4,
                messages: vec!["one".to_string(), "two".to_string()],
            }),
            Response::Refused(Refusal::NotStarted),
            Response::Refused(Refusal::AlreadyRunning),
            Response::Refused(Refusal::AlreadyExited),
            Response::Refused(Refusal::UnusableWindowSize {
                least: WindowSize { rows: 2, cols: 2 },
            }),
            Response::Refused(Refusal::NoTerminal),
            Response::Refused(Refusal::NoMessageSocket),
            Response::Refused(Refusal::NotListening { message: "no such file".to_string() }),
            Response::Refused(Refusal::MessagesEnded),
            Response::Refused(Refusal::Failed { message: "spawning claude".to_string() }),
            Response::Refused(Refusal::Unreadable { message: "not json".to_string() }),
        ];
        for response in responses {
            assert_eq!(round_trip(&response).await, response);
        }
    }

    async fn reads_as<T>(line: &str) -> T
    where
        T: DeserializeOwned,
    {
        match read_message::<_, T>(&mut format!("{line}\n").as_bytes()).await.expect("read") {
            Incoming::Message(read) => read,
            Incoming::Unreadable(complaint) => panic!("{line} is unreadable: {complaint}"),
            Incoming::Ended => panic!("{line} read as nothing"),
        }
    }

    async fn written<T: Serialize>(message: &T) -> String {
        String::from_utf8(wire(message).await).expect("text").trim_end().to_string()
    }

    /// The other side may be built from another version — a container brings
    /// its own agent-master — so what is on the wire is pinned, not only that it
    /// comes back as it went.
    #[tokio::test]
    async fn the_wire_reads_and_is_written_word_for_word() {
        let requests = [
            (
                r#"{"op":"start","command":"claude","args":[],"cwd":"/w","env":{},"size":{"rows":2,"cols":3}}"#,
                Request::Start(ProcessSpec {
                    command: "claude".to_string(),
                    args: Vec::new(),
                    cwd: PathBuf::from("/w"),
                    env: BTreeMap::new(),
                    terminal: Some(WindowSize { rows: 2, cols: 3 }),
                    output: None,
                    message_socket: None,
                }),
            ),
            (r#"{"op":"window"}"#, Request::Window),
            (r#"{"op":"input","data":"eA=="}"#, Request::Input { data: vec![b'x'] }),
            (
                r#"{"op":"resize","size":{"rows":2,"cols":3}}"#,
                Request::Resize { size: WindowSize { rows: 2, cols: 3 } },
            ),
            (r#"{"op":"stop","grace_ms":5}"#, Request::Stop { grace_ms: 5 }),
            (r#"{"op":"state"}"#, Request::State),
            (r#"{"op":"clipboard"}"#, Request::Clipboard),
            (r#"{"op":"send","message":"hi"}"#, Request::Send { message: "hi".to_string() }),
            (r#"{"op":"messages","last_taken":3}"#, Request::Messages { last_taken: 3 }),
            (
                r#"{"op":"about","name":"app-server","request":{"op":"start","command":"codex","args":[],"cwd":"/w","env":{},"output":"/s/log","message_socket":"/s/sock"}}"#,
                about(
                    "app-server",
                    Request::Start(ProcessSpec {
                        command: "codex".to_string(),
                        args: Vec::new(),
                        cwd: PathBuf::from("/w"),
                        env: BTreeMap::new(),
                        terminal: None,
                        output: Some(PathBuf::from("/s/log")),
                        message_socket: Some(PathBuf::from("/s/sock")),
                    }),
                ),
            ),
            (
                r#"{"op":"about","name":"app-server","request":{"op":"state"}}"#,
                about("app-server", Request::State),
            ),
        ];
        for (line, request) in requests {
            assert_eq!(written(&request).await, line);
            assert_eq!(reads_as::<Request>(line).await, request);
        }

        let responses = [
            (
                r#"{"reply":"messages","first":2,"messages":["a"]}"#,
                Response::Messages(Messages { first: 2, messages: vec!["a".to_string()] }),
            ),
            (
                r#"{"reply":"state","state":"exited","code":0,"signal":null,"window":null}"#,
                Response::State(State::Exited { code: Some(0), signal: None, window: None }),
            ),
            (
                r#"{"reply":"refused","fault":"nothing_started"}"#,
                Response::Refused(Refusal::NotStarted),
            ),
            (
                r#"{"reply":"refused","fault":"no_terminal"}"#,
                Response::Refused(Refusal::NoTerminal),
            ),
            (
                r#"{"reply":"refused","fault":"no_message_socket"}"#,
                Response::Refused(Refusal::NoMessageSocket),
            ),
            (
                r#"{"reply":"refused","fault":"not_listening","message":"m"}"#,
                Response::Refused(Refusal::NotListening { message: "m".to_string() }),
            ),
            (
                r#"{"reply":"refused","fault":"messages_ended"}"#,
                Response::Refused(Refusal::MessagesEnded),
            ),
        ];
        for (line, response) in responses {
            assert_eq!(written(&response).await, line);
            assert_eq!(reads_as::<Response>(line).await, response);
        }
        let exited =
            Response::State(State::Exited { code: Some(0), signal: None, window: Some(window()) });
        assert!(written(&exited).await.contains(r#""window":{"size":"#));
    }

    #[tokio::test]
    async fn byte_payloads_travel_as_base64_text() {
        let wire = wire(&Request::Input { data: vec![0x1b, b'[', b'A'] }).await;
        let wire = String::from_utf8(wire).expect("the wire is text");
        assert!(wire.contains(r#""data":"G1tB""#), "{wire}");
    }

    #[tokio::test]
    async fn a_window_of_arbitrary_bytes_comes_back_byte_for_byte() {
        let contents: Vec<u8> = (0..=255u8).cycle().take(5000).collect();
        let sent = Window { size: WindowSize { rows: 24, cols: 80 }, contents };
        assert_eq!(round_trip(&Response::Window(sent.clone())).await, Response::Window(sent));
    }

    #[tokio::test]
    async fn an_empty_byte_payload_is_carried_as_such() {
        let sent = Request::Input { data: Vec::new() };
        assert_eq!(round_trip(&sent).await, Request::Input { data: Vec::new() });
    }

    #[tokio::test]
    async fn lines_that_are_not_messages_come_back_as_unreadable() {
        let unreadable = [
            "not json",
            r#"{"op":"fly"}"#,
            r#"{"op":"input","data":"!!!!"}"#,
            r#"{"op":"stop","grace_ms":"soon"}"#,
            "",
        ];
        for line in unreadable {
            let mut wire = format!("{line}\n");
            let read: Incoming<Request> = read_message(&mut wire.as_bytes()).await.expect("read");
            assert!(
                matches!(read, Incoming::Unreadable(_)),
                "{line:?} should not have been read as a request"
            );
            wire.clear();
        }
    }

    #[tokio::test]
    async fn a_clipboard_that_is_not_readable_text_is_rejected() {
        let mut line = r#"{"reply":"clipboard","data":"!!!!"}"#.to_string();
        line.push('\n');
        let read: Incoming<Response> = read_message(&mut line.as_bytes()).await.expect("read");
        assert!(matches!(read, Incoming::Unreadable(_)));
    }

    #[tokio::test]
    async fn a_closed_source_ends_the_conversation() {
        let read = read_message::<_, Request>(&mut b"".as_slice()).await.expect("read");
        assert!(matches!(read, Incoming::Ended));
    }

    #[tokio::test]
    async fn messages_are_read_one_line_at_a_time() {
        let mut wire = Vec::new();
        write_message(&mut wire, &Request::Window).await.expect("write");
        write_message(&mut wire, &Request::State).await.expect("write");
        let mut source = wire.as_slice();
        for expected in [Request::Window, Request::State] {
            match read_message::<_, Request>(&mut source).await.expect("read") {
                Incoming::Message(request) => assert_eq!(request, expected),
                _ => panic!("expected {expected:?}"),
            }
        }
        assert!(matches!(
            read_message::<_, Request>(&mut source).await.expect("read"),
            Incoming::Ended
        ));
    }

    #[test]
    fn every_refusal_reads_plainly() {
        assert_eq!(Refusal::NotStarted.to_string(), "the process has not been started");
        assert_eq!(Refusal::AlreadyRunning.to_string(), "the process is already running");
        assert_eq!(Refusal::AlreadyExited.to_string(), "the process has already exited");
        assert_eq!(
            Refusal::UnusableWindowSize { least: WindowSize { rows: 2, cols: 2 } }.to_string(),
            "a window smaller than 2x2 cannot be drawn"
        );
        assert_eq!(
            Refusal::Failed { message: "no such file".to_string() }.to_string(),
            "no such file"
        );
        assert_eq!(Refusal::Unreadable { message: "not json".to_string() }.to_string(), "not json");
        assert_eq!(Refusal::NoTerminal.to_string(), "the process runs without a terminal");
        assert_eq!(
            Refusal::NoMessageSocket.to_string(),
            "the process was started with no message socket"
        );
        assert_eq!(
            Refusal::NotListening { message: "refused".to_string() }.to_string(),
            "nothing listens on the process's message socket yet: refused"
        );
        assert_eq!(Refusal::MessagesEnded.to_string(), "the process's message socket is closed");
    }
}
