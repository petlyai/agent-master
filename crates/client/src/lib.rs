use std::time::Duration;

use agent_master_protocol::{read_message, write_message, Incoming, Request, Response};
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

pub use agent_master_protocol::{Messages, ProcessSpec, Refusal, State, Window, WindowSize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cannot reach agent-master: {0}")]
    Unreachable(#[from] std::io::Error),
    #[error("agent-master hung up")]
    HungUp,
    #[error("agent-master said something unreadable: {0}")]
    Unreadable(String),
    #[error("agent-master refused: {0}")]
    Refused(Refusal),
    #[error("agent-master answered {answer} to {asked}")]
    Mismatched { asked: &'static str, answer: &'static str },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct AgentMaster {
    write: OwnedWriteHalf,
    read: BufReader<OwnedReadHalf>,
}

impl AgentMaster {
    pub async fn connect(address: impl ToSocketAddrs) -> Result<Self> {
        let (read, write) = TcpStream::connect(address).await?.into_split();
        Ok(Self { write, read: BufReader::new(read) })
    }

    pub fn process(&mut self, name: &str) -> Process<'_> {
        Process { master: self, name: Some(name.to_string()) }
    }

    pub fn unnamed_process(&mut self) -> Process<'_> {
        Process { master: self, name: None }
    }

    pub async fn start(&mut self, spec: ProcessSpec) -> Result<State> {
        self.unnamed_process().start(spec).await
    }

    pub async fn window(&mut self) -> Result<Window> {
        self.unnamed_process().window().await
    }

    pub async fn input(&mut self, data: &[u8]) -> Result<()> {
        self.unnamed_process().input(data).await
    }

    pub async fn resize(&mut self, size: WindowSize) -> Result<()> {
        self.unnamed_process().resize(size).await
    }

    pub async fn stop(&mut self, grace: Duration) -> Result<State> {
        self.unnamed_process().stop(grace).await
    }

    pub async fn state(&mut self) -> Result<State> {
        self.unnamed_process().state().await
    }

    pub async fn clipboard(&mut self) -> Result<Option<Vec<u8>>> {
        self.unnamed_process().clipboard().await
    }

    pub async fn send(&mut self, message: String) -> Result<()> {
        self.unnamed_process().send(message).await
    }

    pub async fn messages(&mut self, last_taken: u64) -> Result<Messages> {
        self.unnamed_process().messages(last_taken).await
    }

    async fn ask(&mut self, request: Request) -> Result<Response> {
        write_message(&mut self.write, &request).await?;
        match read_message(&mut self.read).await? {
            Incoming::Message(Response::Refused(refusal)) => Err(Error::Refused(refusal)),
            Incoming::Message(response) => Ok(response),
            Incoming::Unreadable(message) => Err(Error::Unreadable(message)),
            Incoming::Ended => Err(Error::HungUp),
        }
    }
}

pub struct Process<'a> {
    master: &'a mut AgentMaster,
    name: Option<String>,
}

impl Process<'_> {
    pub async fn start(self, spec: ProcessSpec) -> Result<State> {
        expect_state(self.ask(Request::Start(spec)).await?, "start")
    }

    pub async fn window(self) -> Result<Window> {
        match self.ask(Request::Window).await? {
            Response::Window(window) => Ok(window),
            other => Err(mismatch("window", &other)),
        }
    }

    pub async fn input(self, data: &[u8]) -> Result<()> {
        expect_done(self.ask(Request::Input { data: data.to_vec() }).await?, "input")
    }

    pub async fn resize(self, size: WindowSize) -> Result<()> {
        expect_done(self.ask(Request::Resize { size }).await?, "resize")
    }

    pub async fn stop(self, grace: Duration) -> Result<State> {
        let grace_ms = grace.as_millis().try_into().unwrap_or(u64::MAX);
        expect_state(self.ask(Request::Stop { grace_ms }).await?, "stop")
    }

    pub async fn state(self) -> Result<State> {
        expect_state(self.ask(Request::State).await?, "state")
    }

    pub async fn clipboard(self) -> Result<Option<Vec<u8>>> {
        match self.ask(Request::Clipboard).await? {
            Response::Clipboard { data } => Ok(data),
            other => Err(mismatch("clipboard", &other)),
        }
    }

    pub async fn send(self, message: String) -> Result<()> {
        expect_done(self.ask(Request::Send { message }).await?, "send")
    }

    pub async fn messages(self, last_taken: u64) -> Result<Messages> {
        match self.ask(Request::Messages { last_taken }).await? {
            Response::Messages(messages) => Ok(messages),
            other => Err(mismatch("messages", &other)),
        }
    }

    async fn ask(self, request: Request) -> Result<Response> {
        let request = match self.name {
            Some(name) => Request::About { name, request: Box::new(request) },
            None => request,
        };
        self.master.ask(request).await
    }
}

fn expect_state(response: Response, asked: &'static str) -> Result<State> {
    match response {
        Response::State(state) => Ok(state),
        other => Err(mismatch(asked, &other)),
    }
}

fn expect_done(response: Response, asked: &'static str) -> Result<()> {
    match response {
        Response::Done => Ok(()),
        other => Err(mismatch(asked, &other)),
    }
}

fn mismatch(asked: &'static str, answer: &Response) -> Error {
    let answer = match answer {
        Response::Done => "done",
        Response::Window(_) => "a window",
        Response::State(_) => "a state",
        Response::Clipboard { .. } => "a clipboard",
        Response::Messages(_) => "messages",
        Response::Refused(_) => "a refusal",
    };
    Error::Mismatched { asked, answer }
}
