use std::sync::Arc;
use std::time::Duration;

use agent_master_protocol::{read_message, write_message, Incoming, Refusal, Request, Response};
use anyhow::Result;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

use crate::processes::{Outcome, Processes};

pub async fn serve(listener: TcpListener, processes: Arc<Processes>) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let processes = processes.clone();
        tokio::spawn(async move {
            if let Err(error) = converse(stream, processes).await {
                log::warn!("connection from {peer} ended: {error}");
            }
        });
    }
}

async fn converse(stream: TcpStream, processes: Arc<Processes>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    loop {
        let response = match read_message(&mut read).await? {
            Incoming::Message(request) => act(&processes, request).await,
            Incoming::Unreadable(message) => Response::Refused(Refusal::Unreadable { message }),
            Incoming::Ended => return Ok(()),
        };
        write_message(&mut write, &response).await?;
    }
}

async fn act(processes: &Processes, request: Request) -> Response {
    let done = match request {
        Request::About { name, request } => act_on(processes, Some(&name), *request).await,
        request => act_on(processes, None, request).await,
    };
    done.unwrap_or_else(Response::Refused)
}

async fn act_on(processes: &Processes, name: Option<&str>, request: Request) -> Outcome<Response> {
    match request {
        Request::Start(spec) => processes.start(name, spec).await.map(Response::State),
        Request::Window => processes.window(name).await.map(Response::Window),
        Request::Input { data } => processes.input(name, &data).await.map(|()| Response::Done),
        Request::Resize { size } => processes.resize(name, size).await.map(|()| Response::Done),
        Request::Stop { grace_ms } => {
            processes.stop(name, Duration::from_millis(grace_ms)).await.map(Response::State)
        }
        Request::State => Ok(Response::State(processes.state(name).await)),
        Request::Clipboard => {
            processes.clipboard(name).await.map(|data| Response::Clipboard { data })
        }
        Request::Send { message } => processes.send(name, message).await.map(|()| Response::Done),
        Request::Messages { last_taken } => {
            processes.messages(name, last_taken).await.map(Response::Messages)
        }
        Request::About { .. } => Err(Refusal::Unreadable {
            message: "a request about a named process cannot carry one about another".to_string(),
        }),
    }
}
