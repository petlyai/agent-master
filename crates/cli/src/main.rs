use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use agent_master_client::{AgentMaster, Messages, Process, ProcessSpec, State, WindowSize};
use anyhow::{anyhow, bail, Result};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(about = "Drives one agent-master program by hand")]
struct Cli {
    /// Address the agent-master program listens on.
    #[arg(long, env = "AGENT_MASTER_ADDR")]
    server: String,
    /// The process to drive, by the name it was started under. Left out, it is
    /// the one started without a name.
    #[arg(long, global = true)]
    process: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the process.
    Start {
        /// Working directory, as agent-master sees it.
        #[arg(long, default_value = ".")]
        cwd: PathBuf,
        /// Environment variable to set on top of what agent-master itself runs with.
        #[arg(long = "env", value_name = "NAME=VALUE", value_parser = split_env)]
        env: Vec<(String, String)>,
        /// Height of the window, in rows.
        #[arg(long, default_value_t = 24)]
        rows: u16,
        /// Width of the window, in columns.
        #[arg(long, default_value_t = 80)]
        cols: u16,
        /// Run it without a terminal, so that it has no window.
        #[arg(long)]
        no_terminal: bool,
        /// Where a process without a terminal prints; left out, what it prints is dropped.
        #[arg(long, requires = "no_terminal")]
        output: Option<PathBuf>,
        /// A unix socket the process serves WebSocket text messages on.
        #[arg(long)]
        message_socket: Option<PathBuf>,
        /// The command and its arguments.
        #[arg(required = true, last = true)]
        argv: Vec<String>,
    },
    /// Write the window to stdout, as the terminal would paint it.
    ///
    /// On a terminal it takes a screen of its own and follows the process until
    /// you press q; anywhere else it writes the bytes once, as they are.
    Window {
        /// Show the window as it is now instead of following it.
        #[arg(long)]
        freeze: bool,
    },
    /// Type into the process: the given text, or raw bytes read from stdin.
    ///
    /// Keys that are not plain text travel as the bytes a terminal sends for them:
    ///
    ///   up      $'\033[A'    Enter      $'\r'
    ///   down    $'\033[B'    Tab        $'\t'
    ///   right   $'\033[C'    Escape     $'\033'
    ///   left    $'\033[D'    Backspace  $'\177'
    ///                        Ctrl-C     $'\003'
    ///
    /// An application that has switched its cursor keys to the application mode
    /// wants $'\033OB' for down, and so on for the other three.
    ///
    /// The $'...' form is bash and zsh; anywhere else, pipe the bytes in instead:
    ///
    ///   printf '\033[B' | amctl input
    #[command(verbatim_doc_comment)]
    Input {
        /// What to type. Left out, the bytes are read from stdin instead.
        text: Option<String>,
    },
    /// Set the window size.
    Resize {
        /// Height of the window, in rows.
        #[arg(long)]
        rows: u16,
        /// Width of the window, in columns.
        #[arg(long)]
        cols: u16,
    },
    /// Stop the process, gently first and forcibly once the grace runs out.
    Stop {
        /// How long to wait for it to end gently, in milliseconds.
        #[arg(long, default_value_t = 5_000)]
        grace_ms: u64,
    },
    /// Report whether the process runs, or how it ended.
    State,
    /// Write out what the process last put on the clipboard.
    Clipboard,
    /// Send the process one message on its message socket: the given text, or
    /// what is read from stdin.
    Send {
        /// The message. Left out, it is read from stdin instead.
        message: Option<String>,
    },
    /// Write out the messages the process sent on its message socket, oldest
    /// first, one to a line after its number and a tab.
    Messages {
        /// The number of the last message already taken: agent-master lets go of
        /// it and of every one before it, for whoever else reads them too. Left
        /// out, nothing is let go of.
        #[arg(long, default_value_t = 0)]
        last_taken: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut master = AgentMaster::connect(&cli.server).await?;
    let name = cli.process.as_deref();
    match cli.command {
        Command::Start { cwd, env, rows, cols, no_terminal, output, message_socket, argv } => {
            let (command, args) = argv.split_first().ok_or_else(|| anyhow!("no command given"))?;
            let spec = ProcessSpec {
                command: command.clone(),
                args: args.to_vec(),
                cwd,
                env: BTreeMap::from_iter(env),
                terminal: (!no_terminal).then_some(WindowSize { rows, cols }),
                output,
                message_socket,
            };
            println!("{}", describe(&driven(&mut master, name).start(spec).await?));
        }
        Command::Window { freeze } => show(&mut master, name, freeze).await?,
        Command::Input { text } => {
            let data = match text {
                Some(text) => text.into_bytes(),
                None => read_stdin().await?,
            };
            driven(&mut master, name).input(&data).await?;
        }
        Command::Resize { rows, cols } => {
            driven(&mut master, name).resize(WindowSize { rows, cols }).await?
        }
        Command::Stop { grace_ms } => {
            let stopped = driven(&mut master, name).stop(Duration::from_millis(grace_ms)).await?;
            println!("{}", describe(&stopped));
        }
        Command::State => println!("{}", describe(&driven(&mut master, name).state().await?)),
        Command::Clipboard => {
            let mut stdout = tokio::io::stdout();
            let copied = driven(&mut master, name).clipboard().await?;
            stdout.write_all(&copied.unwrap_or_default()).await?;
            stdout.flush().await?;
        }
        Command::Send { message } => {
            let message = match message {
                Some(message) => message,
                None => String::from_utf8(read_stdin().await?)?,
            };
            driven(&mut master, name).send(message).await?;
        }
        Command::Messages { last_taken } => {
            let Messages { first, messages } =
                driven(&mut master, name).messages(last_taken).await?;
            for (number, message) in (first..).zip(messages) {
                println!("{number}\t{message}");
            }
        }
    }
    Ok(())
}

fn driven<'a>(master: &'a mut AgentMaster, name: Option<&str>) -> Process<'a> {
    match name {
        Some(name) => master.process(name),
        None => master.unnamed_process(),
    }
}

async fn read_stdin() -> Result<Vec<u8>> {
    let mut data = Vec::new();
    tokio::io::stdin().read_to_end(&mut data).await?;
    Ok(data)
}

async fn show(master: &mut AgentMaster, name: Option<&str>, freeze: bool) -> Result<()> {
    use std::io::Write;

    let mut shown = driven(master, name).window().await?.contents;
    if !(std::io::stdout().is_terminal() && std::io::stdin().is_terminal()) {
        std::io::stdout().write_all(&shown)?;
        return Ok(std::io::stdout().flush()?);
    }

    let _held = HeldScreen::enter()?;
    draw(&shown)?;
    loop {
        match key_within(REFRESH)? {
            Key::Pressed(b'q' | b'Q' | ESCAPE | INTERRUPT) | Key::Ended => return Ok(()),
            Key::Pressed(_) => continue,
            Key::Nothing => {}
        }
        if freeze {
            continue;
        }
        let next = driven(master, name).window().await?.contents;
        if next != shown {
            draw(&next)?;
            shown = next;
        }
    }
}

fn draw(window: &[u8]) -> Result<()> {
    use std::io::Write;

    std::io::stdout().write_all(window)?;
    Ok(std::io::stdout().flush()?)
}

enum Key {
    Pressed(u8),
    Nothing,
    Ended,
}

fn key_within(patience: Duration) -> Result<Key> {
    use std::io::Read;

    let mut waiting = libc::pollfd { fd: libc::STDIN_FILENO, events: libc::POLLIN, revents: 0 };
    let ready = unsafe {
        libc::poll(&mut waiting, 1, i32::try_from(patience.as_millis()).unwrap_or(i32::MAX))
    };
    if ready < 0 {
        bail!("cannot wait for a key");
    }
    if ready == 0 {
        return Ok(Key::Nothing);
    }
    let mut key = [0u8; 1];
    match std::io::stdin().read(&mut key)? {
        0 => Ok(Key::Ended),
        _ => Ok(Key::Pressed(key[0])),
    }
}

const REFRESH: Duration = Duration::from_millis(100);
const ESCAPE: u8 = 0x1b;
const INTERRUPT: u8 = 0x03;
const ALTERNATE_SCREEN_ON: &[u8] = b"\x1b[?1049h";
const ALTERNATE_SCREEN_OFF: &[u8] = b"\x1b[?1049l";
const SHOW_CURSOR: &[u8] = b"\x1b[?25h";

struct HeldScreen {
    settings: libc::termios,
}

impl HeldScreen {
    fn enter() -> Result<Self> {
        use std::io::Write;

        let mut settings: libc::termios = unsafe { std::mem::zeroed() };
        unsafe {
            if libc::tcgetattr(libc::STDIN_FILENO, &mut settings) != 0 {
                bail!("cannot read the terminal's settings");
            }
            let mut raw = settings;
            libc::cfmakeraw(&mut raw);
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) != 0 {
                bail!("cannot take single keys from the terminal");
            }
        }
        std::io::stdout().write_all(ALTERNATE_SCREEN_ON)?;
        std::io::stdout().flush()?;
        Ok(Self { settings })
    }
}

impl Drop for HeldScreen {
    fn drop(&mut self) {
        use std::io::Write;

        let _ = std::io::stdout().write_all(ALTERNATE_SCREEN_OFF);
        let _ = std::io::stdout().write_all(SHOW_CURSOR);
        let _ = std::io::stdout().flush();
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.settings);
        }
    }
}

fn split_env(pair: &str) -> Result<(String, String)> {
    let (name, value) = pair.split_once('=').ok_or_else(|| anyhow!("expected NAME=VALUE"))?;
    Ok((name.to_string(), value.to_string()))
}

fn describe(state: &State) -> String {
    match state {
        State::NotStarted => "not started".to_string(),
        State::Running => "running".to_string(),
        State::Exited { code, signal, window } => {
            let ending = match (code, signal) {
                (Some(code), _) => format!("exit code {code}"),
                (None, Some(signal)) => format!("killed by signal {signal}"),
                (None, None) => "ended for an unknown reason".to_string(),
            };
            match window {
                Some(window) => format!(
                    "exited: {ending}; last window {}x{}, {} bytes",
                    window.size.rows,
                    window.size.cols,
                    window.contents.len()
                ),
                None => format!("exited: {ending}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_master_client::Window;

    fn window() -> Window {
        Window { size: WindowSize { rows: 24, cols: 80 }, contents: vec![b'x'; 12] }
    }

    use clap::CommandFactory;

    fn parse(argv: &[&str]) -> Cli {
        Cli::try_parse_from(argv).expect("parse")
    }

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn starting_takes_the_command_after_a_double_dash_and_fills_in_the_rest() {
        let cli = parse(&["amctl", "--server", "here:1", "start", "--", "sh", "-c", "echo hi"]);
        let Command::Start { cwd, env, rows, cols, no_terminal, output, message_socket, argv } =
            cli.command
        else {
            panic!("expected a start");
        };
        assert_eq!(cwd, PathBuf::from("."));
        assert!(env.is_empty());
        assert_eq!((rows, cols), (24, 80));
        assert!(!no_terminal);
        assert_eq!((output, message_socket), (None, None));
        assert_eq!(argv, vec!["sh", "-c", "echo hi"]);
        assert_eq!(cli.process, None);
    }

    #[test]
    fn a_process_started_without_a_terminal_may_say_where_it_prints_and_listens() {
        let cli = parse(&[
            "amctl",
            "--server",
            "here:1",
            "--process",
            "app-server",
            "start",
            "--no-terminal",
            "--output",
            "/state/log",
            "--message-socket",
            "/state/sock",
            "--",
            "codex",
        ]);
        assert_eq!(cli.process.as_deref(), Some("app-server"));
        let Command::Start { no_terminal, output, message_socket, .. } = cli.command else {
            panic!("expected a start");
        };
        assert!(no_terminal);
        assert_eq!(output, Some(PathBuf::from("/state/log")));
        assert_eq!(message_socket, Some(PathBuf::from("/state/sock")));
    }

    #[test]
    fn an_output_is_for_a_process_without_a_terminal_only() {
        assert!(Cli::try_parse_from([
            "amctl",
            "--server",
            "here:1",
            "start",
            "--output",
            "/state/log",
            "--",
            "sh"
        ])
        .is_err());
    }

    #[test]
    fn the_process_may_be_named_after_the_command_too() {
        let cli = parse(&["amctl", "--server", "here:1", "state", "--process", "connector"]);
        assert_eq!(cli.process.as_deref(), Some("connector"));
    }

    #[test]
    fn environment_pairs_pile_up_and_the_last_of_a_name_wins() {
        let cli = parse(&[
            "amctl", "--server", "here:1", "start", "--env", "A=1", "--env", "A=2", "--env", "B=3",
            "--", "sh",
        ]);
        let Command::Start { env, .. } = cli.command else { panic!("expected a start") };
        assert_eq!(
            BTreeMap::from_iter(env),
            BTreeMap::from([
                ("A".to_string(), "2".to_string()),
                ("B".to_string(), "3".to_string())
            ])
        );
    }

    #[test]
    fn a_start_without_a_command_is_rejected() {
        assert!(Cli::try_parse_from(["amctl", "--server", "here:1", "start"]).is_err());
    }

    #[test]
    fn an_environment_pair_without_a_value_is_rejected_at_the_command_line() {
        assert!(Cli::try_parse_from([
            "amctl", "--server", "here:1", "start", "--env", "NOEQ", "--", "sh"
        ])
        .is_err());
    }

    #[test]
    fn stopping_waits_five_seconds_unless_told_otherwise() {
        let Command::Stop { grace_ms } = parse(&["amctl", "--server", "here:1", "stop"]).command
        else {
            panic!("expected a stop")
        };
        assert_eq!(grace_ms, 5_000);
    }

    #[test]
    fn the_address_may_come_from_the_environment_instead() {
        std::env::set_var("AGENT_MASTER_ADDR", "from-the-environment:1");
        assert_eq!(parse(&["amctl", "state"]).server, "from-the-environment:1");
        std::env::remove_var("AGENT_MASTER_ADDR");
    }

    #[test]
    fn an_environment_pair_splits_on_the_first_equals_sign() {
        assert_eq!(
            split_env("URL=http://x/?a=b").expect("split"),
            ("URL".to_string(), "http://x/?a=b".to_string())
        );
    }

    #[test]
    fn an_empty_value_is_still_a_pair() {
        assert_eq!(split_env("EMPTY=").expect("split"), ("EMPTY".to_string(), String::new()));
    }

    #[test]
    fn a_pair_without_an_equals_sign_is_rejected() {
        assert!(split_env("URL").is_err());
    }

    #[test]
    fn each_state_reads_plainly() {
        assert_eq!(describe(&State::NotStarted), "not started");
        assert_eq!(describe(&State::Running), "running");
        assert!(describe(&State::Exited { code: Some(7), signal: None, window: Some(window()) })
            .starts_with("exited: exit code 7; last window 24x80, 12 bytes"));
        assert!(describe(&State::Exited { code: None, signal: Some(9), window: Some(window()) })
            .contains("killed by signal 9"));
        assert!(describe(&State::Exited { code: None, signal: None, window: Some(window()) })
            .contains("ended for an unknown reason"));
        assert_eq!(
            describe(&State::Exited { code: Some(0), signal: None, window: None }),
            "exited: exit code 0"
        );
    }
}
