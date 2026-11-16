use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_master_client::{AgentMaster, ProcessSpec, Refusal, State, WindowSize};

mod harness;
use harness::*;

#[tokio::test]
async fn a_process_without_a_terminal_ends_with_its_code_and_no_window() {
    let mut master = master().await;
    master.process("job").start(without_a_terminal("exit 3")).await.expect("start");
    assert_eq!(
        ended_as(&mut master, "job").await,
        State::Exited { code: Some(3), signal: None, window: None }
    );
}

#[tokio::test]
async fn what_a_process_without_a_terminal_prints_lands_in_its_output() {
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let output = dir.path().join("output");
    std::fs::write(&output, "before\n").expect("an earlier output");
    let mut spec = without_a_terminal("echo out; echo err >&2");
    spec.output = Some(output.clone());
    master.process("job").start(spec).await.expect("start");
    ended_as(&mut master, "job").await;
    assert_eq!(std::fs::read_to_string(&output).expect("output"), "before\nout\nerr\n");
}

#[tokio::test]
async fn a_process_without_a_terminal_or_an_output_still_runs_to_its_end() {
    let mut master = master().await;
    master.process("job").start(without_a_terminal("echo dropped; exit 0")).await.expect("start");
    assert!(matches!(ended_as(&mut master, "job").await, State::Exited { code: Some(0), .. }));
}

#[tokio::test]
async fn an_output_that_cannot_be_opened_fails_the_start() {
    let mut master = master().await;
    let mut spec = without_a_terminal("true");
    spec.output = Some("/nonexistent/directory/output".into());
    let refused = refusal(master.process("job").start(spec).await.expect_err("start"));
    assert!(matches!(&refused, Refusal::Failed { message } if message.contains("/nonexistent")));
    assert_eq!(master.process("job").state().await.expect("state"), State::NotStarted);
}

#[tokio::test]
async fn a_process_under_a_terminal_cannot_be_given_an_output_too() {
    let mut master = master().await;
    let mut spec = shell("true");
    spec.output = Some(std::env::temp_dir().join("never-written"));
    let refused = refusal(master.start(spec).await.expect_err("start"));
    assert!(matches!(&refused, Refusal::Failed { message } if message.contains("never-written")));
    assert_eq!(master.state().await.expect("state"), State::NotStarted);
}

#[tokio::test]
async fn the_directory_and_environment_reach_a_process_without_a_terminal() {
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let mut spec = without_a_terminal("echo \"$PWD $AGENT_MASTER_TEST\"");
    spec.cwd = dir.path().to_path_buf();
    spec.env = BTreeMap::from([("AGENT_MASTER_TEST".to_string(), "value".to_string())]);
    spec.output = Some(dir.path().join("output"));
    master.process("job").start(spec).await.expect("start");
    let printed = written(&dir.path().join("output")).await;
    let cwd = dir.path().canonicalize().expect("canonical");
    assert_eq!(printed.trim_end(), format!("{} value", cwd.display()));
}

#[tokio::test]
async fn a_process_without_a_terminal_has_no_window_to_read_resize_or_type_into() {
    let mut master = master().await;
    master.process("job").start(without_a_terminal("sleep 30")).await.expect("start");
    let size = WindowSize { rows: 10, cols: 40 };
    let job = || "job";
    assert_eq!(
        refusal(master.process(job()).window().await.expect_err("window")),
        Refusal::NoTerminal
    );
    assert_eq!(
        refusal(master.process(job()).input(b"x").await.expect_err("input")),
        Refusal::NoTerminal
    );
    assert_eq!(
        refusal(master.process(job()).resize(size).await.expect_err("resize")),
        Refusal::NoTerminal
    );
    let refused = master.process(job()).clipboard().await.expect_err("clipboard");
    assert_eq!(refusal(refused), Refusal::NoTerminal);
    master.process(job()).stop(Duration::from_secs(1)).await.expect("stop");
    let refused = master.process(job()).input(b"x").await.expect_err("input after the end");
    assert_eq!(refusal(refused), Refusal::NoTerminal);
    let refused = master.process(job()).resize(size).await.expect_err("resize after the end");
    assert_eq!(refusal(refused), Refusal::NoTerminal);
}

async fn standard_streams_of(
    master: &mut AgentMaster,
    spec: ProcessSpec,
    pid: &Path,
) -> [PathBuf; 3] {
    master.process("job").start(spec).await.expect("start");
    let pid: i32 = written(pid).await.trim().parse().expect("a pid");
    let streams =
        [0, 1, 2].map(|fd| std::fs::read_link(format!("/proc/{pid}/fd/{fd}")).expect("fd"));
    master.process("job").stop(Duration::from_secs(1)).await.expect("stop");
    streams
}

#[tokio::test]
async fn a_process_without_a_terminal_reads_nothing_and_prints_nowhere_unless_given_an_output() {
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let pid_file = dir.path().join("pid");
    let script = format!("echo $$ > {}; exec sleep 30", pid_file.display());
    let null = PathBuf::from("/dev/null");

    let streams = standard_streams_of(&mut master, without_a_terminal(&script), &pid_file).await;
    assert_eq!(streams, [null.clone(), null.clone(), null.clone()]);

    std::fs::remove_file(&pid_file).expect("clear the pid");
    let output = dir.path().join("output");
    let mut spec = without_a_terminal(&script);
    spec.output = Some(output.clone());
    let streams = standard_streams_of(&mut master, spec, &pid_file).await;
    assert_eq!(streams, [null, output.clone(), output]);
}

#[tokio::test]
async fn a_command_that_cannot_be_run_without_a_terminal_fails_the_start() {
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let mut spec = without_a_terminal("true");
    spec.command = "/nonexistent/command".to_string();
    spec.args = Vec::new();
    spec.output = Some(dir.path().join("output"));
    let refused = refusal(master.process("job").start(spec).await.expect_err("start"));
    assert!(
        matches!(&refused, Refusal::Failed { message } if message.contains("spawning /nonexistent/command")),
        "{refused:?}"
    );
    assert_eq!(master.process("job").state().await.expect("state"), State::NotStarted);
    assert!(dir.path().join("output").exists(), "the output is opened before the command is run");
}

#[tokio::test]
async fn stopping_a_process_without_a_terminal_ends_what_it_started_too() {
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let pid_file = dir.path().join("pid");
    let script = format!("sleep 30 & echo $! > {}; wait", pid_file.display());
    master.process("job").start(without_a_terminal(&script)).await.expect("start");
    let started: i32 = written(&pid_file).await.trim().parse().expect("a pid");
    let stopped = master.process("job").stop(Duration::from_secs(5)).await.expect("stop");
    assert!(matches!(stopped, State::Exited { signal: Some(libc::SIGTERM), .. }), "{stopped:?}");
    assert!(gone(started).await, "what the process started outlived it");
}

#[tokio::test]
async fn a_process_without_a_terminal_that_ignores_the_gentle_stop_is_killed() {
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let ready = dir.path().join("ready");
    let script = format!("trap '' TERM; echo up > {}; while :; do sleep 1; done", ready.display());
    master.process("job").start(without_a_terminal(&script)).await.expect("start");
    written(&ready).await;
    let stopped = master.process("job").stop(Duration::from_millis(200)).await.expect("stop");
    assert!(matches!(stopped, State::Exited { signal: Some(libc::SIGKILL), .. }), "{stopped:?}");
}

#[tokio::test]
async fn processes_under_different_names_run_and_stop_apart() {
    let mut master = master().await;
    master.process("one").start(without_a_terminal("sleep 30")).await.expect("start one");
    master.process("two").start(without_a_terminal("sleep 30")).await.expect("start two");
    master.start(shell("sleep 30")).await.expect("start the unnamed one");

    master.process("one").stop(Duration::from_secs(1)).await.expect("stop one");
    assert!(matches!(master.process("one").state().await.expect("state"), State::Exited { .. }));
    assert_eq!(master.process("two").state().await.expect("state"), State::Running);
    assert_eq!(master.state().await.expect("state"), State::Running);

    master.process("two").stop(Duration::from_secs(1)).await.expect("stop two");
    master.stop(Duration::from_secs(1)).await.expect("stop the unnamed one");
}

#[tokio::test]
async fn a_name_holds_one_running_process_at_a_time() {
    let mut master = master().await;
    master.process("job").start(without_a_terminal("sleep 30")).await.expect("start");
    let refused = master.process("job").start(without_a_terminal("sleep 30")).await;
    assert_eq!(refusal(refused.expect_err("a second start")), Refusal::AlreadyRunning);
    master.process("job").stop(Duration::from_secs(1)).await.expect("stop");
    master.process("job").start(without_a_terminal("exit 4")).await.expect("start again");
    assert!(matches!(ended_as(&mut master, "job").await, State::Exited { code: Some(4), .. }));
}

#[tokio::test]
async fn a_name_nothing_was_started_under_is_refused_and_reported_as_not_started() {
    let mut master = master().await;
    master.process("job").start(without_a_terminal("sleep 30")).await.expect("start");
    let ghost = || "ghost";
    assert_eq!(master.process(ghost()).state().await.expect("state"), State::NotStarted);
    let refused = master.process(ghost()).stop(Duration::from_secs(1)).await.expect_err("stop");
    assert_eq!(refusal(refused), Refusal::NotStarted);
    let refused = master.process(ghost()).window().await.expect_err("window");
    assert_eq!(refusal(refused), Refusal::NotStarted);
    let refused = master.process(ghost()).send("x".to_string()).await.expect_err("send");
    assert_eq!(refusal(refused), Refusal::NotStarted);
    let refused = master.process(ghost()).messages(0).await.expect_err("messages");
    assert_eq!(refusal(refused), Refusal::NotStarted);
    assert_eq!(master.state().await.expect("state"), State::NotStarted);
    master.process("job").stop(Duration::from_secs(1)).await.expect("stop");
}

#[tokio::test]
async fn a_named_process_under_a_terminal_has_a_window_of_its_own() {
    let mut master = master().await;
    master.process("screen").start(shell("echo named; sleep 30")).await.expect("start named");
    master.start(shell("echo unnamed; sleep 30")).await.expect("start unnamed");
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        let named = text(&master.process("screen").window().await.expect("window"));
        if named.contains("named") {
            assert!(!named.contains("unnamed"), "{named}");
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the named window never painted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    window_showing(&mut master, "unnamed").await;
    master.process("screen").stop(Duration::from_secs(1)).await.expect("stop named");
    master.stop(Duration::from_secs(1)).await.expect("stop unnamed");
}

#[tokio::test]
async fn closing_takes_every_process_down_at_once_and_starts_nothing_after() {
    let processes = agent_master::Processes::new();
    let dir = tempfile::tempdir().expect("a directory");
    let names = ["one", "two", "three"];
    for name in names {
        let ready = dir.path().join(name);
        let script =
            format!("trap '' TERM; echo up > {}; while :; do sleep 1; done", ready.display());
        processes.start(Some(name), without_a_terminal(&script)).await.expect("start");
        written(&ready).await;
    }
    processes.start(None, shell("sleep 30")).await.expect("start under a terminal");

    let closing = std::time::Instant::now();
    processes.close(Duration::from_secs(1)).await;
    assert!(closing.elapsed() < Duration::from_secs(2), "they were taken down one by one");

    for name in names {
        let state = processes.state(Some(name)).await;
        assert!(
            matches!(state, State::Exited { signal: Some(libc::SIGKILL), .. }),
            "{name}: {state:?}"
        );
    }
    assert!(matches!(processes.state(None).await, State::Exited { .. }));
    let refused = processes.start(Some("late"), without_a_terminal("sleep 30")).await;
    assert!(matches!(refused, Err(Refusal::Failed { message }) if message.contains("ending")));
    assert_eq!(processes.state(Some("late")).await, State::NotStarted);
}

#[tokio::test]
async fn a_process_without_a_terminal_inherits_agent_masters_environment_under_what_it_is_given() {
    std::env::set_var("AGENT_MASTER_DETACHED_INHERITED", "from-the-daemon");
    std::env::set_var("AGENT_MASTER_DETACHED_OVERRIDDEN", "from-the-daemon");
    let mut master = master().await;
    let dir = tempfile::tempdir().expect("a directory");
    let mut spec = without_a_terminal(
        "echo \"$AGENT_MASTER_DETACHED_INHERITED $AGENT_MASTER_DETACHED_OVERRIDDEN\"",
    );
    spec.env = BTreeMap::from([(
        "AGENT_MASTER_DETACHED_OVERRIDDEN".to_string(),
        "from-the-spec".to_string(),
    )]);
    spec.output = Some(dir.path().join("output"));
    master.process("job").start(spec).await.expect("start");
    let printed = written(&dir.path().join("output")).await;
    assert_eq!(printed.trim_end(), "from-the-daemon from-the-spec");
}
