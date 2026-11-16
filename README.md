# AgentMaster

Runs an agent's processes and hands control of them over a port: start one,
read its window, type into it, resize the window, read what it put on the
clipboard, pass messages to it and back, stop it, ask how it ended.

A process runs under a terminal of its own, or without one: then it has no
window, and what it prints goes to a file, if one is given. One process may go
without a name; the rest are told apart by the names they were started under.

It is meant to run wherever the process itself runs — next to the supervisor on a
host, or inside a container with the port published — so that everything above it
drives a host process and a container process the same way.

## Parts

- `crates/daemon` — the program (`svora-am`), which owns an agent's processes.
- `crates/client` — the library everything else talks to it through.
- `crates/cli` — `amctl`, for driving one by hand.
- `crates/protocol` — the wire language, known only to the two sides above.

## By hand

```sh
svora-am --listen 127.0.0.1:7620    # or AGENT_MASTER_LISTEN in the environment

export AGENT_MASTER_ADDR=127.0.0.1:7620
amctl start --cwd /workdir --rows 80 --cols 100 --env TERM=xterm-256color -- claude
amctl window
amctl input 'hello'
printf '\r' | amctl input
amctl resize --rows 40 --cols 120
amctl state
amctl clipboard
amctl stop --grace-ms 5000
```

## Without a terminal, by name

```sh
amctl --process app-server start --no-terminal --output /state/app-server.log \
    --message-socket /state/app-server.sock -- codex app-server --listen unix:///state/app-server.sock
amctl --process app-server state
amctl --process app-server stop
```

Stopping a process, with a terminal or without, reaches its whole process group.

## Messages

A process started with `--message-socket` serves WebSocket text messages on that
unix socket. agent-master connects to it the first time a message is sent or
asked for. What the process sends is numbered from 1 — again from 1 each time
the name is started — and held, up to 10 000, until the controller names the
last one it took: that lets go of it and of every one before it. One controller
takes a process's messages; whoever names a number lets go of them for all.

```sh
amctl --process app-server send '{"id":1,"method":"account/read","params":{}}'
amctl --process app-server messages                  # number, tab, message; nothing let go of
amctl --process app-server messages --last-taken 3
```

## Ending

Ending agent-master with SIGTERM or SIGINT stops every process it runs first,
each given `--stop-grace-ms` (`AGENT_MASTER_STOP_GRACE_MS`, 5000 by default) to
end gently before it is killed, and starts nothing more meanwhile.

## Keys

Plain text goes as an argument; anything else travels as the bytes a terminal
sends for it.

```
up      $'\033[A'    Enter      $'\r'
down    $'\033[B'    Tab        $'\t'
right   $'\033[C'    Escape     $'\033'
left    $'\033[D'    Backspace  $'\177'
                     Ctrl-C     $'\003'
```

```sh
amctl input $'\033[B'          # bash and zsh
printf '\033[B' | amctl input  # anywhere
```

An application that has switched its cursor keys to the application mode wants
`$'\033OB'` for down, and so on for the other three.

`amctl window` writes the window as a terminal would paint it. On a terminal it
takes a screen of its own and follows the process until you press `q` — add
`--freeze` to show it as it is instead. Anywhere else it writes the bytes as
they are, once.
