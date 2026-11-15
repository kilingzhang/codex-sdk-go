# Minimal streaming chat backend

This command demonstrates the first integration milestone: start a Codex
app-server, initialize it, create one conversation, and run sequential streamed
turns in that conversation. It uses only the standard library and this checkout's
SDK. It does not start an HTTP server.

See [local verification](VERIFICATION.md) for the tested runtime and observed results.

## Requirements

- The Go version declared in the repository's `go.mod`.
- A Codex CLI executable on `PATH`, or an explicit `-codex` path.
- Working Codex authentication and model configuration. Check `codex login status`.

The example inherits the SDK's minimal child environment allowlist. It uses the
default Codex home under the user's home directory; a parent `CODEX_HOME` or
custom provider environment variable is not forwarded automatically. It never
reads or prints credential files. Child diagnostic output is written to stderr.

## Verify the complete path

Run from the repository root:

```sh
go run ./examples/minimal-chat -smoke
```

This sends two real model requests in the same thread. The first supplies a
random marker and requests `ACK`. The second asks for the marker without
repeating it. Verification requires both turns to finish with `completed`, both
to produce text deltas, and both answers to match the expected values.

A successful run emits `smoke_passed`, then `process_closed`, and exits with
status zero. Always check the exit status: shutdown can fail after the model
responses have passed verification.

## Send your own prompts

```sh
go run ./examples/minimal-chat \
  -prompt 'Remember that the project name is Cedar. Reply briefly.' \
  -prompt 'What project name did I give you?'
```

Optional flags:

```sh
go run ./examples/minimal-chat \
  -codex /opt/homebrew/bin/codex \
  -cwd /Users/dev/codex-sdk-go \
  -timeout 3m \
  -prompt 'Say hello in one sentence.'
```

`-model` overrides the configured model. Omitting it preserves the existing
configuration. `-timeout` limits startup and all turns together. `-smoke` and
`-prompt` are mutually exclusive.

## Output contract

Stdout contains one JSON object per line. This is a small application envelope,
not a replacement for the official app-server protocol.

| Event | Meaning |
| --- | --- |
| `initialized` | The app-server handshake completed. |
| `thread_started` | The conversation is ready, with its thread ID and model when available. |
| `turn_started` | A turn started, with its thread and turn IDs. |
| `text_delta` | Append `delta` to the message identified by thread, turn, and item IDs. |
| `item_started`, `item_completed` | A typed protocol item started or completed. |
| `turn_completed` | The server reported a terminal turn status. Inspect `status`. |
| `result` | A successfully completed turn, with the last assistant message and delta count. |
| `smoke_passed` | Both smoke-test answers and streaming checks passed. |
| `process_closed` | Closing the owned app-server completed successfully. |
| `error` | Startup, turn, verification, output, or shutdown failed. Exit status is nonzero. |

Treat completed items as authoritative updates to streamed messages rather than
adding a second message. The `result` event is a convenience summary and should
not create another assistant bubble.

## Lifecycle and current scope

- One process and one conversation per invocation; repeated prompts run sequentially.
- The conversation uses the SDK's persistent-thread helper. Each new invocation
  creates a new thread; this example does not implement history browsing or resume.
- Text-only developer instructions ask the model not to use tools. These instructions
  do not change the configured sandbox or enforce a tool prohibition.
- Approval and dynamic-tool handlers are not registered. The SDK returns its
  unsupported-method response for unhandled server requests.
- Ctrl+C, SIGTERM, and timeout cancel the operation and close the owned process.
  This is application shutdown, not an implementation of a reusable UI stop button
  using `turn/interrupt`.
- On Unix, possible surviving descendants are logged as warnings to stderr with
  their PIDs. The SDK does not signal scanned PIDs. These warnings do not fail
  `Close()` or change the exit status; `process_closed` does not assert that all
  detached MCP processes exited. Inspection failures are warnings as well.
- Startup failures and incomplete, failed, or interrupted turns return nonzero.
- Full protocol event coverage, HTTP/WebSocket delivery, UI, and reconnect recovery
  belong to later milestones.
