# Local verification

## Warning-only shutdown revision, 2026-11-14

The automatic termination of scanned descendant PIDs has been removed. Unix
shutdown inspection now records a snapshot and reports possible survivors to
`ProcessOptions.Stderr`. It does not send SIGTERM or SIGKILL to scanned PIDs,
and diagnostic warnings do not change the return value of Close. The example
already routes Stderr to `os.Stderr`. Existing app-server process-group shutdown
and Windows job-object behavior remain unchanged.

The regression fixture starts a child in its own process group. The SDK-level
check verifies that Close reaps the managed parent, returns nil, emits a warning
containing the child PID, and leaves the child's heartbeat advancing. Repeated
Close does not duplicate the warning. The process-level check also verifies that
inspection leaves a detached child alive. Fixture teardown remains test-owned.

Validation passed on macOS:

```sh
go test -race ./internal/process ./appserver
go build -o /tmp/codex-sdk-minimal-chat ./examples/minimal-chat
go vet ./internal/process ./appserver ./examples/minimal-chat
git diff --check
```

The affected packages also built for Linux amd64 and Windows amd64. These are
cross-compilation checks, not runtime verification on those operating systems.

The historical live runs below exercised the withdrawn termination policy and
must not be treated as evidence that the warning-only policy removes leftovers.
No new live model conversation was run for this revision.

## Historical verification, 2026-11-13

Verified on macOS arm64 with Go 1.27.3 and Codex CLI 0.169.0 on 2026-11-13.
The checkout base was `885a9b1bde579e56a6534c062bbf7381906bedf1`, with the
example and process-cleanup changes present in the working tree.

The runtime used the existing local `custom` model-provider configuration and
`gpt-6-astra`. These results establish the local configured route, not a separate
test of the default OpenAI endpoint.

## Live results

| Case | Result | Exit code | Observed descendants remaining after exit |
| --- | --- | --- | --- |
| Two-turn smoke | Both turns completed; the second returned the random marker supplied only in the first. Both turns emitted text deltas, 16 in total. | 0 | 0 of 20 |
| SIGINT after turn start | Reported `context canceled`; owned process cleanup completed. | 1 | 0 of 14 |
| One-second total timeout | Reported `context deadline exceeded`; owned process cleanup completed. | 1 | 0 of 16 |
| Missing Codex executable | Reported the executable lookup error without starting a child. | 1 | 0 of 0 |

The verification driver recorded descendant PIDs during initialization and turn
startup, then checked their liveness after the command exited. This check covers
those observed PIDs; it does not prove the absence of every possible unobserved
short-lived or already-reparented process.

Machine-readable evidence for this run is stored locally in
`/tmp/codex-sdk-minimal-chat-verification-fixed/summary.json`, with separate JSONL
and stderr files for each case. Temporary files are not durable project artifacts.

## Historical cleanup observation and withdrawn workaround

The initial smoke run passed conversation checks but failed immediate descendant
cleanup: MCP launchers had created separate process groups and outlived the
app-server. The SDK previously stopped only the app-server's process group.
The original failing run is retained under
`/tmp/codex-sdk-minimal-chat-verification/`.

The initial workaround captured descendant identities before closing stdin and
terminated surviving recorded descendants after parent-group shutdown. It was
withdrawn on 2026-11-14 because observed ancestry is insufficient authority to
terminate a process. The original observation established immediate survival,
not a permanent leak. Current behavior is diagnostic only, as documented above.

## Historical verification boundary

The following checks passed:

```sh
go build -o /tmp/codex-sdk-minimal-chat ./examples/minimal-chat
go test ./...
go test -race ./internal/process ./appserver
go vet ./...
git diff --check
```

The process package also cross-compiled for Linux amd64 and Windows amd64.
Cross-compilation is not runtime verification on those operating systems.

The first full-suite attempt inherited machine-wide Git hooks and was stopped.
An isolated run then exposed Bash 3.2 compatibility failures in existing hook
fixtures and UTF-8 path encoding. Fixture scripts now use portable variable
presence and PID expressions; path encoding masks character values to one byte.
Fixture Git environments now disable global and system configuration while
retaining repository-local hook configuration. The previously failing focused
hook tests passed after these corrections.
The subsequent unmodified `go test ./...` command passed all packages; the full
output is retained locally in `/tmp/codex-sdk-minimal-chat-full-tests.log`.

This milestone verifies sequential text conversation, streaming, errors and
application shutdown. It does not verify UI, HTTP transport, reconnect/history
recovery, multi-agent behavior, approvals, tool execution or high concurrency.
The process-cleanup change was exercised on macOS; other platforms require their
own runtime verification.
