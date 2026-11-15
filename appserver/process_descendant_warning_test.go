//go:build !windows

package appserver_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	sdk "github.com/dominicnunez/codex-sdk-go/appserver"
)

func TestProcessCloseWarnsWithoutStoppingDetachedChild(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "startup-tree")
	build := exec.Command("go", "build", "-o", binary, "./testdata/startup-tree")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build startup fixture: %v\n%s", err, output)
	}
	root := t.TempDir()
	var stderr bytes.Buffer
	process, err := sdk.StartProcess(context.Background(), &sdk.ProcessOptions{
		BinaryPath: binary,
		Env:        append(os.Environ(), "CODEX_TEST_PROCESS_ROOT="+root, "CODEX_TEST_DETACHED_CHILD=1"),
		Stderr:     &stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The fixture cooperatively exits on this file; no scanned PID is killed.
		_ = os.WriteFile(filepath.Join(root, "stop"), []byte("stop"), 0o600)
		_ = process.Close()
	})
	values := strings.Fields(waitForFileContents(t, filepath.Join(root, "pids")))
	if len(values) != 2 {
		t.Fatal("invalid fixture PID record")
	}
	parentPID, err := strconv.Atoi(values[0])
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(values[1])
	if err != nil {
		t.Fatal(err)
	}
	if group, err := syscall.Getpgid(childPID); err != nil || group != childPID || group == parentPID {
		t.Fatalf("expected separate child group: group=%d error=%v", group, err)
	}
	waitForFileContents(t, filepath.Join(root, "heartbeat"))
	if err := process.Close(); err != nil {
		t.Fatalf("diagnostic warning must not fail Close: %v", err)
	}
	if err := syscall.Kill(parentPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("managed parent was not reaped: %v", err)
	}
	warning := stderr.String()
	if !strings.Contains(warning, "codex-sdk-go: warning:") || !strings.Contains(warning, "pids=["+values[1]+"]") {
		t.Fatalf("missing survivor warning: %q", warning)
	}
	before := waitForFileContents(t, filepath.Join(root, "heartbeat"))
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if after := waitForFileContents(t, filepath.Join(root, "heartbeat")); after != before {
			if err := process.Close(); err != nil || stderr.String() != warning {
				t.Fatalf("repeated Close changed warning or result: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("detached child stopped executing after Close")
}
