//go:build !windows

package process

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDetachedProcessHelper(t *testing.T) {
	role := os.Getenv("SDK_DETACHED_TEST_ROLE")
	if role == "" {
		return
	}
	if role == "child" {
		signal.Ignore(syscall.SIGINT, syscall.SIGTERM)
		if err := os.WriteFile(os.Getenv("SDK_DETACHED_TEST_PID"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			os.Exit(2)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	child := exec.Command(os.Args[0], "-test.run=^TestDetachedProcessHelper$")
	child.Env = append(os.Environ(), "SDK_DETACHED_TEST_ROLE=child")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		os.Exit(3)
	}
	_ = child.Wait()
	os.Exit(0)
}

func TestTreeCloseReportsButDoesNotStopChildInSeparateProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	parent := exec.Command(os.Args[0], "-test.run=^TestDetachedProcessHelper$")
	parent.Env = append(os.Environ(), "SDK_DETACHED_TEST_ROLE=parent", "SDK_DETACHED_TEST_PID="+pidFile)
	ConfigureCommand(parent)
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	parentDone := make(chan struct{})
	go func() { _ = parent.Wait(); close(parentDone) }()
	var childPID int
	t.Cleanup(func() {
		_ = parent.Process.Kill()
		<-parentDone
		if childPID != 0 {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(pidFile)
		if err == nil {
			childPID, err = strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("child did not become ready")
	}
	group, err := syscall.Getpgid(childPID)
	if err != nil || group != childPID || group == parent.Process.Pid {
		t.Fatalf("child did not create a separate group: group=%d error=%v", group, err)
	}
	tree, err := AttachTree(parent)
	if err != nil {
		t.Fatal(err)
	}
	if err := tree.PrepareShutdown(parent.Process); err != nil {
		t.Fatal(err)
	}
	// Simulate parent exit after stdin closes, before descendant inspection.
	if err := parent.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-parentDone
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatal("fixture child must survive its parent's process group")
	}
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}
	warning := tree.CheckDescendants()
	if warning == nil || !strings.Contains(warning.Error(), "pids=["+strconv.Itoa(childPID)+"]") {
		t.Fatalf("expected warning naming surviving child %d, got %v", childPID, warning)
	}
	table, err := readProcessTable()
	if err != nil {
		t.Fatal(err)
	}
	if record, exists := table[childPID]; !exists || record.zombie {
		t.Fatal("diagnostic inspection terminated the detached child")
	}
}

func TestMatchingDescendantsExcludesReusedPIDsAndZombies(t *testing.T) {
	records := map[int]processIdentity{
		10: {pid: 10, started: "original"},
		20: {pid: 20, started: "original"},
		30: {pid: 30, started: "original"},
	}
	table := map[int]processIdentity{
		10: {pid: 10, started: "reused"},
		20: {pid: 20, started: "original", zombie: true},
		30: {pid: 30, started: "original"},
		40: {pid: 40, started: "unrelated"},
	}
	got := matchingDescendants(records, table)
	if len(got) != 1 || got[0] != 30 {
		t.Fatalf("unexpected observed survivors: %v", got)
	}
}

func TestParseProcessTableRejectsMalformedIdentity(t *testing.T) {
	if _, err := parseProcessTable("10 invalid S Thu Nov 13 12:00:00 2026"); err == nil {
		t.Fatal("invalid parent PID was accepted")
	}
	table, err := parseProcessTable("10 1 S Thu Nov 13 12:00:00 2026\n20 10 Z Thu Nov 13 12:00:01 2026\n")
	if err != nil || len(table) != 2 || table[20].parent != 10 || !table[20].zombie {
		t.Fatalf("invalid parsed identities: %v, %v", table, err)
	}
}

func TestCheckDescendantsWithoutSnapshot(t *testing.T) {
	if err := (Tree{}).CheckDescendants(); err != nil {
		t.Fatal(err)
	}
}
