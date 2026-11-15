//go:build !windows

package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type processIdentity struct {
	pid, parent int
	started     string
	zombie      bool
}

type descendantSet struct {
	mu      sync.Mutex
	records map[int]processIdentity
}

// readProcessTable requests identities only, never arguments or environment.
// A fixed executable path avoids resolving a process-control command from PATH.
func readProcessTable() (map[int]processIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/ps", "-axo", "pid=,ppid=,stat=,lstart=")
	cmd.Env = []string{"LC_ALL=C", "PATH=/usr/bin:/bin"}
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("inspect child process identities: %w", err)
	}
	return parseProcessTable(string(data))
}

func parseProcessTable(data string) (map[int]processIdentity, error) {
	records := make(map[int]processIdentity)
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 8 {
			return nil, errors.New("invalid process identity record")
		}
		pid, pidErr := strconv.Atoi(fields[0])
		parent, parentErr := strconv.Atoi(fields[1])
		if pidErr != nil || parentErr != nil || pid <= 0 || parent < 0 {
			return nil, errors.New("invalid process identity PID")
		}
		records[pid] = processIdentity{pid: pid, parent: parent, started: strings.Join(fields[3:], " "), zombie: strings.Contains(fields[2], "Z")}
	}
	return records, nil
}

// PrepareShutdown records descendants before closing stdin can orphan children
// that created their own process groups. This is a shutdown snapshot, not a
// continuous tracker of processes that detached before shutdown began.
func (t Tree) PrepareShutdown(process *os.Process) error {
	if process == nil || t.descendants == nil {
		return nil
	}
	t.descendants.mu.Lock()
	defer t.descendants.mu.Unlock()
	table, err := readProcessTable()
	if err != nil {
		return err
	}
	if t.descendants.records == nil {
		t.descendants.records = make(map[int]processIdentity)
	}
	observed := map[int]bool{process.Pid: true}
	for changed := true; changed; {
		changed = false
		for pid, record := range table {
			if !observed[pid] && observed[record.parent] {
				observed[pid] = true
				t.descendants.records[pid] = record
				changed = true
			}
		}
	}
	return nil
}

func matchingDescendants(records, table map[int]processIdentity) []int {
	var pids []int
	for pid, original := range records {
		if current, ok := table[pid]; ok && !current.zombie && current.started == original.started {
			pids = append(pids, pid)
		}
	}
	sort.Ints(pids)
	return pids
}

// CheckDescendants reports possible survivors from the shutdown snapshot.
// Observed ancestry is diagnostic evidence only and never authorizes a signal.
func (t Tree) CheckDescendants() error {
	if t.descendants == nil {
		return nil
	}
	t.descendants.mu.Lock()
	defer t.descendants.mu.Unlock()
	if len(t.descendants.records) == 0 {
		return nil
	}
	table, err := readProcessTable()
	if err != nil {
		return err
	}
	remaining := matchingDescendants(t.descendants.records, table)
	if len(remaining) == 0 {
		return nil
	}
	return fmt.Errorf("possible descendant processes still running after app-server shutdown: pids=%v; no signals sent to these PIDs by descendant inspection", remaining)
}
