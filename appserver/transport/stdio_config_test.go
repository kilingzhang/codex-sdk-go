package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

func TestStdioTransportConfigDefaultsAndResolution(t *testing.T) {
	r, _ := io.Pipe()
	tr := NewStdioTransport(r, io.Discard)
	defer func() { _ = tr.Close() }()

	got := tr.Config()
	want := DefaultStdioTransportConfig()
	if got != want {
		t.Fatalf("Config() = %+v, want %+v", got, want)
	}

	// Custom streaming backlog scales derived turn-scoped and ordered backlog defaults.
	r2, _ := io.Pipe()
	tr2 := NewStdioTransportWithConfig(r2, io.Discard, StdioTransportConfig{
		StreamingNotificationQueueSize:  512,
		MaxStreamingNotificationBacklog: 2048,
		MaxTurnScopedNotificationQueues: 256,
	})
	defer func() { _ = tr2.Close() }()

	cfg2 := tr2.Config()
	if cfg2.MaxTurnScopedNotificationQueueSize != 512+2048 {
		t.Fatalf("MaxTurnScopedNotificationQueueSize = %d, want %d", cfg2.MaxTurnScopedNotificationQueueSize, 512+2048)
	}
	wantOrdered := 256*256 + (512 + 2048)
	if cfg2.MaxOrderedNotificationBacklog != wantOrdered {
		t.Fatalf("MaxOrderedNotificationBacklog = %d, want %d", cfg2.MaxOrderedNotificationBacklog, wantOrdered)
	}

	// Unlimited streaming backlog scales derived caps to Unlimited.
	r3, _ := io.Pipe()
	tr3 := NewStdioTransportWithConfig(r3, io.Discard, StdioTransportConfig{
		MaxStreamingNotificationBacklog: Unlimited,
	})
	defer func() { _ = tr3.Close() }()

	cfg3 := tr3.Config()
	if cfg3.MaxStreamingNotificationBacklog != Unlimited ||
		cfg3.MaxTurnScopedNotificationQueueSize != Unlimited ||
		cfg3.MaxOrderedNotificationBacklog != Unlimited {
		t.Fatalf("unexpected unlimited derived config: %+v", cfg3)
	}
}

func TestStdioTransportConfigCustomFrameSizeAndUnlimited(t *testing.T) {
	// Tight frame limit (64 bytes) rejects a 128-byte notification.
	rSmall, wSmall := io.Pipe()
	trSmall := NewStdioTransportWithConfig(rSmall, io.Discard, StdioTransportConfig{
		MaxInboundMessageSizeBytes: 64,
	})
	defer func() { _ = trSmall.Close() }()

	payload := strings.Repeat("a", 128)
	msg := fmt.Sprintf(`{"jsonrpc":"2.0","method":"item/started","params":{"data":%q}}`+"\n", payload)
	go func() {
		_, _ = io.WriteString(wSmall, msg)
		_ = wSmall.Close()
	}()

	<-trSmall.ReaderStopped()
	if err := trSmall.ScanErr(); !errors.Is(err, errOversizedInboundFrame) {
		t.Fatalf("ScanErr() = %v, want %v", err, errOversizedInboundFrame)
	}

	// Larger frame limit (> default 10 MiB) accepts an 11 MiB message.
	rLarge, wLarge := io.Pipe()
	trLarge := NewStdioTransportWithConfig(rLarge, io.Discard, StdioTransportConfig{
		MaxInboundMessageSizeBytes:   16 * 1024 * 1024,
		MaxRetainedNotificationBytes: 32 * 1024 * 1024,
	})
	defer func() { _ = trLarge.Close() }()

	gotMethod := make(chan string, 1)
	trLarge.OnNotify(func(_ context.Context, notif Notification) {
		gotMethod <- notif.Method
	})

	bigPayload := bytes.Repeat([]byte{'x'}, 11*1024*1024)
	bigLine := append([]byte(`{"jsonrpc":"2.0","method":"custom/large","params":"`), bigPayload...)
	bigLine = append(bigLine, []byte(`"}`+"\n")...)
	go func() {
		_, _ = wLarge.Write(bigLine)
		_ = wLarge.Close()
	}()

	select {
	case m := <-gotMethod:
		if m != "custom/large" {
			t.Fatalf("received method = %q, want %q", m, "custom/large")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for 11 MiB notification")
	}
}

func TestStdioTransportConfigCustomNotificationByteBudgetAndUnlimited(t *testing.T) {
	// Small byte budget (256 bytes) trips on queued notifications.
	rSmall, _ := io.Pipe()
	trSmall := NewStdioTransportWithConfig(rSmall, io.Discard, StdioTransportConfig{
		MaxRetainedNotificationBytes: 256,
	})
	defer func() { _ = trSmall.Close() }()

	// Block the single turn-scoped worker for thread:1 so subsequent notifications stay buffered.
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	trSmall.OnNotify(func(_ context.Context, _ Notification) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	})

	bigParams := []byte(`{"threadId":"t1","chunk":"` + strings.Repeat("a", 150) + `"}`)
	trSmall.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyAgentMessageDelta, Params: bigParams})
	<-entered
	trSmall.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyAgentMessageDelta, Params: bigParams})

	if err := trSmall.ScanErr(); !errors.Is(err, errNotificationByteLimit) {
		close(gate)
		t.Fatalf("ScanErr() = %v, want %v", err, errNotificationByteLimit)
	}
	close(gate)

	// Unlimited byte budget accepts far beyond the small budget.
	rUnlim, _ := io.Pipe()
	trUnlim := NewStdioTransportWithConfig(rUnlim, io.Discard, StdioTransportConfig{
		MaxRetainedNotificationBytes: Unlimited,
	})
	defer func() { _ = trUnlim.Close() }()

	if res, err := trUnlim.notificationBudget.reserve(maxRetainedNotificationBytes + 1024); err != nil {
		t.Fatalf("unlimited notificationBudget.reserve failed: %v", err)
	} else {
		bufferedNotification{reservation: res}.release()
	}
}

func TestStdioTransportConfigCustomTurnScopedQueuesAndWorkers(t *testing.T) {
	// Allow more than default 128 concurrent active thread queues.
	r, _ := io.Pipe()
	tr := NewStdioTransportWithConfig(r, io.Discard, StdioTransportConfig{
		MaxTurnScopedNotificationQueues: 200,
		TurnScopedNotificationWorkers:   16,
	})
	defer func() { _ = tr.Close() }()

	var active, peak atomic.Int32
	allEntered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	tr.OnNotify(func(_ context.Context, _ Notification) {
		cur := active.Add(1)
		for {
			prev := peak.Load()
			if cur <= prev || peak.CompareAndSwap(prev, cur) {
				break
			}
		}
		if cur == 16 {
			once.Do(func() { close(allEntered) })
		}
		<-release
		active.Add(-1)
	})

	for i := range 160 {
		params := []byte(fmt.Sprintf(`{"threadId":"t-%d"}`, i))
		tr.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyTurnStarted, Params: params})
	}

	select {
	case <-allEntered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatalf("timed out waiting for 16 turn-scoped workers; peak=%d, scanErr=%v", peak.Load(), tr.ScanErr())
	}
	close(release)

	if tr.ScanErr() != nil {
		t.Fatalf("ScanErr() = %v, want nil with 160 active scopes and limit 200", tr.ScanErr())
	}
	if got := peak.Load(); got != 16 {
		t.Fatalf("peak concurrent workers = %d, want 16", got)
	}
}

func TestStdioTransportConfigCustomTurnQueueSizeAndRejectionBudgetAndSendTimeout(t *testing.T) {
	// Custom turn-scoped queue size (2) fails on 3rd buffered message.
	rQ, _ := io.Pipe()
	trQ := NewStdioTransportWithConfig(rQ, io.Discard, StdioTransportConfig{
		MaxTurnScopedNotificationQueueSize: 2,
	})
	defer func() { _ = trQ.Close() }()

	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	trQ.OnNotify(func(_ context.Context, _ Notification) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	})

	p := []byte(`{"threadId":"t1"}`)
	trQ.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyTurnStarted, Params: p})
	<-entered
	trQ.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyTurnStarted, Params: p})
	trQ.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyTurnStarted, Params: p})
	trQ.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyTurnStarted, Params: p})
	close(gate)

	if err := trQ.ScanErr(); !errors.Is(err, errTurnScopedNotificationQueueOverflow) {
		t.Fatalf("ScanErr() = %v, want %v", err, errTurnScopedNotificationQueueOverflow)
	}

	// Custom rejection budget.
	rRej, _ := io.Pipe()
	trRej := NewStdioTransportWithConfig(rRej, io.Discard, StdioTransportConfig{
		MaxRetainedRejectionBytes: 32,
	})
	defer func() { _ = trRej.Close() }()

	if _, err := trRej.rejectionBudget.reserve(33); !errors.Is(err, errRejectionByteLimit) {
		t.Fatalf("rejectionBudget.reserve(33) = %v, want %v", err, errRejectionByteLimit)
	}

	// Custom send timeout and Unlimited send timeout.
	rTO, _ := io.Pipe()
	trTO := NewStdioTransportWithConfig(rTO, io.Discard, StdioTransportConfig{
		SendTimeout: 20 * time.Millisecond,
	})
	defer func() { _ = trTO.Close() }()

	_, err := trTO.Send(context.Background(), Request{ID: RequestID{Value: 1}, Method: "ping"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want %v", err, context.DeadlineExceeded)
	}

	rNoTO, _ := io.Pipe()
	trNoTO := NewStdioTransportWithConfig(rNoTO, io.Discard, StdioTransportConfig{
		SendTimeout: time.Duration(Unlimited),
	})
	defer func() { _ = trNoTO.Close() }()

	ctx, cancel := trNoTO.applyDefaultSendTimeout(context.Background())
	defer cancel()
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		t.Fatal("expected no deadline when SendTimeout is Unlimited")
	}
}
