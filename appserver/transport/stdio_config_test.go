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
	if got.MaxStreamingNotificationBacklog != Unlimited ||
		got.MaxTurnScopedNotificationQueues != Unlimited ||
		got.MaxTurnScopedNotificationQueueSize != Unlimited ||
		got.MaxOrderedNotificationBacklog != Unlimited ||
		got.MaxInboundMessageSizeBytes != Unlimited ||
		got.MaxRetainedNotificationBytes != Unlimited ||
		got.MaxRetainedRejectionBytes != Unlimited ||
		got.SendTimeout != time.Duration(Unlimited) {
		t.Fatalf("expected all default limits to be Unlimited, got %+v", got)
	}

	// Zero-value StdioTransportConfig resolves identically to DefaultStdioTransportConfig.
	rZero, _ := io.Pipe()
	trZero := NewStdioTransportWithConfig(rZero, io.Discard, StdioTransportConfig{})
	defer func() { _ = trZero.Close() }()
	if trZero.Config() != want {
		t.Fatalf("zero config resolved to %+v, want %+v", trZero.Config(), want)
	}

	// BoundedStdioTransportConfig retains the conservative upper bounds.
	bounded := BoundedStdioTransportConfig()
	if bounded.MaxStreamingNotificationBacklog != maxStreamingNotificationBacklog ||
		bounded.MaxTurnScopedNotificationQueues != maxTurnScopedNotificationQueues ||
		bounded.MaxTurnScopedNotificationQueueSize != maxTurnScopedNotificationQueueSize ||
		bounded.MaxOrderedNotificationBacklog != maxOrderedNotificationBacklog ||
		bounded.MaxInboundMessageSizeBytes != maxInboundMessageSizeBytes ||
		bounded.MaxRetainedNotificationBytes != maxRetainedNotificationBytes ||
		bounded.MaxRetainedRejectionBytes != maxRetainedRejectionBytes ||
		bounded.SendTimeout != defaultSendTimeout {
		t.Fatalf("unexpected BoundedStdioTransportConfig: %+v", bounded)
	}

	// Partial custom config keeps unspecified limit fields Unlimited.
	rPartial, _ := io.Pipe()
	trPartial := NewStdioTransportWithConfig(rPartial, io.Discard, StdioTransportConfig{
		StreamingNotificationQueueSize:  512,
		MaxStreamingNotificationBacklog: 2048,
		MaxTurnScopedNotificationQueues: 256,
	})
	defer func() { _ = trPartial.Close() }()

	cfgPartial := trPartial.Config()
	if cfgPartial.StreamingNotificationQueueSize != 512 ||
		cfgPartial.MaxStreamingNotificationBacklog != 2048 ||
		cfgPartial.MaxTurnScopedNotificationQueues != 256 ||
		cfgPartial.MaxTurnScopedNotificationQueueSize != Unlimited ||
		cfgPartial.MaxOrderedNotificationBacklog != Unlimited ||
		cfgPartial.MaxInboundMessageSizeBytes != Unlimited ||
		cfgPartial.MaxRetainedNotificationBytes != Unlimited ||
		cfgPartial.MaxRetainedRejectionBytes != Unlimited ||
		cfgPartial.SendTimeout != time.Duration(Unlimited) {
		t.Fatalf("unexpected partial config resolution: %+v", cfgPartial)
	}
}

func TestStdioTransportDefaultUnlimitedBehavior(t *testing.T) {
	// Default transport accepts frames > 10 MiB.
	rLarge, wLarge := io.Pipe()
	trLarge := NewStdioTransport(rLarge, io.Discard)
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
		t.Fatal("timed out waiting for 11 MiB notification on default transport")
	}

	// Default transport imposes no 64 MiB notification/rejection budgets or 5m SendTimeout.
	rBudget, _ := io.Pipe()
	trBudget := NewStdioTransport(rBudget, io.Discard)
	defer func() { _ = trBudget.Close() }()

	if res, err := trBudget.notificationBudget.reserve(maxRetainedNotificationBytes + 1024); err != nil {
		t.Fatalf("default notificationBudget.reserve failed: %v", err)
	} else {
		bufferedNotification{reservation: res}.release()
	}

	if res, err := trBudget.rejectionBudget.reserve(maxRetainedRejectionBytes + 1024); err != nil {
		t.Fatalf("default rejectionBudget.reserve failed: %v", err)
	} else {
		res.release()
	}

	ctx, cancel := trBudget.applyDefaultSendTimeout(context.Background())
	defer cancel()
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		t.Fatal("expected no deadline on default transport when caller ctx has no deadline")
	}

	// Default transport allows > 128 active thread scopes and > 1280 events per scope.
	rScopes, _ := io.Pipe()
	trScopes := NewStdioTransport(rScopes, io.Discard)
	defer func() { _ = trScopes.Close() }()

	gate := make(chan struct{})
	trScopes.OnNotify(func(_ context.Context, _ Notification) {
		<-gate
	})

	for i := range 200 {
		params := []byte(fmt.Sprintf(`{"threadId":"scope-%d"}`, i))
		trScopes.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyTurnStarted, Params: params})
	}
	for range 1500 {
		trScopes.enqueueNotification(Notification{JSONRPC: "2.0", Method: protocol.NotifyAgentMessageDelta, Params: []byte(`{"threadId":"scope-0","delta":"x"}`)})
	}
	close(gate)

	if err := trScopes.ScanErr(); err != nil {
		t.Fatalf("ScanErr() = %v, want nil with >128 scopes and >1280 per-scope events on default transport", err)
	}
}

func TestStdioTransportConfigCustomFrameSizeAndBounded(t *testing.T) {
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

	// BoundedStdioTransportConfig rejects > 10 MiB frame.
	rBounded, wBounded := io.Pipe()
	trBounded := NewStdioTransportWithConfig(rBounded, io.Discard, BoundedStdioTransportConfig())
	defer func() { _ = trBounded.Close() }()

	bigPayload := bytes.Repeat([]byte{'x'}, 11*1024*1024)
	bigLine := append([]byte(`{"jsonrpc":"2.0","method":"custom/large","params":"`), bigPayload...)
	bigLine = append(bigLine, []byte(`"}`+"\n")...)
	go func() {
		_, _ = wBounded.Write(bigLine)
		_ = wBounded.Close()
	}()

	<-trBounded.ReaderStopped()
	if err := trBounded.ScanErr(); !errors.Is(err, errOversizedInboundFrame) {
		t.Fatalf("Bounded ScanErr() = %v, want %v", err, errOversizedInboundFrame)
	}

	// Larger custom frame limit (16 MiB) accepts an 11 MiB message.
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

func TestStdioTransportConfigCustomNotificationByteBudget(t *testing.T) {
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

	// BoundedStdioTransportConfig enforces the 64 MiB notification budget.
	rBounded, _ := io.Pipe()
	trBounded := NewStdioTransportWithConfig(rBounded, io.Discard, BoundedStdioTransportConfig())
	defer func() { _ = trBounded.Close() }()

	if _, err := trBounded.notificationBudget.reserve(maxRetainedNotificationBytes + 1); !errors.Is(err, errNotificationByteLimit) {
		t.Fatalf("bounded notificationBudget.reserve error = %v, want %v", err, errNotificationByteLimit)
	}
}

func TestStdioTransportConfigCustomTurnScopedQueuesAndWorkers(t *testing.T) {
	// Configure bounded scope count and custom worker pool size.
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

	// Custom send timeout.
	rTO, _ := io.Pipe()
	trTO := NewStdioTransportWithConfig(rTO, io.Discard, StdioTransportConfig{
		SendTimeout: 20 * time.Millisecond,
	})
	defer func() { _ = trTO.Close() }()

	_, err := trTO.Send(context.Background(), Request{ID: RequestID{Value: 1}, Method: "ping"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want %v", err, context.DeadlineExceeded)
	}

	// BoundedStdioTransportConfig applies the 5m send timeout when caller ctx has no deadline.
	rBounded, _ := io.Pipe()
	trBounded := NewStdioTransportWithConfig(rBounded, io.Discard, BoundedStdioTransportConfig())
	defer func() { _ = trBounded.Close() }()

	ctx, cancel := trBounded.applyDefaultSendTimeout(context.Background())
	defer cancel()
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		t.Fatal("expected deadline when using BoundedStdioTransportConfig")
	}
}
