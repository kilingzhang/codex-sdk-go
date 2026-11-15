// Command minimal-chat runs sequential streamed turns in one Codex conversation.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dominicnunez/codex-sdk-go/appserver"
	"github.com/dominicnunez/codex-sdk-go/appserver/protocol"
)

type prompts []string

func (p *prompts) String() string { return strings.Join(*p, "; ") }
func (p *prompts) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("prompt must not be empty")
	}
	*p = append(*p, value)
	return nil
}

// event is the example's JSONL output, not an additional app-server protocol.
type event struct {
	Type     string                      `json:"type"`
	ThreadID string                      `json:"threadId,omitempty"`
	TurnID   string                      `json:"turnId,omitempty"`
	ItemID   string                      `json:"itemId,omitempty"`
	Model    string                      `json:"model,omitempty"`
	Delta    string                      `json:"delta,omitempty"`
	Response string                      `json:"response,omitempty"`
	Status   string                      `json:"status,omitempty"`
	Message  string                      `json:"message,omitempty"`
	Deltas   int                         `json:"deltas,omitempty"`
	Item     *protocol.ThreadItemWrapper `json:"item,omitempty"`
}

func main() {
	var inputs prompts
	binary := flag.String("codex", "codex", "Codex executable name or path")
	model := flag.String("model", "", "Model override; empty uses the Codex configuration")
	cwd := flag.String("cwd", ".", "Working directory for the app-server")
	timeout := flag.Duration("timeout", 3*time.Minute, "Total startup and conversation timeout")
	smoke := flag.Bool("smoke", false, "Verify two streamed turns and cross-turn recall")
	flag.Var(&inputs, "prompt", "Prompt to send; repeat for sequential turns in one thread")
	flag.Parse()

	out := json.NewEncoder(os.Stdout)
	if err := execute(*binary, *model, *cwd, *timeout, *smoke, inputs, out); err != nil {
		_ = out.Encode(event{Type: "error", Message: err.Error()})
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(binary, model, cwd string, timeout time.Duration, smoke bool, inputs prompts, out *json.Encoder) (err error) {
	if flag.NArg() != 0 || timeout <= 0 || (smoke && len(inputs) != 0) || (!smoke && len(inputs) == 0) {
		return errors.New("use -smoke or one or more -prompt flags, with a positive -timeout and no positional arguments")
	}
	var expected string
	if smoke {
		var nonce [12]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return fmt.Errorf("generate recall marker: %w", err)
		}
		expected = "recall-" + hex.EncodeToString(nonce[:])
		inputs = prompts{
			"Remember this marker for the next message: " + expected + ". Reply with exactly ACK. Do not call tools.",
			"What was the marker in my previous message? Reply with only that marker. Do not call tools.",
		}
	}
	binary, err = exec.LookPath(binary)
	if err != nil {
		return fmt.Errorf("find Codex: %w", err)
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return err
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return err
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, timeout)
	defer cancel()

	server, err := appserver.StartProcess(ctx, &appserver.ProcessOptions{
		BinaryPath: binary,
		Dir:        cwd,
		Stderr:     os.Stderr,
		ClientOptions: []protocol.ClientOption{
			protocol.WithRequestTimeout(30 * time.Second),
			protocol.WithHandlerErrorCallback(func(method string, handlerErr error) {
				fmt.Fprintf(os.Stderr, "notification %s: %v\n", method, handlerErr)
			}),
		},
	})
	if err != nil {
		return fmt.Errorf("start app-server: %w", err)
	}
	defer func() {
		if closeErr := server.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close app-server: %w", closeErr))
			return
		}
		err = errors.Join(err, out.Encode(event{Type: "process_closed"}))
	}()
	if _, err := server.Initialize(ctx); err != nil {
		return err
	}
	if err := out.Encode(event{Type: "initialized"}); err != nil {
		return err
	}
	opts := appserver.ConversationOptions{
		Instructions: protocol.Ptr("This is a minimal text-chat example. Answer directly without calling tools or delegating work."),
	}
	if model != "" {
		opts.Model = &model
	}
	conversation, err := server.StartConversation(ctx, opts)
	if err != nil {
		return err
	}
	defer conversation.Close()
	threadID := conversation.ThreadID()
	thread := conversation.Thread()
	if thread.Model != nil {
		model = *thread.Model
	}
	if err := out.Encode(event{Type: "thread_started", ThreadID: threadID, Model: model}); err != nil {
		return err
	}
	for i, prompt := range inputs {
		response, deltas, err := runTurn(ctx, conversation, prompt, out)
		if err != nil {
			return fmt.Errorf("turn %d: %w", i+1, err)
		}
		if smoke {
			want := "ACK"
			if i == 1 {
				want = expected
			}
			if strings.TrimSpace(response) != want || deltas == 0 {
				return fmt.Errorf("turn %d smoke verification failed: response mismatch or no text deltas", i+1)
			}
		}
	}
	if smoke {
		return out.Encode(event{Type: "smoke_passed", ThreadID: threadID, Message: "Two completed streamed turns; second turn recalled the random marker."})
	}
	return nil
}

func runTurn(ctx context.Context, conversation *appserver.Conversation, prompt string, out *json.Encoder) (string, int, error) {
	stream := conversation.TurnStreamed(ctx, appserver.TurnOptions{Prompt: prompt})
	threadID := conversation.ThreadID()
	var turnID string
	deltas := 0
	for incoming, err := range stream.Events() {
		if err != nil {
			return "", deltas, err
		}
		e := event{ThreadID: threadID, TurnID: turnID}
		switch value := incoming.(type) {
		case *appserver.TurnStarted:
			turnID = value.Turn.ID
			e.Type, e.TurnID, e.Status = "turn_started", turnID, string(value.Turn.Status)
		case *appserver.TextDelta:
			deltas++
			e.Type, e.ItemID, e.Delta = "text_delta", value.ItemID, value.Delta
		case *appserver.ItemStarted:
			e.Type, e.Item = "item_started", &value.Item
		case *appserver.ItemCompleted:
			e.Type, e.Item = "item_completed", &value.Item
		case *appserver.TurnCompleted:
			e.Type, e.TurnID, e.Status = "turn_completed", value.Turn.ID, string(value.Turn.Status)
		default:
			continue
		}
		if err := out.Encode(e); err != nil {
			return "", deltas, err
		}
	}
	result := stream.Result()
	if result == nil {
		return "", deltas, errors.New("stream ended without a result")
	}
	if result.Turn.Status != protocol.TurnStatusCompleted {
		return "", deltas, fmt.Errorf("turn ended with status %s", result.Turn.Status)
	}
	err := out.Encode(event{Type: "result", ThreadID: threadID, TurnID: result.Turn.ID, Response: result.Response, Deltas: deltas})
	return result.Response, deltas, err
}
