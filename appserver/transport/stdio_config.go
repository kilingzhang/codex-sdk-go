package transport

import (
	"math"
	"time"
)

// Unlimited disables a supported numeric limit or default timeout when assigned
// to a limit field in StdioTransportConfig.
const Unlimited = -1

// StdioTransportConfig configures queue capacities, worker counts, memory
// budgets, frame limits, and default send timeout for a StdioTransport.
//
// Zero values fall back to the conservative built-in defaults returned by
// DefaultStdioTransportConfig. Negative values disable supported cap fields
// (see Unlimited), except worker counts, channel queue sizes, and read buffer
// size, which require positive sizes and fall back to defaults when <= 0.
type StdioTransportConfig struct {
	// Worker pool sizes (<= 0 uses default).
	InboundRequestWorkers         int
	InboundNotificationWorkers    int
	StreamingNotificationWorkers  int
	ProtectedNotificationWorkers  int
	CriticalNotificationWorkers   int
	TurnScopedNotificationWorkers int

	// Bounded channel capacities (<= 0 uses default).
	InboundRequestQueueSize        int
	InboundNotificationQueueSize   int
	StreamingNotificationQueueSize int
	ProtectedNotificationQueueSize int
	CriticalNotificationQueueSize  int
	OutboundWriteQueueSize         int

	// Backlog and scope limits (0 uses default, < 0 disables cap).
	// When MaxTurnScopedNotificationQueueSize is 0, it defaults to
	// StreamingNotificationQueueSize + MaxStreamingNotificationBacklog (or
	// Unlimited if MaxStreamingNotificationBacklog < 0).
	// When MaxOrderedNotificationBacklog is 0, it defaults to
	// MaxTurnScopedNotificationQueues*256 + MaxTurnScopedNotificationQueueSize
	// (or Unlimited if either component is < 0).
	MaxStreamingNotificationBacklog    int
	MaxTurnScopedNotificationQueues    int
	MaxTurnScopedNotificationQueueSize int
	MaxOrderedNotificationBacklog      int

	// Framing and retained memory limits (0 uses default, < 0 disables cap for
	// MaxInboundMessageSizeBytes, MaxRetainedNotificationBytes, and
	// MaxRetainedRejectionBytes; ReadBufferSizeBytes <= 0 uses default).
	ReadBufferSizeBytes          int
	MaxInboundMessageSizeBytes   int
	MaxRetainedNotificationBytes int
	MaxRetainedRejectionBytes    int

	// SendTimeout sets the default timeout for outbound requests and
	// notifications when the caller context has no deadline.
	// 0 uses the default (5m); < 0 disables the default send timeout.
	SendTimeout time.Duration
}

// DefaultStdioTransportConfig returns a StdioTransportConfig populated with the
// default conservative limits used by NewStdioTransport.
func DefaultStdioTransportConfig() StdioTransportConfig {
	return StdioTransportConfig{
		InboundRequestWorkers:              inboundRequestWorkers,
		InboundNotificationWorkers:         inboundNotificationWorkers,
		StreamingNotificationWorkers:       streamingNotificationWorkers,
		ProtectedNotificationWorkers:       protectedNotificationWorkers,
		CriticalNotificationWorkers:        criticalNotificationWorkers,
		TurnScopedNotificationWorkers:      turnScopedNotificationWorkers,
		InboundRequestQueueSize:            inboundRequestQueueSize,
		InboundNotificationQueueSize:       inboundNotifQueueSize,
		StreamingNotificationQueueSize:     streamingNotifQueueSize,
		ProtectedNotificationQueueSize:     protectedNotifQueueSize,
		CriticalNotificationQueueSize:      criticalNotifQueueSize,
		OutboundWriteQueueSize:             outboundWriteQueueSize,
		MaxStreamingNotificationBacklog:    maxStreamingNotificationBacklog,
		MaxTurnScopedNotificationQueues:    maxTurnScopedNotificationQueues,
		MaxTurnScopedNotificationQueueSize: maxTurnScopedNotificationQueueSize,
		MaxOrderedNotificationBacklog:      maxOrderedNotificationBacklog,
		ReadBufferSizeBytes:                readBufferSizeBytes,
		MaxInboundMessageSizeBytes:         maxInboundMessageSizeBytes,
		MaxRetainedNotificationBytes:       maxRetainedNotificationBytes,
		MaxRetainedRejectionBytes:          maxRetainedRejectionBytes,
		SendTimeout:                        defaultSendTimeout,
	}
}

func resolveStdioTransportConfig(cfg StdioTransportConfig) StdioTransportConfig {
	streamingQueue := resolvePositiveInt(cfg.StreamingNotificationQueueSize, streamingNotifQueueSize)
	streamingBacklog := resolveCapInt(cfg.MaxStreamingNotificationBacklog, maxStreamingNotificationBacklog)
	maxTurnQueues := resolveCapInt(cfg.MaxTurnScopedNotificationQueues, maxTurnScopedNotificationQueues)
	turnQueueSize := resolveTurnScopedQueueSize(cfg.MaxTurnScopedNotificationQueueSize, streamingQueue, streamingBacklog)
	orderedBacklog := resolveOrderedBacklog(cfg.MaxOrderedNotificationBacklog, maxTurnQueues, turnQueueSize)

	sendTimeout := cfg.SendTimeout
	if sendTimeout == 0 {
		sendTimeout = defaultSendTimeout
	} else if sendTimeout < 0 {
		sendTimeout = time.Duration(Unlimited)
	}

	return StdioTransportConfig{
		InboundRequestWorkers:              resolvePositiveInt(cfg.InboundRequestWorkers, inboundRequestWorkers),
		InboundNotificationWorkers:         resolvePositiveInt(cfg.InboundNotificationWorkers, inboundNotificationWorkers),
		StreamingNotificationWorkers:       resolvePositiveInt(cfg.StreamingNotificationWorkers, streamingNotificationWorkers),
		ProtectedNotificationWorkers:       resolvePositiveInt(cfg.ProtectedNotificationWorkers, protectedNotificationWorkers),
		CriticalNotificationWorkers:        resolvePositiveInt(cfg.CriticalNotificationWorkers, criticalNotificationWorkers),
		TurnScopedNotificationWorkers:      resolvePositiveInt(cfg.TurnScopedNotificationWorkers, turnScopedNotificationWorkers),
		InboundRequestQueueSize:            resolvePositiveInt(cfg.InboundRequestQueueSize, inboundRequestQueueSize),
		InboundNotificationQueueSize:       resolvePositiveInt(cfg.InboundNotificationQueueSize, inboundNotifQueueSize),
		StreamingNotificationQueueSize:     streamingQueue,
		ProtectedNotificationQueueSize:     resolvePositiveInt(cfg.ProtectedNotificationQueueSize, protectedNotifQueueSize),
		CriticalNotificationQueueSize:      resolvePositiveInt(cfg.CriticalNotificationQueueSize, criticalNotifQueueSize),
		OutboundWriteQueueSize:             resolvePositiveInt(cfg.OutboundWriteQueueSize, outboundWriteQueueSize),
		MaxStreamingNotificationBacklog:    streamingBacklog,
		MaxTurnScopedNotificationQueues:    maxTurnQueues,
		MaxTurnScopedNotificationQueueSize: turnQueueSize,
		MaxOrderedNotificationBacklog:      orderedBacklog,
		ReadBufferSizeBytes:                resolvePositiveInt(cfg.ReadBufferSizeBytes, readBufferSizeBytes),
		MaxInboundMessageSizeBytes:         resolveCapInt(cfg.MaxInboundMessageSizeBytes, maxInboundMessageSizeBytes),
		MaxRetainedNotificationBytes:       resolveCapInt(cfg.MaxRetainedNotificationBytes, maxRetainedNotificationBytes),
		MaxRetainedRejectionBytes:          resolveCapInt(cfg.MaxRetainedRejectionBytes, maxRetainedRejectionBytes),
		SendTimeout:                        sendTimeout,
	}
}

func resolvePositiveInt(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func resolveCapInt(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	if value < 0 {
		return Unlimited
	}
	return value
}

func resolveTurnScopedQueueSize(configured, streamingQueue, streamingBacklog int) int {
	if configured != 0 {
		return resolveCapInt(configured, maxTurnScopedNotificationQueueSize)
	}
	if streamingBacklog < 0 {
		return Unlimited
	}
	if streamingQueue > math.MaxInt-streamingBacklog {
		return Unlimited
	}
	return streamingQueue + streamingBacklog
}

func resolveOrderedBacklog(configured, maxQueues, maxQueueSize int) int {
	if configured != 0 {
		return resolveCapInt(configured, maxOrderedNotificationBacklog)
	}
	if maxQueues < 0 || maxQueueSize < 0 {
		return Unlimited
	}
	if maxQueues > (math.MaxInt-maxQueueSize)/256 {
		return Unlimited
	}
	return maxQueues*256 + maxQueueSize
}
