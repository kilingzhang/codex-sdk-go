package transport

import "time"

// Unlimited disables a supported numeric limit or default timeout when assigned
// to a limit field in StdioTransportConfig.
const Unlimited = -1

// StdioTransportConfig configures queue capacities, worker counts, memory
// budgets, frame limits, and default send timeout for a StdioTransport.
//
// By default (zero values or DefaultStdioTransportConfig), all size, queue,
// backlog, memory budget, and default send timeout limits are disabled
// (Unlimited), while worker counts, Go channel buffer capacities, and
// ReadBufferSizeBytes fall back to positive built-in sizes when <= 0.
// Use BoundedStdioTransportConfig to opt into conservative upper bounds.
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

	// Backlog and scope limits (<= 0 means Unlimited; > 0 sets an upper bound).
	MaxStreamingNotificationBacklog    int
	MaxTurnScopedNotificationQueues    int
	MaxTurnScopedNotificationQueueSize int
	MaxOrderedNotificationBacklog      int

	// Framing and retained memory limits (ReadBufferSizeBytes <= 0 uses default;
	// MaxInboundMessageSizeBytes, MaxRetainedNotificationBytes, and
	// MaxRetainedRejectionBytes <= 0 mean Unlimited).
	ReadBufferSizeBytes          int
	MaxInboundMessageSizeBytes   int
	MaxRetainedNotificationBytes int
	MaxRetainedRejectionBytes    int

	// SendTimeout sets the default timeout for outbound requests and
	// notifications when the caller context has no deadline.
	// <= 0 disables the default send timeout (Unlimited); > 0 applies the timeout.
	SendTimeout time.Duration
}

// DefaultStdioTransportConfig returns a StdioTransportConfig with no limits
// enforced on frame sizes, retained bytes, queues, backlogs, or send timeouts.
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
		MaxStreamingNotificationBacklog:    Unlimited,
		MaxTurnScopedNotificationQueues:    Unlimited,
		MaxTurnScopedNotificationQueueSize: Unlimited,
		MaxOrderedNotificationBacklog:      Unlimited,
		ReadBufferSizeBytes:                readBufferSizeBytes,
		MaxInboundMessageSizeBytes:         Unlimited,
		MaxRetainedNotificationBytes:       Unlimited,
		MaxRetainedRejectionBytes:          Unlimited,
		SendTimeout:                        time.Duration(Unlimited),
	}
}

// BoundedStdioTransportConfig returns a StdioTransportConfig populated with
// conservative upper bounds for untrusted-peer protection (10 MiB frames,
// 64 MiB retained budgets, 128 active scopes, 1,280 per-scope events, 5m send timeout).
func BoundedStdioTransportConfig() StdioTransportConfig {
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
	sendTimeout := cfg.SendTimeout
	if sendTimeout <= 0 {
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
		StreamingNotificationQueueSize:     resolvePositiveInt(cfg.StreamingNotificationQueueSize, streamingNotifQueueSize),
		ProtectedNotificationQueueSize:     resolvePositiveInt(cfg.ProtectedNotificationQueueSize, protectedNotifQueueSize),
		CriticalNotificationQueueSize:      resolvePositiveInt(cfg.CriticalNotificationQueueSize, criticalNotifQueueSize),
		OutboundWriteQueueSize:             resolvePositiveInt(cfg.OutboundWriteQueueSize, outboundWriteQueueSize),
		MaxStreamingNotificationBacklog:    resolveLimitInt(cfg.MaxStreamingNotificationBacklog),
		MaxTurnScopedNotificationQueues:    resolveLimitInt(cfg.MaxTurnScopedNotificationQueues),
		MaxTurnScopedNotificationQueueSize: resolveLimitInt(cfg.MaxTurnScopedNotificationQueueSize),
		MaxOrderedNotificationBacklog:      resolveLimitInt(cfg.MaxOrderedNotificationBacklog),
		ReadBufferSizeBytes:                resolvePositiveInt(cfg.ReadBufferSizeBytes, readBufferSizeBytes),
		MaxInboundMessageSizeBytes:         resolveLimitInt(cfg.MaxInboundMessageSizeBytes),
		MaxRetainedNotificationBytes:       resolveLimitInt(cfg.MaxRetainedNotificationBytes),
		MaxRetainedRejectionBytes:          resolveLimitInt(cfg.MaxRetainedRejectionBytes),
		SendTimeout:                        sendTimeout,
	}
}

func resolvePositiveInt(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func resolveLimitInt(value int) int {
	if value <= 0 {
		return Unlimited
	}
	return value
}
