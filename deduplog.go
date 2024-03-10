package deduplog

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type HandlerOptions struct {
	HistoryRetentionPeriod time.Duration
}

type DedupHandler struct {
	mu      *sync.Mutex
	handler slog.Handler
	opts    HandlerOptions
	history map[string]time.Time
}

func NewDedupHandler(ctx context.Context, handler slog.Handler, opts *HandlerOptions) *DedupHandler {
	h := &DedupHandler{
		mu:      &sync.Mutex{},
		handler: handler,
	}

	if opts != nil {
		h.opts = *opts
	}

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.removeExpiredHistory()
		}

	}()

	return h
}

func (h *DedupHandler) expired(expireTime time.Time) bool {
	return time.Now().After(expireTime)
}

func (h *DedupHandler) removeExpiredHistory() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for k, v := range h.history {
		if h.expired(v) {
			delete(h.history, k)
		}
	}
}

func (h *DedupHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *DedupHandler) duplicated(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.history[msg]; !ok {
		return false
	}
	if h.expired(h.history[msg]) {
		return false
	}
	return true
}

func (h *DedupHandler) updateHistory(msg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.history[msg] = time.Now().Add(h.opts.HistoryRetentionPeriod)
}

func (h *DedupHandler) Handle(ctx context.Context, r slog.Record) error {
	defer h.updateHistory(r.Message)

	if h.duplicated(r.Message) {
		return nil
	}
	return h.handler.Handle(ctx, r)
}

func (h *DedupHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.handler.WithAttrs(attrs)
}

func (h *DedupHandler) WithGroup(name string) slog.Handler {
	return h.handler.WithGroup(name)
}
