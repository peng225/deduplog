package deduplog

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	DefaultHistoryRetentionPeriod time.Duration = time.Second * 10
	DefaultMaxHistoryCount        int           = 1024
)

type HandlerOptions struct {
	HistoryRetentionPeriod time.Duration
	MaxHistoryCount        int
	DedupLogLevel          slog.Level
}

type DedupHandler struct {
	ctx          context.Context
	mu           sync.Mutex
	handler      slog.Handler
	opts         HandlerOptions
	history      map[string]time.Time
	historyCount int
}

func NewDedupHandler(ctx context.Context, handler slog.Handler, opts *HandlerOptions) *DedupHandler {
	h := &DedupHandler{
		ctx:     ctx,
		mu:      sync.Mutex{},
		handler: handler,
		history: make(map[string]time.Time),
	}

	if opts != nil {
		h.opts = *opts
	} else {
		h.opts.HistoryRetentionPeriod = DefaultHistoryRetentionPeriod
		h.opts.MaxHistoryCount = DefaultMaxHistoryCount
		h.opts.DedupLogLevel = slog.LevelInfo
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
			h.historyCount -= 1
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

func (h *DedupHandler) removeOldestHistory() {
	var toBeDeletedKey string
	toBeDeletedTime := time.Now().Add(h.opts.HistoryRetentionPeriod)
	for k, v := range h.history {
		if v.Before(toBeDeletedTime) {
			toBeDeletedKey = k
			toBeDeletedTime = v
		}
	}
	if toBeDeletedKey == "" {
		panic("toBeDeletedKey should not be empty.")
	}
	delete(h.history, toBeDeletedKey)
	h.historyCount -= 1
}

func (h *DedupHandler) updateHistory(msg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.history[msg]; !ok {
		if h.historyCount >= h.opts.MaxHistoryCount {
			h.removeOldestHistory()
		}
		h.historyCount += 1
	}
	h.history[msg] = time.Now().Add(h.opts.HistoryRetentionPeriod)
}

func (h *DedupHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level <= h.opts.DedupLogLevel && h.duplicated(r.Message) {
		return nil
	}
	h.updateHistory(r.Message)
	return h.handler.Handle(ctx, r)
}

func (h *DedupHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return NewDedupHandler(h.ctx, h.handler.WithAttrs(attrs), &h.opts)
}

func (h *DedupHandler) WithGroup(name string) slog.Handler {
	return NewDedupHandler(h.ctx, h.handler.WithGroup(name), &h.opts)
}
