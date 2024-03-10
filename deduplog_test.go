package deduplog

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupLog(t *testing.T) {
	b := new(bytes.Buffer)
	logger := slog.New(NewDedupHandler(context.Background(), slog.NewJSONHandler(b, nil),
		&HandlerOptions{
			HistoryRetentionPeriod: time.Minute,
		}))
	require.NotNil(t, logger)

	logger.Info("test")
	expectedMsg := "test"
	jsonLog := make(map[string]string)
	err := json.Unmarshal(b.Bytes(), &jsonLog)
	require.NoError(t, err)
	assert.Equal(t, expectedMsg, jsonLog["msg"])

	// The same log is deduplicated.
	b.Reset()
	logger.Info("test")
	assert.Empty(t, b.String())

	// Slightly different log is not deduplicated.
	b.Reset()
	logger.Info("test2")
	expectedMsg = "test2"
	jsonLog = make(map[string]string)
	err = json.Unmarshal(b.Bytes(), &jsonLog)
	require.NoError(t, err)
	assert.Equal(t, expectedMsg, jsonLog["msg"])
}
