// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Zero timeout returns the parent unchanged (legacy behavior).
func TestOnErrorFlushContextZeroTimeoutInheritsParent(t *testing.T) {
	parent, cancel := context.WithDeadline(t.Context(), time.Now().Add(42*time.Millisecond))
	defer cancel()

	flushCtx, flushCancel := onErrorFlushContext(parent, 0)
	defer flushCancel()

	require.Equal(t, parent, flushCtx, "zero timeout must return parent as-is")
	deadline, ok := flushCtx.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(42*time.Millisecond), deadline, 10*time.Millisecond)
}

// A positive timeout detaches the flush from an expired parent deadline.
func TestOnErrorFlushContextDetachesDeadline(t *testing.T) {
	// Parent is already past its deadline.
	parent, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	require.Error(t, parent.Err(), "sanity: parent must be expired")

	flushCtx, flushCancel := onErrorFlushContext(parent, 500*time.Millisecond)
	defer flushCancel()

	require.NoError(t, flushCtx.Err(), "flush ctx must not inherit parent's cancellation")
	deadline, ok := flushCtx.Deadline()
	require.True(t, ok, "flush ctx must have its own deadline")
	assert.True(t, deadline.After(time.Now()), "flush deadline must be in the future")
}

// Parent cancellation does not propagate; only the flush timeout ends it.
func TestOnErrorFlushContextIgnoresParentCancel(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	flushCtx, flushCancel := onErrorFlushContext(parent, 50*time.Millisecond)
	defer flushCancel()

	cancel()
	// Parent is cancelled; flush ctx should remain alive briefly, then time out.
	require.NoError(t, flushCtx.Err(), "parent cancel must not propagate")
	<-flushCtx.Done()
	assert.ErrorIs(t, flushCtx.Err(), context.DeadlineExceeded, "flush ctx must end on its own timeout")
}
