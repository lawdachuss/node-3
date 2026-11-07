package channel

import (
	"reflect"
	"testing"

	"github.com/teacat/chaturbate-dvr/entity"
)

// TestPipelineQueuePopsLargestFirst verifies the queue keeps pipelines sorted
// by FileSize descending: workers always pick the biggest recording next,
// regardless of the order pipelines were enqueued (including retries and
// resumes, which go through the same insertion path).
func TestPipelineQueuePopsLargestFirst(t *testing.T) {
	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "alice"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	pq := NewPipelineQueue(ch)

	// Enqueue deliberately out of order, including a zero-size entry (a file
	// whose stat failed must sort last, not first).
	sizes := []int64{100, 5, 5000, 42, 0, 1000}
	for i, s := range sizes {
		p := newPipeline("/videos/f.mp4", string(rune('a'+i)), "f.mp4", "alice", s)
		pq.mu.Lock()
		pq.enqueueByPriority(p)
		pq.mu.Unlock()
	}

	var got []int64
	pq.mu.Lock()
	for len(pq.pipelines) > 0 {
		p := pq.pipelines[0]
		pq.pipelines = pq.pipelines[1:]
		got = append(got, p.FileSize)
	}
	pq.mu.Unlock()

	want := []int64{5000, 1000, 100, 42, 5, 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pop order = %v, want %v", got, want)
	}
}

// TestPipelineQueuePendingBytes verifies the early-drain estimate: queued
// pipelines plus the file currently being processed.
func TestPipelineQueuePendingBytes(t *testing.T) {
	ch := &Channel{
		Config:   &entity.ChannelConfig{Username: "alice"},
		LogCh:    make(chan string, 20),
		UpdateCh: make(chan bool, 1),
	}
	pq := NewPipelineQueue(ch)

	for _, s := range []int64{1000, 2000, 4000} {
		p := newPipeline("/videos/f.mp4", "h", "f.mp4", "alice", s)
		pq.mu.Lock()
		pq.enqueueByPriority(p)
		pq.mu.Unlock()
	}

	// Simulate one file currently being processed by a worker.
	pq.mu.Lock()
	pq.processingBytes = 8000
	pq.mu.Unlock()

	if got := pq.PendingBytes(); got != 1000+2000+4000+8000 {
		t.Fatalf("PendingBytes = %d, want %d", got, 1000+2000+4000+8000)
	}
}
