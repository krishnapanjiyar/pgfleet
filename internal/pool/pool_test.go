package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestMapPreservesOrder(t *testing.T) {
	in := []int{5, 4, 3, 2, 1, 0}
	out := Map(context.Background(), 3, in, func(_ context.Context, _ int, v int) int {
		time.Sleep(time.Duration(v) * time.Millisecond) // finish out of order
		return v * 10
	})
	for i, v := range in {
		if out[i] != v*10 {
			t.Fatalf("out[%d]=%d want %d", i, out[i], v*10)
		}
	}
}

func TestMapBoundsConcurrency(t *testing.T) {
	var cur, peak int32
	Map(context.Background(), 4, make([]struct{}, 40), func(context.Context, int, struct{}) int {
		n := atomic.AddInt32(&cur, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt32(&cur, -1)
		return 0
	})
	if peak > 4 {
		t.Fatalf("peak concurrency %d exceeds 4 workers", peak)
	}
	if peak < 2 {
		t.Fatalf("expected parallelism, peak=%d", peak)
	}
}

func TestMapEmpty(t *testing.T) {
	if got := Map(context.Background(), 8, []int{}, func(context.Context, int, int) int { return 1 }); len(got) != 0 {
		t.Fatal("expected empty result")
	}
}
