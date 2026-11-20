// Package pool provides a small bounded worker pool built on goroutines.
package pool

import (
	"context"
	"sync"
)

// Map runs fn over items using at most `workers` goroutines and returns the
// results in input order. fn receives the item index so callers can log it.
// Map never leaks goroutines: it returns only after every worker has exited.
func Map[T, R any](ctx context.Context, workers int, items []T, fn func(ctx context.Context, i int, item T) R) []R {
	out := make([]R, len(items))
	if len(items) == 0 {
		return out
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(items) {
		workers = len(items)
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				out[i] = fn(ctx, i, items[i])
			}
		}()
	}
	for i := range items {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return out
}
