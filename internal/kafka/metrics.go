package kafka

import "sync/atomic"

type Metrics struct {
	Published uint64
	Failed    uint64
	Batches   uint64
	Retries   uint64
}

type counters struct {
	published atomic.Uint64
	failed    atomic.Uint64
	batches   atomic.Uint64
	retries   atomic.Uint64
}

func (c *counters) snapshot() Metrics {
	return Metrics{
		Published: c.published.Load(),
		Failed:    c.failed.Load(),
		Batches:   c.batches.Load(),
		Retries:   c.retries.Load(),
	}
}
