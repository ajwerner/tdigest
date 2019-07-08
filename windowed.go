package tdigest

import (
	"sync"
	"time"
)

// NewWindowed returns a new Windowed TDigest.
func NewWindowed() *Windowed {
	// TODO(ajwerner): add configuration.

	w := &Windowed{
		tickInteval: time.Second,
	}
	w.mu.levels = []level{
		{
			period:   1,
			nextTick: 1,
			digests:  makeDigests(6),
		},
	}
}

func makeDigests(n int) []*TDigest {
	ret := make([]*TDigest, 0, n)
	for i := 0; i < n; i++ {
		ret = append(ret, NewTDigest(BufferFactor(0)))
	}
	return ret
}

// Windowed is a TDigest that provides a single write path but provides mechanisms to read
// at various different timescales. The Windowed structure uses hierarchical windows to provide
// high precision estimates at approximate timescales while requiring sublinear number of buckets
// to achieve that historical timescale. The Windowed structure makes tradeoffs which can be configured
//
// Imagine we wanted to know about the trailing 1s and 1m. In practice one thing that we could do is
// keep a ring-buffer of the last 1m of tdigests in 1s intervals such that there are 59 "closed" digests
// which only contain a "merged" buffer and no write buffer as well as two "read" digests and an "open"
// digest. This will contain a single concurrent write buffer that gets merged into each of the "read"
// digests as well as the "open" digest.
//
// This already offers some issues, it is not actually the trailing 1s and 1m you'll be reading but rather
// it's the last 1-2s depending on when the last tick occurred and similarly for the minute it's the last
// 1m-1m1s. For the minute this certainly isn't a problem and even for the trailing 1s this is probably okay
// (furthermore, we're probably not interested in the trailing 1s, instead we're more likely to be interested
// in the last ~10s.
//
// We can extend this tradeoff further by allowing further window size. For example, imagine we keep this
// 1s buffer as described above, but then we also keep a next layer which represent 2s intervals, then we
// only need to keep 4 of them to get to a trailing 10s buffer with a 2s window size. We can then layer these
// things up to use 5 more to get the trailing 1m with a 10s window size. This allows us to get a reasonable
// window size on a 5 minute trailing period of 1m using just 15 tdigests instead of the 300 we'd need if we
// kept all 1s ring buffers.
type Windowed struct {
	tickInterval time.Duration
	lastTick     time.Time
	mu           struct {
		// Use the concurrent's RWMutex?

		cond sync.Cond
		// We need to have levels
		open   *Concurrent
		levels []level
	}
	// We now want some number of levels where each level has a tick period
	// a next tick var (or last, same difference). It also has a slice of
	// tdigest structs.
	//
	// Then, upon each tick, we add the open to what we need to and then
	// we tick the other levels as needed.

	// this is hand-wavy but maybe will work?

}

func (w *Windowed) AddAt(t *time.Time, mean, count float64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if t.Sub(w.mu.lastTick) > w.tickInterval {
		w.tickAtRLocked(t)
	}
	w.mu.open.Add(mean, count)
}

func (w *Windowed) Reader(level int) Reader {
	return w.mu.levels[level].Reader
}

type level struct {
	period   int
	nextTick int
	head     int
	digests  []*TDigest
	reader   *TDigest

	// I want to have something that will always have a readable value for
	// each timescale that is interesting.
	// We can acheive this by keeping multiple levels of fully merged digests.

	// 5
	// []
	// []
	// 0-1s
	//
	// (0-1)-(1-5) (0-5)-(5-10)
	//
	// []            []           []           // Only need to keep an open one if you want to read
	// (0-1)-(1-2)s  (0-1)-(2-3)s (0-1)-(3-4)s 4-5s
	//
	// 5
	// []             []     []     []     []// Only need to keep an open one if you want to read
	// (0-5)-(5-10)s  (0-5)-(10-15s)  15-20s 20-25s 25-30s
	//
	// 5
	// []      []     []
	// 30-60s  60-90s 90-2m
	//
	// 2m-4m 4m-6m 6m-8m  8m-10m
	// []    []    []     []
	//
	// * 0-(10-12)m updated every 5s rotated every 2m
	// []

	// You define a base tick unit and then a number of levels each with a number of buckets
	// Then you can define a reader at any unit that is constructable from the base tick unit
	// with a granularity of some smaller unit.
	//
	// For example:
	//

	// On add:
	// add to the buffer, if full, merge into open base
	// On tick, check for needs to tick in reverse order
	// Tick by

	open *Concurrent
}

// The interface we want is to have readers which dictate their trailing age range

// (0-f)-(d-d+w) // three parameter
//
// (0-1s)-(5m-5m+30s)
// (0-1s)-(10s-15s)
// To acheive this you need to be able to merge

// Tick algorithm:
// Determine the tick needs from largest down to smallest.
// Every smaller level will tick if its parent ticks.
// Ticking generally requires throwing something away and then taking
// the just produced value from ticking the previous level.
// I guess it looks like

func (l level) tick(num int) {
	// If it's time for me to tick (num%l.interval == 0) then remove my largest value.
	// We need to thread a merge buffer down into the recursive call.
}

type WindowedConfig struct {
	Tick    time.Duration
	Levels  []int
	Readers []ReaderConfig
}

// This should say something about when a a reader gets updated and re-created
type WindowedReaderConfig struct {
}
