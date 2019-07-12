package tdigest

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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

	mu struct {
		sync.RWMutex
		mergeBufMu syncutil.Mutex
		mergeBuf   *TDigest
		spare      *TDigest
		lastTick   time.Time
		ticks      int

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

// type digestRingBuf struct {
// 	head    int32
// 	len     int32
// 	digests []*closedTDigest
// }

// func (rb *digestRingBuf) back() *closedTDigest {
// 	return rb.at(rb.len - 1)
// }

// func (rb *digestRingBuf) at(idx int) *closedTDigest {
// 	return rb.digests[(rb.head+rb.len)%len(rb.digests)]
// }

// func (rb *digestRingBuf) pushFront(td *closedTDigest) {
// 	if rb.full() {
// 		panic("cannot push onto a full digest")
// 	}
// 	if rb.head--; rb.head < 0 {
// 		rb.head += len(rb.digests)
// 	}
// 	rb.digests[rb.head] = td
// }

// func (rb *digestRingBuf) full() bool {
// 	return rb.len == len(rb.digests)
// }

type level struct {
	period  int
	head    int
	digests []*TDigest
}

// NewWindowed returns a new Windowed TDigest.
func NewWindowed() *Windowed {
	// TODO(ajwerner): add configuration.
	w := &Windowed{
		tickInterval: time.Second,
	}
	const size = 128
	w.mu.levels = []level{
		{
			// (0-1)-(1-2)s
			period:  1,
			digests: makeDigests(1, size),
		},
		{
			// (0-2)-(2-4), (0-2)-(4-6), (0-2)-(6-8), (0-2)-(8-10)s
			period:  2,
			digests: makeDigests(4, size),
		},
		{
			period:  10,
			digests: makeDigests(5, size),
		},
		{
			period:  60,
			digests: makeDigests(4, size),
		},
		{
			period:  300,
			digests: makeDigests(2, size),
		},
		{
			period:  900,
			digests: makeDigests(1, size),
		},
	}
	w.mu.open = NewConcurrent(Compression(size), BufferFactor(10))
	w.mu.spare = New(Compression(size), BufferFactor(2))
	w.mu.mergeBuf = New(Compression(size), BufferFactor(len(w.mu.levels)-1))
	// Probably should make buffer factor here the max of len(levels) and the
	// max of any level in len

	return w
}

func makeDigests(n int, size int) []*TDigest {
	ret := make([]*TDigest, 0, n)
	for i := 0; i < n; i++ {
		ret = append(ret, New(Compression(float64(size)), BufferFactor(1)))
	}
	return ret
}

func (w *Windowed) AddAt(t time.Time, mean, count float64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if t.Sub(w.mu.lastTick) > w.tickInterval {
		w.tickAtRLocked(t)
	}
	w.mu.open.Add(mean, count)
	fmt.Printf("Add %v ticks: %v, mean: %v, count: %v: %v\n", t, w.mu.ticks, mean, count, w.stringRLocked(t))
}

func (w *Windowed) tickAtRLocked(t time.Time) {
	w.mu.RUnlock()
	defer w.mu.RLock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.lastTick.IsZero() {
		w.mu.lastTick = t
		return
	}
	ticksNeeded := int(t.Sub(w.mu.lastTick) / w.tickInterval)
	// TODO(ajwerner): optimize when many ticks are needed.
	if ticksNeeded <= 0 {
		return
	}
	for i := 0; i < ticksNeeded; i++ {
		w.tickLocked()
		fmt.Println("tick", w.mu.ticks, t, w.stringRLocked(w.mu.lastTick))
	}
}

func (w *Windowed) tickLocked() {
	// A tick means moving the current open interval down to the next level
	// It may also mean merging all of the current bottom level into a new
	// digest for the next level which may need to happen recursively.
	w.mu.ticks++
	w.mu.lastTick = w.mu.lastTick.Add(w.tickInterval)
	// Take the merged buf from the top and write it into the "spare"
	w.mu.open.compress()
	closed := w.mu.spare
	w.mu.spare = nil
	closed.numMerged = copy(closed.centroids,
		w.mu.open.centroids[:w.mu.open.mu.numMerged])
	closed.unmergedIdx = closed.numMerged
	w.mu.open.clear()
	for i := range w.mu.levels {
		l := &w.mu.levels[i]
		for _, d := range l.digests {
			d.Merge(closed)
		}
		needsTick := w.mu.ticks%(l.period) == 0
		// This assumes that if a level does not tick then a higher level cannot
		// either.
		if !needsTick {
			break
		}
		curTail := l.head - 1
		if curTail < 0 {
			curTail = len(l.digests) - 1
		}
		toDiscard := l.digests[curTail]
		l.head = curTail
		//fmt.Println("set head", l.head, "of level", i, "to", closed)
		l.digests[l.head] = closed
		closed = toDiscard
	}
	closed.clear()
	w.mu.spare = closed
}

var NowFunc = time.Now

func (w *Windowed) String() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stringRLocked(NowFunc())
}

func (w *Windowed) stringRLocked(now time.Time) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "Windowed{lastTick: %v\n", w.mu.lastTick)
	curDur := now.Sub(w.mu.lastTick)
	tc := w.mu.open.TotalCount()
	fmt.Fprintf(&buf, "\tnow (0-%v): %v %v\n", now.Sub(w.mu.lastTick), w.mu.open.String(), tc)
	for i := range w.mu.levels {
		l := &w.mu.levels[i]
		offset := curDur + w.tickInterval*time.Duration(w.mu.ticks%(l.period+1))
		for j, d := range l.digests {
			dur := offset + w.tickInterval*time.Duration((j+1)*l.period)
			d.compress()
			fmt.Fprintf(&buf, "\t%v,%v: (%v-%v): %v\n", i, j, offset, dur, d)
		}
	}
	return buf.String()
}

type WindowedReader interface {
	Reader(trailing time.Duration) (last time.Duration, r Reader)
}

type windowedReader Windowed

func (w *Windowed) Reader(f func(WindowedReader)) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	w.mu.mergeBufMu.Lock()
	defer w.mu.mergeBufMu.Unlock()
	f((*windowedReader)(w))
}

func (w *windowedReader) Reader(trailing time.Duration) (last time.Duration, r Reader) {
	w.mu.mergeBuf.clear()
	var d time.Duration

	// I feel like I should just "hard-code" some values

	// ticks := int(trailing / w.tickInterval)
	// we want to add all of the
	// for i := 0; i < len(w.levels); i++ {
	// 	l := &w.mu.levels[i]
	// 	if l.period*len(l.digests) > ticks {
	// 		// we
	// 	}
	// 	// for each level we want to add the part that is represented in that level
	// 	// that is not represented in the level above.

	// }
	for i := w.findLevel(trailing); i >= 0; i-- {
		l := &w.mu.levels[i]
		d += w.tickInterval * time.Duration(l.period*len(l.digests))
		totalCount := 0.0
		digest := l.digests[(l.head+len(l.digests)-1)%len(l.digests)]
		for _, c := range digest.centroids[:digest.numMerged] {
			w.mu.mergeBuf.Add(c.mean, c.count-totalCount)
			totalCount = c.count
		}
	}
	w.mu.mergeBuf.compress()
	return d, w.mu.mergeBuf
}

func (w *windowedReader) findLevel(trailing time.Duration) int {
	for i := len(w.mu.levels) - 1; i >= 0; i-- {
		l := &w.mu.levels[i]
		if w.tickInterval*time.Duration(l.period*len(l.digests)) < trailing {
			return i
		}
	}
	return 0
}

//   ]
//      [1s] 3s] 5s] 7s] 9s]
//
//    |
//    V
// :]
//

//
//_______________________________________________________________________________
//
//      0  |  |  |  |  V  |  |  |  |  X  |  |  |  |  XV |  |  |  |  XX |  |  |  | XXV |  |  |  |XXX
//  0| -1s]<
//  1|    (2s]  <4s]  <6s]  <8s] <10s]<
//  2|                               (                         <20s]                        <30s]<
//  3|---------------------------------------------------------------------------------------------
//  2|     (20]30]40]50]<
//  3|                 (              ]              ]              ]              ]             ]<
//      0  |  |  |  |  L  |  |  |  |  C  |  |  |  |  CL |  |  |  |  CC |  |  |  | CCL |  |  |  |CCC
//_______________________________________________________________________________

//-------------------------------------------------------------------------------
//
//      (1s]
//         (2s]
//            (   4s]   6s]   8s]  10s]
//                                    (                          20s]                          30s]
//      ___________________________________________________________________________________________
//      0  |  |  |  |  V  |  |  |  |  X  |  |  |  |  V  |  |  |  |  D  |  |  |  |  V  |  |  |  |  D
//
//
//  1|  (   2s]   4s]   6s]   8s]  10s]
//  2|                                (                          20s]                         30s]
//  3|..___________________________________________________________________________________________
//      0  |  |  |  |  V  |  |  |  |  X  |  |  |  |  XV |  |  |  |  XX |  |  |  | XXV |  |  |  |XXX

//-------------------------------------------------------------------------------
//
//  We only keep these around so that we can read at this level with a 2s window
//
//  1|  (   2s]   4s]   6s]   8s]
//  2|  (                             ]                          20s]
//  3|..___________________________________________________________________________________________
//      0  |  |  |  |  V  |  |  |  |  X  |  |  |  |  XV |  |  |  |  XX |  |  |  | XXV |  |  |  |XXX
//
//  2|  (20]  ]  ]  ]
//  3|  (           ]              ]              ]              ]
//      ___________________________________________________________________________________________
//      0  |  |  |  |  L  |  |  |  |  C  |  |  |  |  CL |  |  |  |  CC |  |  |  | CCL |  |  |  |CCC

//-------------------------------------------------------------------------------
//
//  1|  (   2s]   4s]   6s]   8s]
//  2|  (                             ]                          20s]
//  3|..___________________________________________________________________________________________
//      0  |  |  |  |  V  |  |  |  |  X  |  |  |  |  XV |  |  |  |  XX |  |  |  | XXV |  |  |  |XXX
//
//  2|  (20]  ]  ]  ]
//  3|  (           ]              ]              ]              ]
//      ___________________________________________________________________________________________
//      0  |  |  |  |  L  |  |  |  |  C  |  |  |  |  CL |  |  |  |  CC |  |  |  | CCL |  |  |  |CCC
//-------------------------------------------------------------------------------

//
// I can read from the combo of the last window of each layer and the open window.
// The read buffer at most needs to be the compression factor*depth
//
//
// Upon tick I have to ask if I need to use the full thing?
//
//
//     -]
//      (   2s]   4s]   6s]   8s]  10s]
//      _______________________________
//      0  |  |  |  |  V  |  |  |  |  D
//
//

//      0  |  |  |  |  V  |  |  |  |  D  |  |  |  |  V  |  |  |  |  D
//    (<1s]
//        (<2s]  <4s]  <6s]  <8s] <10s]
//                                    (                         <20s]                         <30s]
//
//      0  |  |  |  |  V  |  |  |  |  D
//      (1s]
//         (2s]   4s]   6s]   8s]  10s]
//      0  |  |  |  |  V  |  |  |  |  D
//      (   2s]   4s]   6s]   8s]  10s]
//      0  |  |  |  |  V  |  |  |  |  D

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
