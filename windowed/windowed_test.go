package windowed_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ajwerner/tdigest"
	"github.com/ajwerner/tdigest/windowed"
)

var (
	t0     = time.Unix(0, 0)
	t0_001 = t0.Add(time.Millisecond)
	t0_500 = t0.Add(500 * time.Millisecond)
	t1     = t0.Add(time.Second)
	t1_001 = t1.Add(time.Millisecond)
	t1_500 = t1.Add(500 * time.Millisecond)
	t100   = t0.Add(100 * time.Second)
)

func TestWindowed(t *testing.T) {
	w := windowed.NewTDigest()
	start := time.Now()
	for d := 0 * time.Nanosecond; d < 20*time.Minute; d += time.Second {
		dd := d
		w.AddAt(start.Add(dd), dd.Seconds(), 1)
		dd = d + 400*time.Millisecond
		dd = d + 200*time.Millisecond
		w.AddAt(start.Add(dd), dd.Seconds(), 1)
		dd = d + 300*time.Millisecond
		w.AddAt(start.Add(dd), dd.Seconds(), 1)
	}

	var r windowed.Reader
	for _, d := range []time.Duration{
		time.Second,
		2 * time.Second,
		5 * time.Second,
		9 * time.Second,
		10 * time.Second,
		11 * time.Second,
		59 * time.Second,
		60 * time.Second,
		4 * time.Minute,
		5 * time.Minute,
		5*time.Minute + time.Second,
		10 * time.Minute,
	} {
		r.Read(d, w, func(trailing time.Duration, reader tdigest.Reader) {
			fmt.Printf("%6v %6v %v\n", d, trailing, reader)
		})
	}
	fmt.Println(w)
}
