package windowed_test

import (
	"fmt"
	"testing"
	"time"

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
	w := windowed.NewWindowed()
	w.AddAt(t0, 0, 1)
	w.AddAt(t0_001, .001, 1)
	w.AddAt(t0_500, .5, 1)
	w.AddAt(t1, 1, 1)
	w.AddAt(t1_001, 1.001, 1)
	w.AddAt(t1_500, 1.500, 1)
	w.AddAt(t100, 100, 1)
	w.Reader(func(wr windowed.WindowedReader) {
		d, r := wr.Reader(1 * time.Second)
		fmt.Println(d, r.ValueAt(.99))
		d, r = wr.Reader(60 * time.Second)
		fmt.Println(d, r.ValueAt(.99))
		d, r = wr.Reader(61 * time.Second)
		fmt.Println(d, r.ValueAt(.99))
		d, r = wr.Reader(4 * time.Minute)
		fmt.Println(d, r.ValueAt(.99))
	})
}

func TestWindowedMore(t *testing.T) {
	w := windowed.NewWindowed()
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
	w.Reader(func(wr windowed.WindowedReader) {
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
			trailing, reader := wr.Reader(d)
			fmt.Printf("%6v %6v %v\n", d, trailing, reader)
		}
		fmt.Println(w)
	})
}
