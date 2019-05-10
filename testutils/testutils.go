package testutils

import "github.com/ajwerner/tdigest"

func CombineOptions(options ...[]tdigest.Option) [][]tdigest.Option {
	dims := make([]int, 0, len(options))
	for _, o := range options {
		dims = append(dims, len(o))
	}
	var out [][]tdigest.Option
	create := func(n int) {
		out = make([][]tdigest.Option, n)
	}
	set := func(outIdx, dim, dimPos int) {
		if len(out[outIdx]) == 0 {
			out[outIdx] = make([]tdigest.Option, len(options))
		}
		out[outIdx][dim] = options[dim][dimPos]
	}
	CartesianProduct(create, set, dims...)
	return out
}

type Options []tdigest.Option

func CartesianProduct(create func(numOut int), set func(out, dim, dimPos int), dims ...int) {
	if len(dims) == 0 {
		return
	}
	c := cursor{
		dims: dims,
		cur:  make([]int, len(dims)),
	}
	n := c.numOut()
	create(n)
	out := 0
	for ok := true; ok && out < n; ok = c.next() {
		for dim := 0; dim < len(dims); dim++ {
			set(out, dim, c.cur[dim])
		}
		out++
	}
}

type cursor struct {
	cur  []int
	dims []int
}

func (c cursor) numOut() int {
	n := 1
	for _, dim := range c.dims {
		n *= dim
	}
	return n
}

func (c cursor) next() (ok bool) {
	curDim := len(c.cur) - 1
	for ; curDim >= 0; curDim-- {
		c.cur[curDim]++
		if c.cur[curDim] < c.dims[curDim] {
			curDim++
			break
		}
	}
	if curDim < 0 {
		return false
	}
	for ; curDim < len(c.cur); curDim++ {
		c.cur[curDim] = 0
	}
	return true
}
