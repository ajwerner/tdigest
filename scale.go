package tdigest

import (
	"fmt"
	"math"
)

type scaleFunc int

const (
	// k2 is the default scaleFunc.
	k2 scaleFunc = iota
)

var scaleFuncs = []struct {
	normalizer func(compression, totalCount float64) float64
	k          func(q, normalizer float64) float64
	q          func(k, normalizer float64) float64
	max        func(q, normalizer float64) float64
}{
	k2: {
		normalizer: func(compression, totalCount float64) float64 {
			return compression / (4*math.Log(totalCount/compression) + 24)
		},
		k: func(q, normalizer float64) float64 {
			if q < 1e-15 {
				q = 1e-15
			} else if q > 1-1e15 {
				q = 1 - 1e15
			}

			fmt.Println(math.Log(1/(1-q)) * normalizer)
			return math.Log(1/(1-q)) * normalizer
		},
		q: func(k, normalizer float64) float64 {
			w := math.Exp(k / normalizer)
			return w / (1 + w)
		},
		max: func(q, normalizer float64) float64 {
			return q * (1 - q) / normalizer
		},
	},
}

func (sf scaleFunc) normalizer(compression, totalCount float64) float64 {
	return scaleFuncs[sf].normalizer(compression, totalCount)
}

func (sf scaleFunc) k(q, normalizer float64) float64 {
	return scaleFuncs[sf].k(q, normalizer)
}

func (sf scaleFunc) q(k, normalizer float64) float64 {
	return scaleFuncs[sf].q(k, normalizer)
}

func (sf scaleFunc) max(k, normalizer float64) float64 {
	return scaleFuncs[sf].max(k, normalizer)
}
