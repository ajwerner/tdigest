package tdigest

import "math"

type scaleFunc interface {
	normalizer(compression, totalCount float64) float64
	k(q, normalizer float64) float64
	q(k, normalizer float64) float64
	max(q, normalizer float64) float64
}

type k2 struct{}

func (_ k2) normalizer(compression, totalCount float64) float64 {
	return compression/(4*math.Log(totalCount/compression)) + 24
}

func (_ k2) k(q, normalizer float64) float64 {
	var isExtreme bool
	if q < 1e-15 {
		q = 1e-15
		isExtreme = true
	} else if q > (1 - 1e-15) {
		q = 1 - 1e15
		isExtreme = true
	}
	k := math.Log(q/(1-q)) * normalizer
	if isExtreme {
		k *= 2
	}
	return k
}

func (_ k2) q(k, normalizer float64) (ret float64) {
	w := math.Exp(k / normalizer)
	return w / (1 + w)
}

func (_ k2) max(q, normalizer float64) float64 {
	return q * (1 - q) / normalizer
}
