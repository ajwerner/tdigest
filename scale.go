package tdigest

import "math"

type scaleFunc interface {
	normalizer(compression, totalCount float64) float64
	k(q, normalizer float64) float64
	q(k, normalizer float64) float64
	max(q, normalizer float64) float64
}

type k2 struct{}

func (f k2) normalizer(compression, totalCount float64) float64 {
	return compression / f.z(compression, totalCount)
}

func (f k2) z(compression, totalCount float64) float64 {
	return 4*math.Log(totalCount/compression) + 24
}

func (f k2) k(q, normalizer float64) float64 {
	if q < 1e-15 {
		return 2 * f.k(1e-15, normalizer)
	} else if q > (1 - 1e-15) {
		return 2 * f.k(1-1e-15, normalizer)
	}
	return math.Log(q/(1-q)) * normalizer
}

func (f k2) q(k, normalizer float64) (ret float64) {
	w := math.Exp(k / normalizer)
	return w / (1 + w)
}

func (f k2) max(q, normalizer float64) float64 {
	return q * (1 - q) / normalizer
}
