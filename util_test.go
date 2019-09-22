package tdigest_test

import (
	"testing"

	"github.com/ajwerner/tdigest"
	"github.com/ajwerner/tdigest/testutils"
	"github.com/ajwerner/tdigest/testutils/accuracytest"
)

func TestAccuracy(t *testing.T) {
	constructorOps := testutils.CombineOptions(
		[]tdigest.Option{
			tdigest.BufferFactor(1),
			tdigest.BufferFactor(2),
			tdigest.BufferFactor(5),
			tdigest.BufferFactor(10),
			tdigest.BufferFactor(20),
		},
		[]tdigest.Option{
			tdigest.Compression(16),
			tdigest.Compression(64),
			tdigest.Compression(128),
			tdigest.Compression(256),
			tdigest.Compression(512),
		},
		[]tdigest.Option{
			tdigest.UseWeightLimit(true),
			tdigest.UseWeightLimit(false),
		},
	)
	tests := accuracytest.MakeTests(accuracytest.CombineOptions(
		[]accuracytest.Option{accuracytest.N(100000)},
		accuracytest.Distributions,
		accuracytest.Orders,
		append(
			accuracytest.Constructors(
				"Concurrent",
				func(o ...tdigest.Option) tdigest.Sketch {
					return tdigest.NewConcurrent(o...)
				},
				constructorOps),
			accuracytest.Constructors(
				"TDigest",
				func(o ...tdigest.Option) tdigest.Sketch {
					return tdigest.New(o...)
				},
				constructorOps)...),
	)...)
	for _, at := range tests {
		t.Run(at.String(), func(t *testing.T) { at.Run(t) })
	}
}
