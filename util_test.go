package tdigest_test

import (
	"testing"

	"github.com/ajwerner/tdigest/testutils/accuracytest"
)

func TestIt(t *testing.T) {
	for _, at := range accuracytest.Tests {
		t.Run(at.String(), func(t *testing.T) { at.Run(t) })
	}
}
