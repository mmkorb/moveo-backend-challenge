//go:build !integration

package main

import (
	"testing"

	"go.uber.org/goleak"
)

// Integration runs have their own TestMain (testcontainers keeps background
// goroutines alive for the whole run), so the leak check covers the unit run only.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
