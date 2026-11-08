package apis

import (
	"testing"
	"time"
)

func TestRateClientResetsAfterBackwardClockStep(t *testing.T) {
	client := newRateClient(2, 60)

	// an exhausted window that starts in the future, as left behind by a
	// backward wall-clock step
	client.start = time.Now().Unix() + 3600
	client.available = 0

	if !client.consume() {
		t.Fatal("Expected the window to reset after a backward clock step")
	}

	if !client.consume() {
		t.Fatal("Expected the reset window to allow maxAllowed requests")
	}

	if client.consume() {
		t.Fatal("Expected the reset window to be exhausted after maxAllowed requests")
	}
}
