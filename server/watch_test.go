package server

import "testing"

func TestEffectiveStartRevisionDefaultsToNextRevision(t *testing.T) {
	if got := effectiveStartRevision(0, 12); got != 13 {
		t.Fatalf("effectiveStartRevision(0, 12) = %d, want 13", got)
	}
}

func TestEffectiveStartRevisionKeepsExplicitRevision(t *testing.T) {
	if got := effectiveStartRevision(7, 12); got != 7 {
		t.Fatalf("effectiveStartRevision(7, 12) = %d, want 7", got)
	}
}

func TestMatchesWatchExactKey(t *testing.T) {
	if !matchesWatch("/foo", "/foo", "") {
		t.Fatal("exact watch should match the watched key")
	}
	if matchesWatch("/foobar", "/foo", "") {
		t.Fatal("exact watch must not match keys that merely share the prefix")
	}
}

func TestMatchesWatchRange(t *testing.T) {
	key := "/registry/pods/default/"
	rangeEnd := "/registry/pods/default0"

	if !matchesWatch("/registry/pods/default/pod-a", key, rangeEnd) {
		t.Fatal("range watch should match keys inside the half-open range")
	}
	if matchesWatch("/registry/pods/default0", key, rangeEnd) {
		t.Fatal("range watch must exclude rangeEnd itself")
	}
}

func TestMatchesWatchFromKeyOnwards(t *testing.T) {
	if !matchesWatch("/b", "/a", "\x00") {
		t.Fatal(`rangeEnd "\x00" should match keys after the start key`)
	}
	if matchesWatch("/0", "/a", "\x00") {
		t.Fatal(`rangeEnd "\x00" must not match keys before the start key`)
	}
}
