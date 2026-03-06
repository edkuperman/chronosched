package dag

import "testing"

func TestDetectCycles_FindsCycle(t *testing.T) {
	edges := []Edge{{From: "A", To: "B"}, {From: "B", To: "C"}, {From: "C", To: "A"}}
	cycles := DetectCycles(edges)
	if len(cycles) != 1 {
		t.Fatalf("expected 1 cycle, got %d", len(cycles))
	}
	if len(cycles[0]) != 3 {
		t.Fatalf("unexpected cycle length: %#v", cycles[0])
	}
	want := map[string]bool{"A": true, "B": true, "C": true}
	for _, n := range cycles[0] {
		if !want[n] {
			t.Fatalf("unexpected cycle: %#v", cycles[0])
		}
	}
	if !HasCycle(edges) {
		t.Fatal("expected HasCycle to return true")
	}
}

func TestDetectCycles_AcyclicGraph(t *testing.T) {
	edges := []Edge{{From: "A", To: "B"}, {From: "B", To: "C"}, {From: "A", To: "D"}}
	if cycles := DetectCycles(edges); len(cycles) != 0 {
		t.Fatalf("expected no cycles, got %#v", cycles)
	}
	if HasCycle(edges) {
		t.Fatal("expected HasCycle to return false")
	}
}
