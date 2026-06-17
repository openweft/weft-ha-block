// e2e_failover_test.go is the end-to-end proof of the whole control + data
// plane against a REAL (embedded) etcd: two nodes share a synchronous replica
// set; node-a leads and writes; node-a dies (lease lost); node-b wins, FENCES
// node-a, and only then writes. It asserts the safety properties end-to-end —
// the lost-lease demotion stops the old writer, fence-before-promote runs before
// the new writer activates, the fenced node cannot write (no split-brain), and
// the replicas stay byte-identical with both writers' data.

package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-volumes/replica"
	replicaha "github.com/go-volumes/replica-ha"
)

func (f *fakeFencer) fencedCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.fenced...)
}

func dumpDevice(t *testing.T, d *memDevice, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := d.ReadAt(b, 0); err != nil {
		t.Fatalf("ReadAt dump: %v", err)
	}
	return b
}

func TestE2E_TwoNodeFencedFailover(t *testing.T) {
	const devSize = 8192
	endpoint := startEmbeddedEtcd(t)

	// Two shared synchronous replicas. BOTH nodes' engines write to the SAME two
	// devices, so the post-failover contents directly prove consistency and the
	// absence of split-brain. MinInSync=2 → every committed write is on both.
	r0, r1 := newMemDevice(devSize), newMemDevice(devSize)
	mkEngine := func() *replica.Engine {
		eng, err := replica.New(
			[]replica.Replica{{Name: "r0", Dev: r0}, {Name: "r1", Dev: r1}},
			replica.Config{MinInSync: 2},
		)
		if err != nil {
			t.Fatalf("engine: %v", err)
		}
		return eng
	}

	startNode := func(node string, fencer *fakeFencer) *replicaha.ActiveDevice {
		coord := newCoord(t, endpoint, "vol-shared", node, 3)
		t.Cleanup(func() { _ = coord.Close() })
		ctrl, dev, err := replicaha.New(mkEngine(), coord, fencer, quietLog())
		if err != nil {
			t.Fatalf("controller %s: %v", node, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = ctrl.Run(ctx); close(done) }()
		t.Cleanup(func() { cancel(); <-done })
		return dev
	}

	// --- node-a comes up first and must become the active writer ---
	fencerA := &fakeFencer{}
	coordA := newCoord(t, endpoint, "vol-shared", "node-a", 3)
	ctrlA, devA, err := replicaha.New(mkEngine(), coordA, fencerA, quietLog())
	if err != nil {
		t.Fatalf("controller node-a: %v", err)
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() { _ = ctrlA.Run(ctxA); close(doneA) }()
	t.Cleanup(func() { cancelA(); <-doneA })

	if !waitActive(devA, true, 12*time.Second) {
		t.Fatal("node-a never became the active writer")
	}
	if got := fencerA.fencedCopy(); len(got) != 0 {
		t.Fatalf("the first leader must fence nobody, fenced=%v", got)
	}
	d1 := []byte("DATA-FROM-NODE-A")
	if _, err := devA.WriteAt(d1, 0); err != nil {
		t.Fatalf("node-a WriteAt: %v", err)
	}

	// --- node-b joins as a follower while node-a holds the lease ---
	fencerB := &fakeFencer{}
	devB := startNode("node-b", fencerB)

	// It must settle passive and reject writes — no two active writers.
	if waitActive(devB, true, 2*time.Second) {
		t.Fatal("node-b became active while node-a holds the lease — split-brain")
	}
	if _, err := devB.WriteAt([]byte("nope"), 0); err != replicaha.ErrNotLeader {
		t.Fatalf("follower node-b WriteAt = %v; want ErrNotLeader", err)
	}

	// --- FAILOVER: node-a dies (its lease is lost) ---
	if err := coordA.Close(); err != nil {
		t.Fatalf("close node-a coordinator: %v", err)
	}
	// node-a's lost lease surfaces as IsSelf=false → it demotes and stops writing.
	if !waitActive(devA, false, 15*time.Second) {
		t.Fatal("node-a still writable after losing its lease")
	}

	// node-b wins, FENCES node-a, and only then activates.
	if !waitActive(devB, true, 20*time.Second) {
		t.Fatal("node-b never took over after node-a died")
	}
	if got := fencerB.fencedCopy(); !contains(got, "node-a") {
		t.Fatalf("node-b must fence node-a before promoting (no split-brain), fenced=%v", got)
	}
	// The fenced old leader must reject writes.
	if _, err := devA.WriteAt([]byte("zombie"), 0); err != replicaha.ErrNotLeader {
		t.Fatalf("fenced node-a WriteAt = %v; want ErrNotLeader", err)
	}
	d2 := []byte("DATA-FROM-NODE-B")
	if _, err := devB.WriteAt(d2, 1000); err != nil {
		t.Fatalf("node-b WriteAt after takeover: %v", err)
	}

	// --- consistency: both replicas hold A's and B's data, byte-identical ---
	if !bytes.Equal(dumpDevice(t, r0, devSize), dumpDevice(t, r1, devSize)) {
		t.Fatal("replicas diverged after the fenced failover")
	}
	if got := dumpDevice(t, r0, len(d1)); !bytes.Equal(got, d1) {
		t.Fatalf("node-a's pre-failover write was lost: %q", got)
	}
	got2 := make([]byte, len(d2))
	if _, err := r0.ReadAt(got2, 1000); err != nil || !bytes.Equal(got2, d2) {
		t.Fatalf("node-b's post-failover write was lost: %q (err %v)", got2, err)
	}
}
