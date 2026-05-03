package snowflake

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNew_RejectsOutOfRangeWorkerID(t *testing.T) {
	cases := []int64{-1, MaxWorkerID + 1, 1 << 20}
	for _, w := range cases {
		if _, err := New(w); !errors.Is(err, ErrInvalidWorkerID) {
			t.Errorf("worker=%d: expected ErrInvalidWorkerID, got %v", w, err)
		}
	}
}

func TestNextID_MonotonicAndUnique(t *testing.T) {
	sf, err := New(1)
	if err != nil {
		t.Fatal(err)
	}

	const n = 50_000
	seen := make(map[int64]struct{}, n)
	var prev int64
	for i := 0; i < n; i++ {
		id, err := sf.NextID()
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id: %d", id)
		}
		seen[id] = struct{}{}
		if id <= prev {
			t.Fatalf("non-monotonic: prev=%d cur=%d", prev, id)
		}
		prev = id
	}
}

func TestNextID_ConcurrentUnique(t *testing.T) {
	sf, err := New(42)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const perG = 5_000
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[int64]struct{}, goroutines*perG)
	var dupes int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				id, err := sf.NextID()
				if err != nil {
					t.Errorf("NextID: %v", err)
					return
				}
				mu.Lock()
				if _, ok := seen[id]; ok {
					atomic.AddInt64(&dupes, 1)
				}
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if dupes > 0 {
		t.Fatalf("found %d duplicate ids across goroutines", dupes)
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	const wid int64 = 123
	sf, err := New(wid)
	if err != nil {
		t.Fatal(err)
	}
	id, err := sf.NextID()
	if err != nil {
		t.Fatal(err)
	}
	d := sf.Decode(id)
	if d.WorkerID != wid {
		t.Errorf("worker mismatch: got %d want %d", d.WorkerID, wid)
	}
	if d.Sequence < 0 || d.Sequence > SequenceMask {
		t.Errorf("sequence out of range: %d", d.Sequence)
	}
}

func TestNextID_ClockBackwardsRefusedWhenLargeDrift(t *testing.T) {
	now := int64(1_800_000_000_000)
	clk := func() int64 { return now }

	sf, err := New(1, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sf.NextID(); err != nil {
		t.Fatal(err)
	}

	// Jump backwards by 100ms — beyond tolerance.
	now -= 100
	if _, err := sf.NextID(); !errors.Is(err, ErrClockBackwards) {
		t.Fatalf("expected ErrClockBackwards, got %v", err)
	}
}

func TestNextID_SequenceOverflowAdvancesTimestamp(t *testing.T) {
	t.Parallel()
	now := int64(1_800_000_000_000)
	advance := false
	clk := func() int64 {
		if advance {
			now++
		}
		return now
	}
	sf, err := New(1, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}

	// Fill the sequence in a single ms.
	for i := 0; i <= SequenceMask; i++ {
		if _, err := sf.NextID(); err != nil {
			t.Fatal(err)
		}
	}
	// Next call must wait for the next ms — flip clock to advance.
	advance = true
	id, err := sf.NextID()
	if err != nil {
		t.Fatal(err)
	}
	d := sf.Decode(id)
	if d.TimestampMs <= 1_800_000_000_000 {
		t.Errorf("timestamp did not advance: %d", d.TimestampMs)
	}
}

func BenchmarkNextID(b *testing.B) {
	sf, _ := New(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sf.NextID()
	}
}
