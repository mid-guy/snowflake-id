package snowflake

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	WorkerIDBits = 10
	SequenceBits = 12

	MaxWorkerID  = -1 ^ (-1 << WorkerIDBits)
	SequenceMask = -1 ^ (-1 << SequenceBits)

	workerIDShift  = SequenceBits
	timestampShift = SequenceBits + WorkerIDBits

	// DefaultEpoch is 2024-01-01 00:00:00 UTC in milliseconds.
	DefaultEpoch int64 = 1704067200000

	// maxBackwardDriftMs is the largest clock regression we will wait through
	// before refusing to generate an ID.
	maxBackwardDriftMs int64 = 5
)

var (
	ErrInvalidWorkerID = errors.New("snowflake: worker ID out of range")
	ErrClockBackwards  = errors.New("snowflake: clock moved backwards beyond tolerance")
)

// Clock returns the current time in milliseconds since the Unix epoch.
// Injected for testability.
type Clock func() int64

func defaultClock() int64 { return time.Now().UnixMilli() }

type Snowflake struct {
	mu            sync.Mutex
	workerID      int64
	epoch         int64
	clock         Clock
	lastTimestamp int64
	sequence      int64
}

type Option func(*Snowflake)

func WithEpoch(epochMs int64) Option {
	return func(s *Snowflake) { s.epoch = epochMs }
}

func WithClock(c Clock) Option {
	return func(s *Snowflake) { s.clock = c }
}

func New(workerID int64, opts ...Option) (*Snowflake, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, fmt.Errorf("%w: got %d, allowed 0..%d", ErrInvalidWorkerID, workerID, MaxWorkerID)
	}
	s := &Snowflake{
		workerID:      workerID,
		epoch:         DefaultEpoch,
		clock:         defaultClock,
		lastTimestamp: -1,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *Snowflake) WorkerID() int64 { return s.workerID }

func (s *Snowflake) NextID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timestamp := s.clock()

	if timestamp < s.lastTimestamp {
		drift := s.lastTimestamp - timestamp
		if drift > maxBackwardDriftMs {
			return 0, fmt.Errorf("%w: drift=%dms", ErrClockBackwards, drift)
		}
		time.Sleep(time.Duration(drift) * time.Millisecond)
		timestamp = s.clock()
		if timestamp < s.lastTimestamp {
			return 0, fmt.Errorf("%w: persistent drift", ErrClockBackwards)
		}
	}

	if timestamp == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & SequenceMask
		if s.sequence == 0 {
			for timestamp <= s.lastTimestamp {
				timestamp = s.clock()
			}
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = timestamp

	id := ((timestamp - s.epoch) << timestampShift) |
		(s.workerID << workerIDShift) |
		s.sequence
	return id, nil
}

// Decoded breaks an ID back into its components for inspection/debugging.
type Decoded struct {
	TimestampMs int64
	WorkerID    int64
	Sequence    int64
}

func (s *Snowflake) Decode(id int64) Decoded {
	return Decode(id, s.epoch)
}

func Decode(id, epoch int64) Decoded {
	return Decoded{
		TimestampMs: (id >> timestampShift) + epoch,
		WorkerID:    (id >> workerIDShift) & MaxWorkerID,
		Sequence:    id & SequenceMask,
	}
}
