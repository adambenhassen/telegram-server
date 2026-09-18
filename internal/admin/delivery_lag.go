package admin

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// DeliveryLagState describes whether the last complete live-connection lag
// sample is usable.
type DeliveryLagState string

const (
	DeliveryLagAvailable   DeliveryLagState = "available"
	DeliveryLagStale       DeliveryLagState = "stale"
	DeliveryLagUnavailable DeliveryLagState = "unavailable"
)

// DeliveryLagCoverage describes how many eligible connections had an
// authoritative account head in the current sampling attempt.
type DeliveryLagCoverage string

const (
	DeliveryLagCoverageFull    DeliveryLagCoverage = "full"
	DeliveryLagCoveragePartial DeliveryLagCoverage = "partial"
	DeliveryLagCoverageNone    DeliveryLagCoverage = "none"
)

// DeliveryLag is the fixed aggregate exposed by the authenticated admin
// metrics surfaces. It contains no account, connection, or other identifier.
type DeliveryLag struct {
	WorstPts            *int64              `json:"worst_pts"`
	State               DeliveryLagState    `json:"state"`
	Coverage            DeliveryLagCoverage `json:"coverage"`
	EligibleConnections int                 `json:"eligible_connections"`
	SampledConnections  int                 `json:"sampled_connections"`
	SampledAt           *time.Time          `json:"sampled_at"`
}

const deliveryLagSampleTimeout = time.Second

// deliveryLagState is the immutable state held by DeliveryLagSampler. Keeping
// only the last complete aggregate means a partial attempt cannot lower or
// replace the value represented by sampledAt.
type deliveryLagState struct {
	attempt             uint64
	hasFullSample       bool
	fullWorstPts        int64
	fullSampledAt       time.Time
	state               DeliveryLagState
	coverage            DeliveryLagCoverage
	eligibleConnections int
	sampledConnections  int
}

// DeliveryLagSampler samples live-connection lag and retains the last complete
// result across failed or partial attempts. Its state is process-local and
// aggregate only. Atomic immutable snapshots keep concurrent JSON and SSE
// collectors race-free without putting a lock on the delivery path.
type DeliveryLagSampler struct {
	now     func() time.Time
	attempt atomic.Uint64
	state   atomic.Pointer[deliveryLagState]
}

// NewDeliveryLagSampler creates a sampler using the system clock.
func NewDeliveryLagSampler() *DeliveryLagSampler {
	return NewDeliveryLagSamplerWithClock(time.Now)
}

// NewDeliveryLagSamplerWithClock creates a sampler with a supplied clock. The
// clock is useful for deterministic tests and is otherwise fixed for the
// sampler's lifetime.
func NewDeliveryLagSamplerWithClock(now func() time.Time) *DeliveryLagSampler {
	if now == nil {
		now = time.Now
	}
	s := &DeliveryLagSampler{now: now}
	s.state.Store(&deliveryLagState{
		state:    DeliveryLagUnavailable,
		coverage: DeliveryLagCoverageNone,
	})
	return s
}

func (s *DeliveryLagSampler) sample(ctx context.Context, registry *mtproto.SessionRegistry, st *store.Store) DeliveryLag {
	return s.sampleWithAccountHead(ctx, registry, st.AccountHead)
}

func (s *DeliveryLagSampler) sampleWithAccountHead(
	ctx context.Context,
	registry *mtproto.SessionRegistry,
	accountHead func(context.Context, int64) (int64, error),
) DeliveryLag {
	if ctx == nil {
		ctx = context.Background()
	}
	// The sampler owns its hard budget. A request that disconnects while the
	// sample is in flight must not publish a partial result into the process
	// shared state consumed by the other authenticated surfaces.
	sampleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deliveryLagSampleTimeout)
	defer cancel()

	attempt := s.attempt.Add(1)
	raw := registry.SampleDeliveryLag(sampleCtx, accountHead)
	var sampledAt time.Time
	if raw.Complete && raw.SampledConnections == raw.EligibleConnections {
		sampledAt = s.now().UTC()
	}
	return s.publish(attempt, raw, sampledAt)
}

func (s *DeliveryLagSampler) publish(attempt uint64, raw mtproto.DeliveryLagSample, sampledAt time.Time) DeliveryLag {
	coverage := deliveryLagCoverage(raw)
	full := coverage == DeliveryLagCoverageFull

	for {
		prior := s.state.Load()
		if prior != nil && prior.attempt > attempt {
			return deliveryLagResponse(prior)
		}

		next := &deliveryLagState{
			attempt:             attempt,
			hasFullSample:       full,
			state:               DeliveryLagUnavailable,
			coverage:            coverage,
			eligibleConnections: raw.EligibleConnections,
			sampledConnections:  raw.SampledConnections,
		}
		if full {
			next.fullWorstPts = raw.WorstPts
			next.fullSampledAt = sampledAt
			next.state = DeliveryLagAvailable
		} else if prior != nil {
			next.hasFullSample = prior.hasFullSample
			next.fullWorstPts = prior.fullWorstPts
			next.fullSampledAt = prior.fullSampledAt
			if prior.hasFullSample {
				next.state = DeliveryLagStale
			}
		}

		if s.state.CompareAndSwap(prior, next) {
			return deliveryLagResponse(next)
		}
	}
}

// Snapshot returns the most recently published lag state without sampling.
// It is used when a cached JSON or dashboard response needs the current state
// shared with an SSE collector.
func (s *DeliveryLagSampler) Snapshot() DeliveryLag {
	return deliveryLagResponse(s.state.Load())
}

func deliveryLagCoverage(sample mtproto.DeliveryLagSample) DeliveryLagCoverage {
	if !sample.Complete {
		if sample.SampledConnections == 0 {
			return DeliveryLagCoverageNone
		}
		return DeliveryLagCoveragePartial
	}
	switch sample.SampledConnections {
	case sample.EligibleConnections:
		return DeliveryLagCoverageFull
	case 0:
		return DeliveryLagCoverageNone
	default:
		return DeliveryLagCoveragePartial
	}
}

func deliveryLagResponse(state *deliveryLagState) DeliveryLag {
	if state == nil {
		return DeliveryLag{
			State:    DeliveryLagUnavailable,
			Coverage: DeliveryLagCoverageNone,
		}
	}

	response := DeliveryLag{
		State:               state.state,
		Coverage:            state.coverage,
		EligibleConnections: state.eligibleConnections,
		SampledConnections:  state.sampledConnections,
	}
	if state.hasFullSample {
		worst := state.fullWorstPts
		sampledAt := state.fullSampledAt
		response.WorstPts = &worst
		response.SampledAt = &sampledAt
	}
	return response
}
