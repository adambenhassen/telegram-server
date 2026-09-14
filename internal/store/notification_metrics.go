package store

import (
	"errors"
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	notificationWindowSeconds = int64((time.Hour / time.Second))
	notificationBucketCount   = int(notificationWindowSeconds + 1)
	notificationCounterCount  = 10 // nine fixed channels plus invalid input
	notificationInvalidIndex  = notificationCounterCount - 1
	notificationUnsetEpoch    = int64(-1 << 63)
	notificationSnapshotTries = 4
	pushOutcomeCount          = 4
	pushLatencyFiniteBuckets  = 15
	pushLatencyBucketCount    = pushLatencyFiniteBuckets + 1
)

// PushOutcome is the fixed result set for one attempted persisted-update push.
// It is deliberately an enum rather than a string so an input or error cannot
// create a metric series.
type PushOutcome uint8

const (
	PushOutcomeSuccess PushOutcome = iota
	PushOutcomeOwnerMismatch
	PushOutcomeEncodeFailure
	PushOutcomeWriteFailure
)

// NotificationChannelCounts is the fixed per-channel distribution of valid
// notifications. Its fields deliberately mirror the compiled Postgres
// channel constants; no caller-supplied channel becomes a field or key.
type NotificationChannelCounts struct {
	Updates      int64
	Typing       int64
	Evict        int64
	ChannelPost  int64
	Encryption   int64
	Status       int64
	EncryptedMsg int64
	Reactions    int64
	Pinned       int64
}

// PushOutcomeCounts holds one count for every possible result of an attempted
// persisted-update push.
type PushOutcomeCounts struct {
	Success       int64
	OwnerMismatch int64
	EncodeFailure int64
	WriteFailure  int64
}

// PushMetricsSnapshot is the rolling process-local telemetry for persisted
// account-update pushes. Latency percentiles are upper bounds of fixed
// histogram buckets, not raw observations.
type PushMetricsSnapshot struct {
	WindowSeconds                        float64
	SampleCount                          int64
	P50Milliseconds                      float64
	P50Overflow                          bool
	P95Milliseconds                      float64
	P95Overflow                          bool
	Outcomes                             PushOutcomeCounts
	LatencyBucketUpperBoundsMilliseconds [pushLatencyFiniteBuckets]float64
	LatencyBucketCounts                  [pushLatencyBucketCount]int64
}

// NotificationMetricsSnapshot is the process-local rolling notification
// telemetry exposed to the admin metrics surface.
type NotificationMetricsSnapshot struct {
	WindowSeconds float64
	NotifyCount   int64
	RatePerSecond float64
	Channels      NotificationChannelCounts
	Invalid       int64
	Push          PushMetricsSnapshot
}

// notificationMetricBucket is one second of fixed counters. readers is a
// small reader gate: zero or more readers may increment counters, while -1
// exclusively owns the bucket during an epoch reset. writerPending prevents
// new snapshots from entering once a reset is waiting for active readers. It
// avoids a mutex on the notification hot path and makes resets exact under
// concurrent recording.
type notificationMetricBucket struct {
	epoch         atomic.Int64
	readers       atomic.Int32
	writerPending atomic.Bool
	counts        [notificationCounterCount]atomic.Int64
	pushOutcomes  [pushOutcomeCount]atomic.Int64
	latencies     [pushLatencyBucketCount]atomic.Int64
}

// NotificationMetrics counts valid notifications received by one process.
// The state is intentionally independent of Postgres and is reset by creating
// a new value at process startup.
type NotificationMetrics struct {
	now               func() time.Time
	startedAt         time.Time
	buckets           [notificationBucketCount]notificationMetricBucket
	beforePushLatency func()
}

// NewNotificationMetrics creates an empty process-local notification
// recorder using time.Now as its clock.
func NewNotificationMetrics() *NotificationMetrics {
	return NewNotificationMetricsWithClock(time.Now)
}

// NewNotificationMetricsWithClock creates an empty recorder with a supplied
// clock. The clock is a test seam; production uses NewNotificationMetrics.
func NewNotificationMetricsWithClock(now func() time.Time) *NotificationMetrics {
	if now == nil {
		now = time.Now
	}
	m := &NotificationMetrics{
		now:       now,
		startedAt: now(),
	}
	for i := range m.buckets {
		m.buckets[i].epoch.Store(notificationUnsetEpoch)
	}
	return m
}

var errUnknownNotificationChannel = errors.New("unknown notification channel")

// RecordValidNotification records one successfully parsed notification. An
// unknown channel is treated as invalid and is never assigned a dynamic
// series.
func (m *NotificationMetrics) RecordValidNotification(channel string) error {
	index := notificationChannelIndex(channel)
	if index < 0 {
		if err := m.RecordInvalidNotification(); err != nil {
			return err
		}
		return errUnknownNotificationChannel
	}
	m.record(index)
	return nil
}

// RecordInvalidNotification records one malformed or unknown notification
// without retaining its channel or payload.
func (m *NotificationMetrics) RecordInvalidNotification() error {
	if m == nil {
		return nil
	}
	m.record(notificationInvalidIndex)
	return nil
}

// RecordPushOutcome records one result for an attempted persisted-update push.
// Successful attempts also record the elapsed time from acceptedAt through the
// completion of PushTo. The timestamp and latency stay in process memory.
func (m *NotificationMetrics) RecordPushOutcome(outcome PushOutcome, acceptedAt time.Time) {
	if m == nil {
		return
	}
	if outcome >= pushOutcomeCount {
		outcome = PushOutcomeWriteFailure
	}

	now := m.now()
	latency := time.Duration(0)
	if !acceptedAt.IsZero() {
		latency = max(now.Sub(acceptedAt), 0)
	}
	second := now.Unix()
	bucket := &m.buckets[notificationBucketIndex(second)]
	beforePushLatency := m.beforePushLatency
	for {
		if bucket.epoch.Load() != second {
			bucket.reset(second)
			continue
		}
		if bucket.writerPending.Load() {
			runtime.Gosched()
			continue
		}

		readers := bucket.readers.Load()
		if readers < 0 || !bucket.readers.CompareAndSwap(readers, readers+1) {
			runtime.Gosched()
			continue
		}
		if bucket.writerPending.Load() || bucket.epoch.Load() != second {
			bucket.readers.Add(-1)
			continue
		}
		if outcome == PushOutcomeSuccess {
			// The latency bucket is the single atomic source for successful
			// samples; snapshots derive both success and sample count from it.
			if beforePushLatency != nil {
				beforePushLatency()
			}
			bucket.latencies[pushLatencyBucketIndex(latency)].Add(1)
		} else {
			bucket.pushOutcomes[outcome].Add(1)
		}
		bucket.readers.Add(-1)
		return
	}
}

// Snapshot returns the current rolling-hour notification counters. The window
// begins when the recorder is constructed, is capped at one hour, and has no
// values before process startup.
func (m *NotificationMetrics) Snapshot() NotificationMetricsSnapshot {
	if m == nil {
		return NotificationMetricsSnapshot{}
	}
	now := m.now()
	nowSecond := now.Unix()
	cutoff := nowSecond - notificationWindowSeconds

	var counts [notificationCounterCount]int64
	var pushOutcomes [pushOutcomeCount]int64
	var latencyBuckets [pushLatencyBucketCount]int64
	for i := range m.buckets {
		epoch, bucketCounts, bucketOutcomes, bucketLatencies, ok := m.buckets[i].snapshot()
		if !ok {
			continue
		}
		if epoch <= cutoff || epoch > nowSecond {
			continue
		}
		for j, count := range bucketCounts {
			counts[j] += count
		}
		for j, count := range bucketOutcomes {
			pushOutcomes[j] += count
		}
		for j, count := range bucketLatencies {
			latencyBuckets[j] += count
		}
	}

	window := now.Sub(m.startedAt)
	window = max(window, 0)
	window = min(window, time.Hour)
	windowSeconds := window.Seconds()
	notifyCount := int64(0)
	for i := range notificationInvalidIndex {
		notifyCount += counts[i]
	}
	rate := float64(0)
	if windowSeconds > 0 {
		rate = float64(notifyCount) / windowSeconds
	}

	return NotificationMetricsSnapshot{
		WindowSeconds: windowSeconds,
		NotifyCount:   notifyCount,
		RatePerSecond: rate,
		Channels: NotificationChannelCounts{
			Updates:      counts[0],
			Typing:       counts[1],
			Evict:        counts[2],
			ChannelPost:  counts[3],
			Encryption:   counts[4],
			Status:       counts[5],
			EncryptedMsg: counts[6],
			Reactions:    counts[7],
			Pinned:       counts[8],
		},
		Invalid: counts[notificationInvalidIndex],
		Push: PushMetricsSnapshot{
			WindowSeconds:   windowSeconds,
			SampleCount:     pushOutcomes[PushOutcomeSuccess],
			P50Milliseconds: percentileMilliseconds(latencyBuckets, 50),
			P50Overflow:     percentileOverflow(latencyBuckets, 50),
			P95Milliseconds: percentileMilliseconds(latencyBuckets, 95),
			P95Overflow:     percentileOverflow(latencyBuckets, 95),
			Outcomes: PushOutcomeCounts{
				Success:       pushOutcomes[PushOutcomeSuccess],
				OwnerMismatch: pushOutcomes[PushOutcomeOwnerMismatch],
				EncodeFailure: pushOutcomes[PushOutcomeEncodeFailure],
				WriteFailure:  pushOutcomes[PushOutcomeWriteFailure],
			},
			LatencyBucketUpperBoundsMilliseconds: pushLatencyBucketUpperBoundsMilliseconds,
			LatencyBucketCounts:                  latencyBuckets,
		},
	}
}

// pushLatencyBucketBounds are the upper bounds of the fixed latency buckets.
// Durations above the final bound are retained in the final overflow bucket.
var pushLatencyBucketBounds = [...]time.Duration{
	time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	time.Minute,
}

var pushLatencyBucketUpperBoundsMilliseconds = [...]float64{
	1,
	2,
	5,
	10,
	20,
	50,
	100,
	200,
	500,
	1000,
	2000,
	5000,
	10000,
	30000,
	60000,
}

// PushLatencyBucketUpperBoundsMilliseconds returns the fixed finite histogram
// bounds. The final bucket is the overflow bucket above the last bound.
func PushLatencyBucketUpperBoundsMilliseconds() [pushLatencyFiniteBuckets]float64 {
	return pushLatencyBucketUpperBoundsMilliseconds
}

func pushLatencyBucketIndex(latency time.Duration) int {
	for i, bound := range pushLatencyBucketBounds {
		if latency <= bound {
			return i
		}
	}
	return len(pushLatencyBucketBounds)
}

func percentileMilliseconds(buckets [pushLatencyBucketCount]int64, percentile int64) float64 {
	index, ok := percentileBucketIndex(buckets, percentile)
	if !ok {
		return 0
	}
	if index == pushLatencyFiniteBuckets {
		return pushLatencyBucketUpperBoundsMilliseconds[pushLatencyFiniteBuckets-1]
	}
	return pushLatencyBucketUpperBoundsMilliseconds[index]
}

func percentileOverflow(buckets [pushLatencyBucketCount]int64, percentile int64) bool {
	index, ok := percentileBucketIndex(buckets, percentile)
	return ok && index == pushLatencyFiniteBuckets
}

func percentileBucketIndex(buckets [pushLatencyBucketCount]int64, percentile int64) (int, bool) {
	var total int64
	for _, count := range buckets {
		total += count
	}
	if total == 0 {
		return 0, false
	}
	rank := int64(math.Ceil(float64(total) * float64(percentile) / 100))
	rank = max(rank, 1)
	var seen int64
	for i, count := range buckets {
		seen += count
		if seen >= rank {
			return i, true
		}
	}
	return pushLatencyFiniteBuckets, true
}

func notificationChannelIndex(channel string) int {
	switch channel {
	case ChannelUpdates:
		return 0
	case ChannelTyping:
		return 1
	case ChannelEvict:
		return 2
	case ChannelPost:
		return 3
	case ChannelEncryption:
		return 4
	case ChannelStatus:
		return 5
	case ChannelEncryptedMsg:
		return 6
	case ChannelReactions:
		return 7
	case ChannelPinned:
		return 8
	default:
		return -1
	}
}

func (m *NotificationMetrics) record(index int) {
	second := m.now().Unix()
	bucket := &m.buckets[notificationBucketIndex(second)]
	for {
		if bucket.epoch.Load() != second {
			bucket.reset(second)
			continue
		}
		if bucket.writerPending.Load() {
			runtime.Gosched()
			continue
		}

		readers := bucket.readers.Load()
		if readers < 0 || !bucket.readers.CompareAndSwap(readers, readers+1) {
			runtime.Gosched()
			continue
		}
		if bucket.writerPending.Load() || bucket.epoch.Load() != second {
			bucket.readers.Add(-1)
			continue
		}
		bucket.counts[index].Add(1)
		bucket.readers.Add(-1)
		return
	}
}

func notificationBucketIndex(second int64) int {
	index := second % int64(notificationBucketCount)
	if index < 0 {
		index += int64(notificationBucketCount)
	}
	return int(index)
}

func (b *notificationMetricBucket) reset(second int64) {
	if !b.writerPending.CompareAndSwap(false, true) {
		runtime.Gosched()
		return
	}
	// The caller may have observed a stale epoch before another recorder
	// finished initializing this same second. Rechecking after taking the
	// writer flag prevents that stale reset from clearing live counters.
	if b.epoch.Load() == second {
		b.writerPending.Store(false)
		return
	}
	for !b.readers.CompareAndSwap(0, -1) {
		runtime.Gosched()
	}
	b.epoch.Store(second)
	for i := range b.counts {
		b.counts[i].Store(0)
	}
	for i := range b.pushOutcomes {
		b.pushOutcomes[i].Store(0)
	}
	for i := range b.latencies {
		b.latencies[i].Store(0)
	}
	b.readers.Store(0)
	b.writerPending.Store(false)
}

func (b *notificationMetricBucket) snapshot() (int64, [notificationCounterCount]int64, [pushOutcomeCount]int64, [pushLatencyBucketCount]int64, bool) {
	var counts [notificationCounterCount]int64
	var outcomes [pushOutcomeCount]int64
	var latencies [pushLatencyBucketCount]int64
	for range notificationSnapshotTries {
		if b.writerPending.Load() {
			return notificationUnsetEpoch, counts, outcomes, latencies, false
		}
		readers := b.readers.Load()
		if readers < 0 || !b.readers.CompareAndSwap(readers, readers+1) {
			runtime.Gosched()
			continue
		}
		if b.writerPending.Load() {
			b.readers.Add(-1)
			return notificationUnsetEpoch, counts, outcomes, latencies, false
		}
		epoch := b.epoch.Load()
		for i := range b.counts {
			counts[i] = b.counts[i].Load()
		}
		for i := range b.pushOutcomes {
			outcomes[i] = b.pushOutcomes[i].Load()
		}
		for i := range b.latencies {
			latencies[i] = b.latencies[i].Load()
		}
		var successCount int64
		for _, count := range latencies {
			successCount += count
		}
		outcomes[PushOutcomeSuccess] = successCount
		b.readers.Add(-1)
		return epoch, counts, outcomes, latencies, true
	}
	return notificationUnsetEpoch, counts, outcomes, latencies, false
}
