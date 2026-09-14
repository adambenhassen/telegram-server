package store

import (
	"errors"
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

// NotificationMetricsSnapshot is the process-local rolling notification
// telemetry exposed to the admin metrics surface.
type NotificationMetricsSnapshot struct {
	WindowSeconds float64
	NotifyCount   int64
	RatePerSecond float64
	Channels      NotificationChannelCounts
	Invalid       int64
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
}

// NotificationMetrics counts valid notifications received by one process.
// The state is intentionally independent of Postgres and is reset by creating
// a new value at process startup.
type NotificationMetrics struct {
	now       func() time.Time
	startedAt time.Time
	buckets   [notificationBucketCount]notificationMetricBucket
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
	for i := range m.buckets {
		epoch, bucketCounts, ok := m.buckets[i].snapshot()
		if !ok {
			continue
		}
		if epoch <= cutoff || epoch > nowSecond {
			continue
		}
		for j, count := range bucketCounts {
			counts[j] += count
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
	}
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
	for !b.readers.CompareAndSwap(0, -1) {
		runtime.Gosched()
	}
	b.epoch.Store(second)
	for i := range b.counts {
		b.counts[i].Store(0)
	}
	b.readers.Store(0)
	b.writerPending.Store(false)
}

func (b *notificationMetricBucket) snapshot() (int64, [notificationCounterCount]int64, bool) {
	var counts [notificationCounterCount]int64
	for range notificationSnapshotTries {
		if b.writerPending.Load() {
			return notificationUnsetEpoch, counts, false
		}
		readers := b.readers.Load()
		if readers < 0 || !b.readers.CompareAndSwap(readers, readers+1) {
			runtime.Gosched()
			continue
		}
		if b.writerPending.Load() {
			b.readers.Add(-1)
			return notificationUnsetEpoch, counts, false
		}
		epoch := b.epoch.Load()
		for i := range b.counts {
			counts[i] = b.counts[i].Load()
		}
		b.readers.Add(-1)
		return epoch, counts, true
	}
	return notificationUnsetEpoch, counts, false
}
