package store

import (
	"errors"
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	notificationWindowSeconds   = int64((time.Hour / time.Second))
	notificationBucketCount     = int(notificationWindowSeconds + 1)
	notificationCounterCount    = 10 // nine fixed channels plus invalid input
	notificationInvalidIndex    = notificationCounterCount - 1
	notificationUnsetEpoch      = int64(-1 << 63)
	notificationSnapshotTries   = 4
	pushOutcomeCount            = 4
	pushLatencyFiniteBuckets    = 15
	pushLatencyBucketCount      = pushLatencyFiniteBuckets + 1
	rateLimitDenialSurfaceCount = 20 // nineteen fixed surfaces plus dropped
	rateLimitDenialDroppedIndex = rateLimitDenialSurfaceCount - 1
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

// RecorderFailureCategory is the closed set of telemetry recorder failures.
// It identifies the recorder family only; no original error or request value
// can become a metric dimension.
type RecorderFailureCategory uint8

const (
	RecorderFailureRateLimitDenial RecorderFailureCategory = iota
	RecorderFailurePushOutcome
	RecorderFailureNotification
	recorderFailureCategoryCount
)

func (c RecorderFailureCategory) String() string {
	switch c {
	case RecorderFailureRateLimitDenial:
		return "rate_limit_denial"
	case RecorderFailurePushOutcome:
		return "push_outcome"
	case RecorderFailureNotification:
		return "notification"
	default:
		return "unknown"
	}
}

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

// RateLimitDenialSurfaceCounts holds one count for every fixed client-visible
// rate-limit surface. Its field set is deliberately closed so an internal
// surface name or error cannot create metric cardinality.
type RateLimitDenialSurfaceCounts struct {
	MessageSend               int64
	CreateChat                int64
	AddChatUser               int64
	CreateChannel             int64
	MessagesSearch            int64
	ContactsSearch            int64
	MessagesSearchGlobal      int64
	SaveFilePart              int64
	UploadGetFile             int64
	SendCodeIPCalls           int64
	SendCodeIPDistinctNumbers int64
	SignInFailIP              int64
	CheckPassword             int64
	CheckPasswordIP           int64
	GetPasswordIP             int64
	SignUpIP                  int64
	PasswordProof             int64
	GetPassword               int64
	UpdateProfile             int64
}

// PushOutcomeCounts holds one count for every possible result of an attempted
// persisted-update push.
type PushOutcomeCounts struct {
	Success       int64
	OwnerMismatch int64
	EncodeFailure int64
	WriteFailure  int64
}

// RecorderFailureCounts holds one count for every fixed telemetry recorder
// family. It is deliberately aggregate-only: errors, payloads, and identities
// never enter the snapshot.
type RecorderFailureCounts struct {
	RateLimitDenial int64
	PushOutcome     int64
	Notification    int64
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

// RateLimitDenialMetricsSnapshot is the process-local rolling telemetry for
// requests that actually returned FLOOD_WAIT from a fixed rate-limit surface.
// Dropped contains only denials whose internal surface was not in the fixed
// contract; it is intentionally excluded from Count and BySurface.
type RateLimitDenialMetricsSnapshot struct {
	WindowSeconds float64
	Count         int64
	RatePerSecond float64
	BySurface     RateLimitDenialSurfaceCounts
	Dropped       int64
}

// NotificationMetricsSnapshot is the process-local rolling notification
// telemetry exposed to the admin metrics surface.
type NotificationMetricsSnapshot struct {
	WindowSeconds    float64
	NotifyCount      int64
	RatePerSecond    float64
	Channels         NotificationChannelCounts
	Invalid          int64
	Push             PushMetricsSnapshot
	RateLimitDenials RateLimitDenialMetricsSnapshot
	RecorderFailures RecorderFailureCounts
}

// notificationMetricBucket is one second of fixed counters. readers is a
// small reader gate: zero or more readers may increment counters, while -1
// exclusively owns the bucket during an epoch reset. writerPending prevents
// new snapshots from entering once a reset is waiting for active readers. It
// avoids a mutex on the notification hot path and makes resets exact under
// concurrent recording.
type notificationMetricBucket struct {
	epoch            atomic.Int64
	readers          atomic.Int32
	writerPending    atomic.Bool
	counts           [notificationCounterCount]atomic.Int64
	rateLimitDenials [rateLimitDenialSurfaceCount]atomic.Int64
	pushOutcomes     [pushOutcomeCount]atomic.Int64
	latencies        [pushLatencyBucketCount]atomic.Int64
	recorderFailures [recorderFailureCategoryCount]atomic.Int64
}

// NotificationMetrics counts valid notifications received by one process.
// The state is intentionally independent of Postgres and is reset by creating
// a new value at process startup.
type NotificationMetrics struct {
	now                        func() time.Time
	startedAt                  time.Time
	buckets                    [notificationBucketCount]notificationMetricBucket
	beforePushLatency          func()
	recorderFailureLogSamplers [recorderFailureCategoryCount]recorderFailureLogSampler
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
	if m == nil {
		return nil
	}
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

// RecordRateLimitDenial records one client-visible FLOOD_WAIT from a fixed
// rate-limit surface. Unknown surfaces are counted only in the bounded dropped
// counter and never retain the supplied name.
func (m *NotificationMetrics) RecordRateLimitDenial(surface string) {
	if err := m.RecordRateLimitDenialResult(surface); err != nil {
		return
	}
}

// RecordRateLimitDenialResult is the error-reporting form used by the request
// boundary so recorder failures can be surfaced without changing the legacy
// no-result convenience method.
func (m *NotificationMetrics) RecordRateLimitDenialResult(surface string) error {
	if m == nil {
		return nil
	}
	index := rateLimitDenialSurfaceIndex(surface)
	if index < 0 {
		index = rateLimitDenialDroppedIndex
	}
	m.recordRateLimitDenial(index)
	return nil
}

// RecordPushOutcome records one result for an attempted persisted-update push.
// Successful attempts also record the elapsed time from acceptedAt through the
// completion of PushTo. The timestamp and latency stay in process memory.
func (m *NotificationMetrics) RecordPushOutcome(outcome PushOutcome, acceptedAt time.Time) {
	if err := m.RecordPushOutcomeResult(outcome, acceptedAt); err != nil {
		return
	}
}

// RecordPushOutcomeResult is the error-reporting form used by the delivery
// boundary so recorder failures can be surfaced without changing the legacy
// no-result convenience method.
func (m *NotificationMetrics) RecordPushOutcomeResult(outcome PushOutcome, acceptedAt time.Time) error {
	if m == nil {
		return nil
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
		return nil
	}
}

// RecordRecorderFailure records one fixed-category failure of a telemetry
// recorder. Failure accounting is itself best effort and never invokes a test
// hook or retains the failure value.
func (m *NotificationMetrics) RecordRecorderFailure(category RecorderFailureCategory) {
	if m == nil || category >= recorderFailureCategoryCount {
		return
	}
	m.recordRecorderFailure(int(category), m.recorderFailureSecond())
}

// recorderFailureSecond uses the current clock when possible, but falls back to
// process start if the recorder's own clock is the failing operation. That lets
// a recorder panic still produce its bounded failure signal.
func (m *NotificationMetrics) recorderFailureSecond() (second int64) {
	second = m.startedAt.Unix()
	if m.now == nil {
		return second
	}
	defer func() {
		if recover() != nil {
			second = m.startedAt.Unix()
		}
	}()
	return m.now().Unix()
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
	var rateLimitDenials [rateLimitDenialSurfaceCount]int64
	var pushOutcomes [pushOutcomeCount]int64
	var latencyBuckets [pushLatencyBucketCount]int64
	var recorderFailures [recorderFailureCategoryCount]int64
	for i := range m.buckets {
		epoch, bucketCounts, bucketDenials, bucketOutcomes, bucketLatencies, bucketFailures, ok := m.buckets[i].snapshot()
		if !ok {
			continue
		}
		if epoch <= cutoff || epoch > nowSecond {
			continue
		}
		for j, count := range bucketCounts {
			counts[j] += count
		}
		for j, count := range bucketDenials {
			rateLimitDenials[j] += count
		}
		for j, count := range bucketOutcomes {
			pushOutcomes[j] += count
		}
		for j, count := range bucketLatencies {
			latencyBuckets[j] += count
		}
		for j, count := range bucketFailures {
			recorderFailures[j] += count
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
	denialCount := int64(0)
	for i := range rateLimitDenialDroppedIndex {
		denialCount += rateLimitDenials[i]
	}
	denialRate := float64(0)
	if windowSeconds > 0 {
		denialRate = float64(denialCount) / windowSeconds
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
		RateLimitDenials: RateLimitDenialMetricsSnapshot{
			WindowSeconds: windowSeconds,
			Count:         denialCount,
			RatePerSecond: denialRate,
			BySurface: RateLimitDenialSurfaceCounts{
				MessageSend:               rateLimitDenials[0],
				CreateChat:                rateLimitDenials[1],
				AddChatUser:               rateLimitDenials[2],
				CreateChannel:             rateLimitDenials[3],
				MessagesSearch:            rateLimitDenials[4],
				ContactsSearch:            rateLimitDenials[5],
				MessagesSearchGlobal:      rateLimitDenials[6],
				SaveFilePart:              rateLimitDenials[7],
				UploadGetFile:             rateLimitDenials[8],
				SendCodeIPCalls:           rateLimitDenials[9],
				SendCodeIPDistinctNumbers: rateLimitDenials[10],
				SignInFailIP:              rateLimitDenials[11],
				CheckPassword:             rateLimitDenials[12],
				CheckPasswordIP:           rateLimitDenials[13],
				GetPasswordIP:             rateLimitDenials[14],
				SignUpIP:                  rateLimitDenials[15],
				PasswordProof:             rateLimitDenials[16],
				GetPassword:               rateLimitDenials[17],
				UpdateProfile:             rateLimitDenials[18],
			},
			Dropped: rateLimitDenials[rateLimitDenialDroppedIndex],
		},
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
		RecorderFailures: RecorderFailureCounts{
			RateLimitDenial: recorderFailures[RecorderFailureRateLimitDenial],
			PushOutcome:     recorderFailures[RecorderFailurePushOutcome],
			Notification:    recorderFailures[RecorderFailureNotification],
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

func rateLimitDenialSurfaceIndex(surface string) int {
	switch surface {
	case "message_send":
		return 0
	case "create_chat":
		return 1
	case "add_chat_user":
		return 2
	case "create_channel":
		return 3
	case "messages_search":
		return 4
	case "contacts_search":
		return 5
	case "messages_search_global":
		return 6
	case "save_file_part":
		return 7
	case "upload_get_file":
		return 8
	case "send_code_ip_calls":
		return 9
	case "send_code_ip_distinct_numbers":
		return 10
	case "sign_in_fail_ip":
		return 11
	case "check_password":
		return 12
	case "check_password_ip":
		return 13
	case "get_password_ip":
		return 14
	case "sign_up_ip":
		return 15
	case "password_proof":
		return 16
	case "get_password":
		return 17
	case "update_profile":
		return 18
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

func (m *NotificationMetrics) recordRateLimitDenial(index int) {
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
		bucket.rateLimitDenials[index].Add(1)
		bucket.readers.Add(-1)
		return
	}
}

func (m *NotificationMetrics) recordRecorderFailure(index int, second int64) {
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
		bucket.recorderFailures[index].Add(1)
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
	for i := range b.rateLimitDenials {
		b.rateLimitDenials[i].Store(0)
	}
	for i := range b.pushOutcomes {
		b.pushOutcomes[i].Store(0)
	}
	for i := range b.latencies {
		b.latencies[i].Store(0)
	}
	for i := range b.recorderFailures {
		b.recorderFailures[i].Store(0)
	}
	b.readers.Store(0)
	b.writerPending.Store(false)
}

func (b *notificationMetricBucket) snapshot() (int64, [notificationCounterCount]int64, [rateLimitDenialSurfaceCount]int64, [pushOutcomeCount]int64, [pushLatencyBucketCount]int64, [recorderFailureCategoryCount]int64, bool) {
	var counts [notificationCounterCount]int64
	var denials [rateLimitDenialSurfaceCount]int64
	var outcomes [pushOutcomeCount]int64
	var latencies [pushLatencyBucketCount]int64
	var failures [recorderFailureCategoryCount]int64
	for range notificationSnapshotTries {
		if b.writerPending.Load() {
			return notificationUnsetEpoch, counts, denials, outcomes, latencies, failures, false
		}
		readers := b.readers.Load()
		if readers < 0 || !b.readers.CompareAndSwap(readers, readers+1) {
			runtime.Gosched()
			continue
		}
		if b.writerPending.Load() {
			b.readers.Add(-1)
			return notificationUnsetEpoch, counts, denials, outcomes, latencies, failures, false
		}
		epoch := b.epoch.Load()
		for i := range b.counts {
			counts[i] = b.counts[i].Load()
		}
		for i := range b.rateLimitDenials {
			denials[i] = b.rateLimitDenials[i].Load()
		}
		for i := range b.pushOutcomes {
			outcomes[i] = b.pushOutcomes[i].Load()
		}
		for i := range b.latencies {
			latencies[i] = b.latencies[i].Load()
		}
		for i := range b.recorderFailures {
			failures[i] = b.recorderFailures[i].Load()
		}
		var successCount int64
		for _, count := range latencies {
			successCount += count
		}
		outcomes[PushOutcomeSuccess] = successCount
		b.readers.Add(-1)
		return epoch, counts, denials, outcomes, latencies, failures, true
	}
	return notificationUnsetEpoch, counts, denials, outcomes, latencies, failures, false
}
