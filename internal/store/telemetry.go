package store

import (
	"log/slog"
	"sync/atomic"
	"time"
)

const recorderFailureLogInterval = 10 * time.Second

// recorderFailureLogSampler keeps a recorder outage from turning its own
// diagnostic into a log flood. The category is fixed by the caller, so one
// sampler per category is enough and no request-derived state is retained.
type recorderFailureLogSampler struct {
	last atomic.Int64
}

func (s *recorderFailureLogSampler) allow(now time.Time) bool {
	n := now.UnixNano()
	last := s.last.Load()
	if last != 0 && n-last < int64(recorderFailureLogInterval) {
		return false
	}
	return s.last.CompareAndSwap(last, n)
}

var fallbackRecorderFailureLogSamplers [recorderFailureCategoryCount]recorderFailureLogSampler

// InvokeRecorder runs one fixed telemetry write and converts either an error
// return or a panic into the same failure result. Callers keep the recorder
// outside their functional result path.
func InvokeRecorder(call func() error) (failed bool) {
	if call == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			failed = true
		}
	}()
	return call() != nil
}

// ReportRecorderFailure records and logs a fixed recorder-family failure. Both
// the signal and logger are defensive boundaries: telemetry must not turn its
// own failure into a request, delivery, or shutdown failure.
func ReportRecorderFailure(log *slog.Logger, metrics *NotificationMetrics, category RecorderFailureCategory) {
	if category >= recorderFailureCategoryCount {
		return
	}
	if metrics != nil {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					return
				}
			}()
			metrics.RecordRecorderFailure(category)
		}()
	}
	shouldLog := true
	if metrics != nil {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					shouldLog = true
				}
			}()
			shouldLog = metrics.allowRecorderFailureLog(category)
		}()
	} else {
		shouldLog = fallbackRecorderFailureLogSamplers[category].allow(time.Now())
	}
	if !shouldLog {
		return
	}
	if log == nil {
		return
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				return
			}
		}()
		log.Error("telemetry recorder failure", "category", category.String())
	}()
}

func (m *NotificationMetrics) allowRecorderFailureLog(category RecorderFailureCategory) bool {
	now := m.startedAt
	if now.IsZero() {
		now = time.Now()
	}
	if m.now != nil {
		func() {
			defer func() {
				if recover() != nil {
					return
				}
			}()
			candidate := m.now()
			if !candidate.IsZero() {
				now = candidate
			}
		}()
	}
	return m.recorderFailureLogSamplers[category].allow(now)
}
