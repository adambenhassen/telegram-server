package store

import "log/slog"

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
