package api

import (
	"sync"
	"time"
)

const signUpRejectionLogInterval = 10 * time.Second

type signUpRejectionClass uint8

const (
	signUpMalformedRequest signUpRejectionClass = iota
	signUpRegistrationModeUnavailable
	signUpRateLimited
	signUpHandleInvalid
	signUpFirstNameInvalid
	signUpLastNameInvalid
	signUpCodeInvalid
	signUpInviteInvalid
	signUpHandleOccupied
	signUpSessionStateInvalid
	signUpInternal
	signUpRejectionClassCount
)

func (c signUpRejectionClass) String() string {
	return [...]string{
		"malformed_request",
		"registration_mode_unavailable",
		"rate_limited",
		"handle_invalid",
		"first_name_invalid",
		"last_name_invalid",
		"code_invalid",
		"invite_invalid",
		"handle_occupied",
		"session_state_invalid",
		"internal",
	}[c]
}

type signUpRejectionSample struct {
	last        int64
	initialized bool
	suppressed  int64
}

// signUpRejectionSampler keeps each fixed rejection class on its own interval
// so a flood of one outcome cannot silence the first record for another.
type signUpRejectionSampler struct {
	mu      sync.Mutex
	samples [signUpRejectionClassCount]signUpRejectionSample
}

func (s *signUpRejectionSampler) allow(class signUpRejectionClass, now time.Time) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sample := &s.samples[class]
	n := now.UnixNano()
	if sample.initialized && n-sample.last < int64(signUpRejectionLogInterval) {
		sample.suppressed++
		return 0, false
	}
	dropped := sample.suppressed
	sample.last = n
	sample.initialized = true
	sample.suppressed = 0
	return dropped, true
}

func (h *handlers) logSignUpRejection(class signUpRejectionClass) {
	suppressed, ok := h.signUpRejectionLogs.allow(class, h.now())
	if !ok {
		return
	}
	h.log.Warn(
		"auth.signUp rejected",
		"reason", class.String(),
		"registration_mode", string(h.registrationMode),
		"suppressed", suppressed,
	)
}
