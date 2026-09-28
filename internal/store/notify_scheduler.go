package store

import (
	"context"
	"sync"
)

// Notification callbacks may perform database reads and socket writes. A fixed
// worker set keeps that work off the LISTEN goroutine while the per-key lanes
// preserve order for notifications that address the same owner.
const (
	notificationWorkerCount      = 8
	notificationQueueLimit       = 256
	notificationLanePendingLimit = 32
)

type notificationTask struct {
	ctx      context.Context
	run      func(context.Context)
	coalesce bool
}

type notificationLane struct {
	key     string
	queued  bool
	running bool
	pending []notificationTask
}

type notificationScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan *notificationLane

	mu      sync.Mutex
	lanes   map[string]*notificationLane
	pending int
	wg      sync.WaitGroup
}

func newNotificationScheduler(parent context.Context) *notificationScheduler {
	ctx, cancel := context.WithCancel(parent)
	s := &notificationScheduler{
		ctx:    ctx,
		cancel: cancel,
		ready:  make(chan *notificationLane, notificationQueueLimit),
		lanes:  make(map[string]*notificationLane),
	}
	for range notificationWorkerCount {
		s.wg.Go(s.worker)
	}
	return s
}

// submit adds one callback without waiting for a blocked callback or growing
// the queue without limit. Consecutive coalescible callbacks for one key share
// one pending slot. Each key also has its own pending cap, so one noisy owner
// cannot consume the shared capacity needed to keep other owners responsive.
// A full queue drops the nudge, which durable update streams recover through
// their next difference request.
func (s *notificationScheduler) submit(key string, task notificationTask) bool {
	if task.ctx == nil {
		task.ctx = s.ctx
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || task.run == nil {
		return false
	}
	lane := s.lanes[key]
	newLane := false
	if lane == nil {
		if len(s.lanes) >= notificationQueueLimit {
			return false
		}
		lane = &notificationLane{key: key}
		s.lanes[key] = lane
		newLane = true
	}
	if task.coalesce && len(lane.pending) > 0 && lane.pending[len(lane.pending)-1].coalesce {
		lane.pending[len(lane.pending)-1] = task
		return true
	}
	if len(lane.pending) >= notificationLanePendingLimit {
		if len(lane.pending) == 0 && !lane.running && !lane.queued {
			delete(s.lanes, key)
		}
		return false
	}
	if s.pending >= notificationQueueLimit && !newLane {
		if len(lane.pending) == 0 && !lane.running && !lane.queued {
			delete(s.lanes, key)
		}
		return false
	}
	lane.pending = append(lane.pending, task)
	s.pending++
	if lane.running || lane.queued {
		return true
	}
	lane.queued = true
	select {
	case s.ready <- lane:
		return true
	default:
		lane.queued = false
		lane.pending = lane.pending[:len(lane.pending)-1]
		s.pending--
		delete(s.lanes, key)
		return false
	}
}

func (s *notificationScheduler) worker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case lane := <-s.ready:
			task, ok := s.next(lane)
			if !ok {
				continue
			}
			if s.ctx.Err() == nil {
				task.run(task.ctx)
			}
			s.finish(lane)
		}
	}
}

func (s *notificationScheduler) next(lane *notificationLane) (notificationTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || !lane.queued || len(lane.pending) == 0 {
		return notificationTask{}, false
	}
	task := lane.pending[0]
	lane.pending = lane.pending[1:]
	lane.queued = false
	lane.running = true
	s.pending--
	return task, true
}

func (s *notificationScheduler) finish(lane *notificationLane) {
	s.mu.Lock()
	lane.running = false
	if s.ctx.Err() != nil || len(lane.pending) == 0 {
		delete(s.lanes, lane.key)
		s.mu.Unlock()
		return
	}
	lane.queued = true
	s.mu.Unlock()

	select {
	case s.ready <- lane:
	case <-s.ctx.Done():
	}
}

func (s *notificationScheduler) stop() {
	s.cancel()
	s.wg.Wait()
}

func (l *Listener) schedule(key string, task notificationTask) {
	if l.scheduler == nil {
		task.run(task.ctx)
		return
	}
	if !l.scheduler.submit(key, task) {
		l.log.Debug("notification callback queue full", "key", key)
	}
}
