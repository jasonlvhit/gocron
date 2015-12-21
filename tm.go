package gocron

import (
	"math"
	"sort"
	"sync"
	"time"
)

type TimerWheel interface {
	// Delete all scheduled jobs
	Clear()

	// Create a new periodic job
	Every(interval uint64) *Job

	// Create a one time job
	After(interval uint64) *Job

	// Remove specific job j
	Cancel(*Job) bool

	// Run all the jobs that are scheduled to run.
	RunPending()

	// Start all the pending jobs Add seconds ticker
	Start() chan bool
}

type TimerWheelConfig struct {
	TimeSlotCount uint
	SlotInterval  time.Duration
	SequenceCall  bool
	Logf          func(format string, v ...interface{})
}

var DefaultTimerWheelConfig = &TimerWheelConfig{
	TimeSlotCount: 60 * 60,
	SlotInterval:  1 * time.Second,
	SequenceCall:  false,
}

type timeSlot struct {
	lock sync.Mutex
	jobs []*Job
}

type timerWheel struct {
	*TimerWheelConfig
	slots       []timeSlot
	currentSlot int
}

func NewTimerWheel(cfg *TimerWheelConfig) TimerWheel {
	if cfg == nil {
		cfg = DefaultTimerWheelConfig
	}

	return &timerWheel{
		TimerWheelConfig: cfg,
		slots:            make([]timeSlot, cfg.TimeSlotCount),
	}
}

func (tw *timerWheel) Clear() {
	for _, slot := range tw.slots {
		slot.lock.Lock()
		slot.jobs = nil
		slot.lock.Unlock()
	}
}

func (tw *timerWheel) Every(interval uint64) *Job {
	job := NewJob(interval)
	job.onSchedule = func() interface{} {
		return tw.scheduleJob(job)
	}

	return job
}

func (tw *timerWheel) After(interval uint64) *Job {
	job := NewJob(interval)
	job.onSchedule = func() interface{} {
		job.onSchedule = nil

		return tw.scheduleJob(job)
	}

	return job
}

func (tw *timerWheel) Cancel(job *Job) bool {
	if n, ok := job.userData.(int); ok {
		slot := tw.slots[n]

		slot.lock.Lock()
		defer slot.lock.Unlock()

		for i, j := range slot.jobs {
			if j == job {
				if tw.Logf != nil {
					tw.Logf("remove job %p @ slot #%d:%d", job, n, i)
				}

				slot.jobs = append(slot.jobs[:i], slot.jobs[i+1:]...)

				return true
			}
		}
	}

	return false
}

func (tw *timerWheel) RunPending() {
	jobs := tw.getPending(tw.currentSlot)

	go func() {
		for _, job := range jobs {
			if tw.SequenceCall {
				tw.runJob(job)
			} else {
				go tw.runJob(job)
			}
		}
	}()
}

func (tw *timerWheel) getPending(n int) (jobs []*Job) {
	slot := tw.slots[n]

	if len(slot.jobs) == 0 {
		return
	}

	slot.lock.Lock()

	if slot.jobs[len(slot.jobs)-1].shouldRun() {
		// Fast Path O(1)
		jobs = slot.jobs
		slot.jobs = nil
	} else {
		// Binary Search O(log n)
		pending := sort.Search(len(slot.jobs), func(i int) bool {
			return !slot.jobs[i].shouldRun()
		})

		jobs = slot.jobs[:pending]
		slot.jobs = slot.jobs[pending:]
	}

	slot.lock.Unlock()

	if tw.Logf != nil {
		tw.Logf("found %d pending jobs @ slot #%d @ %s, %s", len(jobs), tw.currentSlot, time.Now(), jobs)
	}

	return
}

func (tw *timerWheel) Start() chan bool {
	stopped := make(chan bool, 1)
	ticker := time.NewTicker(tw.SlotInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				tw.currentSlot = (tw.currentSlot + 1) % len(tw.slots)
				tw.RunPending()

			case <-stopped:
				return
			}
		}
	}()

	return stopped
}

func (tw *timerWheel) runJob(job *Job) {
	defer func() {
		if r := recover(); r != nil {
			tw.scheduleJob(job)
		}
	}()

	job.run()
}

func (tw *timerWheel) nextSlot(job *Job) (int, *timeSlot) {
	nextSlot := (tw.currentSlot + int(math.Ceil(float64(job.nextRun.Sub(time.Now()))/float64(tw.SlotInterval)))) % len(tw.slots)

	if tw.Logf != nil {
		tw.Logf("%p was mapped to slot #%d", job, nextSlot)
	}

	return nextSlot, &tw.slots[nextSlot]
}

func (tw *timerWheel) scheduleJob(job *Job) int {
	n, slot := tw.nextSlot(job)

	slot.lock.Lock()

	if len(slot.jobs) == 0 || job.nextRun.After(slot.jobs[len(slot.jobs)-1].nextRun) {
		// Fast Path O(1)
		slot.jobs = append(slot.jobs, job)
	} else {
		// Binary Search O(log n)
		i := sort.Search(len(slot.jobs), func(i int) bool {
			return slot.jobs[i].nextRun.After(job.nextRun)
		})

		jobs := append(slot.jobs[:i], job)

		slot.jobs = append(jobs, slot.jobs[i:]...)
	}

	slot.lock.Unlock()

	return n
}
