package gocron

import (
	"sort"
	"sync"
	"time"
)

// Scheduler keeps a slice of jobs that it executes at a regular interval
type Scheduler interface {
	// Start starts the scheduler
	Start()

	// IsRunning returns true if the job  has started
	IsRunning() bool

	// Stop stops the scheduler from executing jobs
	Stop()

	// Every creates a new job, and adds it to the `Scheduler`
	Every(interval uint64) *Job

	// Remove removes an individual job from the scheduler. It returns true if the job was found and removed from the `Scheduler`
	Remove(*Job) bool

	// Clear removes all of the jobs that have been added to the scheduler
	Clear()

	// Location sets the default location of every job created with `Every`.
	// The default location is `time.Local`
	Location(*time.Location)

	// NextRun returns the next next job to be run and the time in which it will be run
	NextRun() (*Job, time.Time)

	// RunAll runs all of the jobs regardless of wether or not they are pending
	RunAll()

	// RunPending runs all of the pending jobs
	RunPending()

	// RunAllWithDelay runs all of the jobs regardless of wether or not they are pending with a delay
	RunAllWithDelay(time.Duration)
}

// NewScheduler create a new scheduler.
// Note: the current implementation is not concurrency safe.
func NewScheduler() Scheduler {
	return &scheduler{
		isStopped: make(chan bool),
		location:  time.Local,
	}
}

// Scheduler contains jobs and a loop to run the jobs
type scheduler struct {
	jobs      []*Job
	isRunning bool
	isStopped chan bool
	location  *time.Location
	mutex     sync.Mutex
}

// Len returns the number of jobs that have been scheduled. It is part of the `sort.Interface` interface
func (s *scheduler) Len() int {
	return len(s.jobs)
}

// Swap swaps the order of two jobs in the backing slice. It is part of the `sort.Interface` interface
func (s *scheduler) Swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}

// Less swaps the order of two jobs in the backing slice. It is part of the `sort.Interface` interface
func (s *scheduler) Less(i, j int) bool {
	return s.jobs[j].nextRun.After(s.jobs[i].nextRun)
}

// NextRun returns the job and time when the next job should run
func (s *scheduler) NextRun() (*Job, time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.jobs) == 0 {
		return nil, time.Time{}
	}
	sort.Sort(s)
	return s.jobs[0], s.jobs[0].nextRun
}

// Every schedules a new job
func (s *scheduler) Every(interval uint64) *Job {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	job := newJob(interval).Location(s.location)
	s.jobs = append(s.jobs, job) // TODO: make this a binary insert

	return job
}

// runPending runs all of the jobs pending at this time
func (s *scheduler) runPending(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	sort.Sort(s)
	for _, job := range s.jobs {
		if !job.isInit() {
			job.init(now)
		}
		if job.shouldRun(now) {
			job.run()
		} else {
			break
		}
	}
}

// Depricated: RunPending runs all of the jobs that are scheduled to run
func (s *scheduler) RunPending() {
	s.runPending(time.Now())
}

// Depricated: RunAll rungs all jobs regardless if they are scheduled to run or not
func (s *scheduler) RunAll() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()
	sort.Sort(s)
	for _, job := range s.jobs {
		if !job.isInit() {
			job.init(now)
		}
		job.run()
	}

}

// Depricated: RunAllWithDelay all jobs with delay seconds
func (s *scheduler) RunAllWithDelay(d time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()
	sort.Sort(s)
	for _, job := range s.jobs {
		if !job.isInit() {
			job.init(now)
		}
		job.run()
		time.Sleep(d)
	}

}

// Location sets the default location for every job created with `Scheduler.Every(...)`. By default the location is `time.Local`
func (s *scheduler) Location(location *time.Location) {
	s.location = location
}

// Removes a job from the queue
func (s *scheduler) Remove(j *Job) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// TODO: make this a binary search based removal
	for i, job := range s.jobs {
		if j == job {
			s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
			return true
		}
	}

	return false
}

// Clear deletes all scheduled jobs
func (s *scheduler) Clear() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.jobs = []*Job{}
}

// Start all the pending jobs
// Add seconds ticker
func (s *scheduler) Start() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// only start the scheduler if it hasn't been started yet
	if s.isRunning {
		return
	}

	// start the scheduler
	isStarted := make(chan bool)
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case now := <-ticker.C:
				if !s.isRunning {
					// initialize all of the jobs with the first ticker time
					// so that they are all in sync with the run loop
					for _, job := range s.jobs {
						job.init(now)
					}
					s.isRunning = true
					isStarted <- true
				}
				s.runPending(now)
			case <-s.isStopped:
				s.isRunning = false
				s.isStopped <- true // send a confirmation message back to the `Stop()` method
				return
			}
		}
	}()

	// wait until he ticker has been started and all of the jobs have been initialized
	<-isStarted
}

// IsRunning returns true if the scheduler is startes
func (s *scheduler) IsRunning() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.isRunning
}

// Stop stops the scheduler
func (s *scheduler) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// only send the stop signal if the scheduler has been started
	if s.isRunning {
		s.isStopped <- true
		<-s.isStopped // wait for the ticker to send a confirmation message back through the stop channel just before it shuts down the ticker loop
	}
}
