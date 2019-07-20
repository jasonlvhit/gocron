package gocron

import (
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
)

var defaultOption = func(j *Job) {
	j.ShouldDoImmediately = true
}

func task() {
	fmt.Println("I am a running job.")
}

func taskWithParams(a int, b string) {
	fmt.Println(a, b)
}

func assertEqualTime(name string, t *testing.T, actual, expected time.Time) {
	if actual != expected {
		t.Errorf("test name: %s actual different than expected want: %v -> got: %v", name, expected, actual)
	}
}

func TestSecond(t *testing.T) {

	defaultScheduler.Every(1, defaultOption).Second().Do(task)
	defaultScheduler.Every(1, defaultOption).Second().Do(taskWithParams, 1, "hello")
	stop := defaultScheduler.Start()
	time.Sleep(5 * time.Second)
	close(stop)

	if err := defaultScheduler.Err(); err != nil {
		t.Error(err)
	}
	defaultScheduler.Clear()
}

func Test_formatTime(t *testing.T) {
	tests := []struct {
		name     string
		args     string
		wantHour int
		wantMin  int
		wantErr  bool
	}{
		{
			name:     "normal",
			args:     "16:18",
			wantHour: 16,
			wantMin:  18,
			wantErr:  false,
		},
		{
			name:     "normal",
			args:     "6:18",
			wantHour: 6,
			wantMin:  18,
			wantErr:  false,
		},
		{
			name:     "notnumber",
			args:     "e:18",
			wantHour: 0,
			wantMin:  0,
			wantErr:  true,
		},
		{
			name:     "outofrange",
			args:     "25:18",
			wantHour: 25,
			wantMin:  18,
			wantErr:  true,
		},
		{
			name:     "wrongformat",
			args:     "19:18:17",
			wantHour: 0,
			wantMin:  0,
			wantErr:  true,
		},
		{
			name:     "wrongminute",
			args:     "19:1e",
			wantHour: 19,
			wantMin:  0,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHour, gotMin, err := formatTime(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("formatTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHour != tt.wantHour {
				t.Errorf("formatTime() gotHour = %v, want %v", gotHour, tt.wantHour)
			}
			if gotMin != tt.wantMin {
				t.Errorf("formatTime() gotMin = %v, want %v", gotMin, tt.wantMin)
			}
		})
	}
}

func TestTaskAt(t *testing.T) {
	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule to run in next minute
	now := time.Now()
	// Expected start time
	startTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+1, 0, 0, loc)
	// Expected next start time day after
	startNext := startTime.AddDate(0, 0, 1)

	// Schedule every day At
	startAt := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute()+1)
	dayJob := s.Every(1, defaultOption).Day().At(startAt)
	if err := dayJob.Err(); err != nil {
		t.Error(err)
	}

	dayJobDone := make(chan bool, 1)
	// Job running 5 sec
	dayJob.Do(func() {
		t.Log(time.Now(), "job start")
		time.Sleep(2 * time.Second)
		dayJobDone <- true
		t.Log(time.Now(), "job done")
	})

	// Check first run
	nextRun := dayJob.NextScheduledTime()
	assertEqualTime("first run", t, nextRun, startTime)

	sStop := s.Start()      // Start scheduler
	<-dayJobDone            // Wait job done
	close(sStop)            // Stop scheduler
	time.Sleep(time.Second) // wait for scheduler to reschedule job

	// Check next run
	nextRun = dayJob.NextScheduledTime()
	assertEqualTime("next run", t, nextRun, startNext)
}

func TestDaily(t *testing.T) {
	now := time.Now()

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// schedule next run 1 day
	dayJob := s.Every(1, defaultOption).Day()
	dayJob.scheduleNextRun(true)
	exp := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("1 day", t, dayJob.nextRun, exp)

	// schedule next run 2 days
	dayJob = s.Every(2, defaultOption).Days()
	dayJob.scheduleNextRun(true)
	exp = time.Date(now.Year(), now.Month(), now.Day()+2, 0, 0, 0, 0, loc)
	assertEqualTime("2 days", t, dayJob.nextRun, exp)

	// Job running longer than next schedule 1day 2 hours
	dayJob = s.Every(1, defaultOption).Day()
	dayJob.lastRun = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+2, 0, 0, 0, loc)
	dayJob.scheduleNextRun(true)
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("1 day 2 hours", t, dayJob.nextRun, exp)

	// At() 2 hours before now
	hour := now.Hour() - 2
	minute := now.Minute()
	startAt := fmt.Sprintf("%02d:%02d", hour, minute)
	dayJob = s.Every(1, defaultOption).Day().At(startAt)
	if err := dayJob.Err(); err != nil {
		t.Error(err)
	}

	dayJob.scheduleNextRun(true)
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, loc)
	assertEqualTime("at 2 hours before now", t, dayJob.nextRun, exp)
}

func TestWeekdayAfterToday(t *testing.T) {
	now := time.Now()

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule job at next week day
	var weekJob *Job
	switch now.Weekday() {
	case time.Monday:
		weekJob = s.Every(1, defaultOption).Tuesday()
	case time.Tuesday:
		weekJob = s.Every(1, defaultOption).Wednesday()
	case time.Wednesday:
		weekJob = s.Every(1, defaultOption).Thursday()
	case time.Thursday:
		weekJob = s.Every(1, defaultOption).Friday()
	case time.Friday:
		weekJob = s.Every(1, defaultOption).Saturday()
	case time.Saturday:
		weekJob = s.Every(1, defaultOption).Sunday()
	case time.Sunday:
		weekJob = s.Every(1, defaultOption).Monday()
	}

	// First run
	weekJob.scheduleNextRun(true)
	exp := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("first run", t, weekJob.nextRun, exp)

	// Simulate job run 7 days before
	weekJob.lastRun = weekJob.nextRun.AddDate(0, 0, -7)
	// Next run
	weekJob.scheduleNextRun(true)
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, loc)
	assertEqualTime("next run", t, weekJob.nextRun, exp)
}

func TestWeekdayBeforeToday(t *testing.T) {
	now := time.Now()

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule job at day before
	var weekJob *Job
	switch now.Weekday() {
	case time.Monday:
		weekJob = s.Every(1, defaultOption).Sunday()
	case time.Tuesday:
		weekJob = s.Every(1, defaultOption).Monday()
	case time.Wednesday:
		weekJob = s.Every(1, defaultOption).Tuesday()
	case time.Thursday:
		weekJob = s.Every(1, defaultOption).Wednesday()
	case time.Friday:
		weekJob = s.Every(1, defaultOption).Thursday()
	case time.Saturday:
		weekJob = s.Every(1, defaultOption).Friday()
	case time.Sunday:
		weekJob = s.Every(1, defaultOption).Saturday()
	}

	weekJob.scheduleNextRun(true)
	exp := time.Date(now.Year(), now.Month(), now.Day()+6, 0, 0, 0, 0, loc)
	assertEqualTime("first run", t, weekJob.nextRun, exp)

	// Simulate job run 7 days before
	weekJob.lastRun = weekJob.nextRun.AddDate(0, 0, -7)
	// Next run
	weekJob.scheduleNextRun(true)
	exp = time.Date(now.Year(), now.Month(), now.Day()+6, 0, 0, 0, 0, loc)
	assertEqualTime("nest run", t, weekJob.nextRun, exp)
}

func TestWeekdayAt(t *testing.T) {
	now := time.Now()

	hour := now.Hour()
	minute := now.Minute()
	startAt := fmt.Sprintf("%02d:%02d", hour, minute)

	// Create new scheduler to have clean test env
	s := NewScheduler()

	// Schedule job at next week day
	var weekJob *Job
	switch now.Weekday() {
	case time.Monday:
		weekJob = s.Every(1, defaultOption).Tuesday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Tuesday:
		weekJob = s.Every(1, defaultOption).Wednesday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Wednesday:
		weekJob = s.Every(1, defaultOption).Thursday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Thursday:
		weekJob = s.Every(1, defaultOption).Friday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Friday:
		weekJob = s.Every(1, defaultOption).Saturday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Saturday:
		weekJob = s.Every(1, defaultOption).Sunday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	case time.Sunday:
		weekJob = s.Every(1, defaultOption).Monday().At(startAt)
		if err := weekJob.Err(); err != nil {
			t.Error(err)
		}
	}

	// First run
	weekJob.scheduleNextRun(true)
	exp := time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, loc)
	assertEqualTime("first run", t, weekJob.nextRun, exp)

	// Simulate job run 7 days before
	weekJob.lastRun = weekJob.nextRun.AddDate(0, 0, -7)
	// Next run
	weekJob.scheduleNextRun(true)
	exp = time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, loc)
	assertEqualTime("next run", t, weekJob.nextRun, exp)
}

type foo struct {
	jobNumber int64
}

func (f *foo) incr() {
	atomic.AddInt64(&f.jobNumber, 1)
}

func (f *foo) getN() int64 {
	return atomic.LoadInt64(&f.jobNumber)
}

const expectedNumber int64 = 10

var (
	testF  *foo
	client *redis.Client
)

func init() {
	s, err := miniredis.Run()
	if err != nil {
		log.Fatal(err)
	}
	// defer s.Close()

	client = redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	testF = new(foo)
}

func TestBasicDistributedJob1(t *testing.T) {
	t.Parallel()

	var defaultOption = func(j *Job) {
		j.DistributedJobName = "counter"
		j.DistributedRedisClient = client
	}

	sc := NewScheduler()
	sc.Every(1, defaultOption).Second().Do(testF.incr)

loop:
	for {
		select {
		case <-sc.Start():
		case <-time.After(10 * time.Second):
			sc.Clear()
			break loop
		}
	}

	if (expectedNumber-1 != testF.getN()) && (expectedNumber != testF.getN()) && (expectedNumber+1 != testF.getN()) {
		t.Errorf("1 expected number of jobs %d, got %d", expectedNumber, testF.getN())
	}

}

func TestBasicDistributedJob2(t *testing.T) {
	t.Parallel()

	var defaultOption = func(j *Job) {
		j.DistributedJobName = "counter"
		j.DistributedRedisClient = client
	}

	sc := NewScheduler()
	sc.Every(1, defaultOption).Second().Do(testF.incr)

loop:
	for {
		select {
		case <-sc.Start():
		case <-time.After(10 * time.Second):
			sc.Clear()
			break loop
		}
	}

	if (expectedNumber-1 != testF.getN()) && (expectedNumber != testF.getN()) && (expectedNumber+1 != testF.getN()) {
		t.Errorf("2 expected number of jobs %d, got %d", expectedNumber, testF.getN())
	}
}

func TestBasicDistributedJob3(t *testing.T) {
	t.Parallel()

	var defaultOption = func(j *Job) {
		j.DistributedJobName = "counter"
		j.DistributedRedisClient = client
	}

	sc := NewScheduler()
	sc.Every(1, defaultOption).Second().Do(testF.incr)

loop:
	for {
		select {
		case <-sc.Start():
		case <-time.After(10 * time.Second):
			sc.Clear()
			break loop
		}
	}

	if (expectedNumber-1 != testF.getN()) && (expectedNumber != testF.getN()) && (expectedNumber+1 != testF.getN()) {
		t.Errorf("3 expected number of jobs %d, got %d", expectedNumber, testF.getN())
	}
}

func TestBasicDistributedJob4(t *testing.T) {
	t.Parallel()

	var defaultOption = func(j *Job) {
		j.DistributedJobName = "counter"
		j.DistributedRedisClient = client
	}

	sc := NewScheduler()
	sc.Every(1, defaultOption).Second().Do(testF.incr)

loop:
	for {
		select {
		case <-sc.Start():
		case <-time.After(10 * time.Second):
			sc.Clear()
			break loop
		}
	}

	if (expectedNumber-1 != testF.getN()) && (expectedNumber != testF.getN()) && (expectedNumber+1 != testF.getN()) {
		t.Errorf("4 expected number of jobs %d, got %d", expectedNumber, testF.getN())
	}
}

func TestBasicDistributedJob5(t *testing.T) {
	t.Parallel()

	var defaultOption = func(j *Job) {
		j.DistributedJobName = "counter"
		j.DistributedRedisClient = client
	}

	sc := NewScheduler()
	sc.Every(1, defaultOption).Second().Do(testF.incr)

loop:
	for {
		select {
		case <-sc.Start():
		case <-time.After(10 * time.Second):
			sc.Clear()
			break loop
		}
	}

	if (expectedNumber-1 != testF.getN()) && (expectedNumber != testF.getN()) && (expectedNumber+1 != testF.getN()) {
		t.Errorf("5 expected number of jobs %d, got %d", expectedNumber, testF.getN())
	}
}
