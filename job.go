package gocron

import (
	"errors"
	"reflect"
	"time"
)

var (
	// ErrTaskIsNotAFuncError is the error panicked when a task passed to `Job.Do`
	ErrTaskIsNotAFuncError = errors.New("the `task` your a scheduling must be of type func")

	// ErrMissmatchedTaskParams is the error panicked when someone passes too many or too few params to `Job.Do`
	ErrMissmatchedTaskParams = errors.New("the `task` your a scheduling must be of type func")

	// ErrJobIsNotInitialized is the error panicked when a job is scheduled that was not initialized
	ErrJobIsNotInitialized = errors.New("this job was not intialized")

	// ErrIncorrectTimeFormat is the error panicked when `At` is passed an incorrect time
	ErrIncorrectTimeFormat = errors.New("the time format is incorrect")

	// ErrIntervalNotValid error panicked when the interval is not valid
	ErrIntervalNotValid = errors.New("the interval must be greater than 0")
)

const (
	// Day is the duration for a Days worth of time
	Day = 24 * time.Hour

	// Week is the duration for a Weeks worth of time
	Week = 7 * Day
)

// Job calculates the time intervals in which a task should be executed.
//
// A Note About Initialization
//
// Jobs are always run as if there first execution day, week, and time occured or will occur in the current week.
// Note that a week starts on time.Sunday (go's iota value) and the time starts at "00:00".
//
// Lets take the following code for example:
//  // ...
//  sechduler.Every(3).Monday().At("05:00").Do(task)
//  scheduler.Start()
//
// If `scheduler.Start()` is called any time before Monday at 5:00 am, the first time `task` is executed will be this Monday at 5:30 am.
// The next time `task` will be called will be three weeks after that on Monday at 5:30 am
//
// However, if `scheduler.Start()` is called any time after Monday at 5:00 am, the first time `task` is executed will next Monday at 5:30 am.
// The next time `task` will be called will be still be three weeks after that on Monday at 5:30 am
//
// The reason for this is in the event where a server is restarted multiple times within the specified interval
// If there is other behavior desired, please open up an issue :) or send your use case to mark@dealyze.com
//
type Job struct {

	// pause interval * unit bettween runs
	interval uint64

	// the tasks this job executes
	tasks []reflect.Value

	// the parameters that will be passed to this job upon execution
	tasksParams [][]reflect.Value

	// time units the `interval` is the quantity of , e.g. `time.Minute`, `time.Hour`, `Week`...
	unit time.Duration

	// optional time at which this job runs
	atTime time.Duration

	// time of last run
	lastRun time.Time

	// time of next run
	nextRun time.Time

	// specific day of the week to start on
	weekDay time.Weekday

	// location the time of the job takes place in
	location *time.Location
}

// NewJob creates a new job
func newJob(interval uint64) *Job {
	if interval == 0 {
		panic(ErrIntervalNotValid)
	}
	return &Job{
		interval: interval,
		location: time.Local,
		atTime:   -time.Second,
	}
}

// should run returns true if the job should be run now
func (j *Job) shouldRun(now time.Time) bool {
	return now.After(j.nextRun) || now.Equal(j.nextRun)
}

// run the job
func (j *Job) run() {
	for i, task := range j.tasks {
		task.Call(j.tasksParams[i])
	}
	j.lastRun = j.nextRun
	j.nextRun = j.lastRun.Add(time.Duration(j.interval) * j.unit)
}

// isInit returns true if the the `lastRun` and `nextRun` time have been initialized by `init()`
func (j *Job) isInit() bool {
	return !j.lastRun.IsZero() && !j.nextRun.IsZero()
}

// init sets the `lastRun` and `nextRun` times
func (j *Job) init(now time.Time) {
	// compute the current time
	currentTime := time.Duration(now.Hour())*time.Hour + time.Duration(now.Minute())*time.Minute

	// set the default atTime of the job if it hasn't been set explicitly by `At`
	if j.atTime < 0 {
		j.atTime = currentTime
	}

	// create the lastRun time
	if j.unit == Week {
		// the lastRun time either occured `interval` weeks ago, so that its nextRun will come up some time this week
		// or the lastRun occured this week already so that its nextRun will come up some time `interval` weeks from now
		j.lastRun = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, now.Second(), now.Nanosecond(), j.location).
			Add(time.Duration(j.weekDay-now.Weekday()) * Day).
			Add(j.atTime)

		if j.lastRun.After(now) {
			// this should be run for the first time this week
			j.lastRun = j.lastRun.Add(-time.Duration(j.interval) * Week)
		} else if j.lastRun.Before(now) {
			// this should be run for the first time next week
			j.lastRun = j.lastRun.Add(-time.Duration(j.interval-1) * Week)
		}

	} else if j.unit == Day {
		// the lastRun time either occured `interval` days ago, so that its nextRun will come up some time today
		// or the lastRun occured today already so that its nextRun will come up some time `interval` days from now
		j.lastRun = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, now.Second(), now.Nanosecond(), j.location).Add(j.atTime)

		if j.lastRun.After(now) {
			// this should be run for the first time today
			j.lastRun = j.lastRun.Add(-time.Duration(j.interval) * Day)
		} else if j.lastRun.Before(now) {
			// this should be run for the first time tomorrow
			j.lastRun = j.lastRun.Add(-time.Duration(j.interval-1) * Day)
		}

	} else {
		j.lastRun = now
	}

	// create the nextRun
	j.nextRun = j.lastRun.Add(time.Duration(j.interval) * j.unit)
}

// Do specifies the taks that should be called executed and the parameters it should be passed
// Example
//
//  // ...
//	job := Every(1).Day().At("10:30").Do(task, paramOne, "paramTwo")  // performs `task(paramOne, "paramTwo")` every day at 10:30 am
//  job.Do(task2, paramThree, "paramFour")                            // `task2(paramThree, "paramFour")` will perperformed at the same interval
//
func (j *Job) Do(task interface{}, params ...interface{}) *Job {
	// reflect the task and params in to values
	taskValue := reflect.ValueOf(task)
	paramValues := make([]reflect.Value, len(params))
	for i, param := range params {
		paramValues[i] = reflect.ValueOf(param)
	}

	// panic if the task won't be able to be executed
	if taskValue.Type().NumIn() != len(paramValues) {
		panic(ErrMissmatchedTaskParams)
	} else if taskValue.Kind() != reflect.Func {
		panic(ErrTaskIsNotAFuncError)
	}

	// add the task and its params to the job
	j.tasks = append(j.tasks, taskValue)
	j.tasksParams = append(j.tasksParams, paramValues)

	return j
}

// At adds a time component to daily or weekly recurring tasks.
//
// note: if no time is specified, the `At` time will default to whenever `Schedule.Start()` is called
//
// Example
//
//  // ...
//	Every(1).Day().At("10:30").Do(task)    // performs a task every day at 10:30 am
//	Every(1).Monday().At("22:30").Do(task) // performs a task every Monday at 10:30 pm
//	Every(1).Monday().Do(task)             // performs a task every Monday at whatever time `Schedule.Start()` is called
//
func (j *Job) At(t string) *Job {
	hour := int((t[0]-'0')*10 + (t[1] - '0'))
	min := int((t[3]-'0')*10 + (t[4] - '0'))
	if hour < 0 || hour > 23 || min < 0 || min > 59 {
		panic(ErrIncorrectTimeFormat)
	}
	j.atTime = time.Duration(hour) * time.Hour
	j.atTime += time.Duration(min) * time.Minute
	return j
}

// Seconds sets a job to run every `x` number of seconds
//
// Example
//
//  // ...
//	Every(5).Seconds().Do(task) // executes the task func every 5 seconds
//
func (j *Job) Seconds() *Job {
	j.unit = time.Second
	return j
}

// Second is an alias for `Seconds`
func (j *Job) Second() *Job {
	return j.Seconds()
}

// Minutes sets a job to run every `x` number of minutes
//
// Example
//
//  // ...
//	Every(5).Minutes().Do(task) // executes the task func every 5 minutes
//
func (j *Job) Minutes() *Job {
	j.unit = time.Minute
	return j
}

// Minute is an alias for `Minutes`
func (j *Job) Minute() *Job {
	return j.Minutes()
}

// Hours sets a task to run every `x` number of hours
//
// Example
//
//  // ...
//	Every(5).Hours().Do(task) // executes the task func every 5 hours
//
func (j *Job) Hours() *Job {
	j.unit = time.Hour
	return j
}

// Hour is an alias for `Hours`
func (j *Job) Hour() *Job {
	return j.Hours()
}

// Days sets a task to run every `x` number of hours
//
// Example
//
//  // ...
//	Every(5).Days().Do(task) // executes the task func every 5 days
//
func (j *Job) Days() *Job {
	j.unit = Day
	return j
}

// Day is an alias for `Days`
func (j *Job) Day() *Job {
	return j.Days()
}

// Weekday sets the task to be performed on a certian day of the week
//
// Example
//
//  // ...
//  scheduler.Every(1).Weekday(time.Sunday).Do(task) // executes the task once every Sunday at whatever time `Scheduler.Start()` is called
//  scheduler.Every(2).Weekday(time.Monday).At("05:00").Do(task) // executes the task every other Monday at 7 am
//
func (j *Job) Weekday(weekday time.Weekday) *Job {
	j.weekDay = weekday
	j.unit = Week
	return j
}

// Monday is an alias for `Weekday(time.Monday)`
func (j *Job) Monday() *Job {
	return j.Weekday(time.Monday)
}

// Tuesday is an alias for `Weekday(time.Tuesday)`
func (j *Job) Tuesday() *Job {
	return j.Weekday(time.Tuesday)
}

// Wednesday is an alias for `Weekday(time.Wednesday)`
func (j *Job) Wednesday() *Job {
	return j.Weekday(time.Wednesday)
}

// Thursday is an alias for `Weekday(time.Thursday)`
func (j *Job) Thursday() *Job {
	return j.Weekday(time.Thursday)
}

// Friday is an alias for `Weekday(time.Friday)`
func (j *Job) Friday() *Job {
	return j.Weekday(time.Friday)
}

// Saturday is an alias for `Weekday(time.Saturday)`
func (j *Job) Saturday() *Job {
	return j.Weekday(time.Saturday)
}

// Sunday is an alias for `Weekday(time.Sunday)`
func (j *Job) Sunday() *Job {
	return j.Weekday(time.Sunday)
}

// Weeks is an alias for `Weekday(time.Now().Weekday())`
func (j *Job) Weeks() *Job {
	return j.Weekday(time.Now().Weekday())
}

// Week is an alias for `Weekday(time.Now().Weekday())`
func (j *Job) Week() *Job {
	return j.Weekday(time.Now().Weekday())
}

// Location sets the timezone of the job.
// Jobs created by `NewJob(...)` have a default location of`time.Local`.
// Jobs created by `Scheduler.Every(...)` have a default timezone of whatever `Scheduler.Location(...)` is set to.
//
// Example
//
//  // ...
//  est, err := time.LoadLocation("America/New_York")
//  if err != nil { // you probably haven't set up your server correctly ;)
//  	panic(err)
//  }
//  Every(2).Monday().At("05:00").Location(est).Do(task) // executes the task every monday at 5:00 am eastern standard time
//
func (j *Job) Location(loc *time.Location) *Job {
	j.location = loc
	return j
}
