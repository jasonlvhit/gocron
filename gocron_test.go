// Tests for gocron
package gocron

import (
	"fmt"
	"github.com/marksalpeter/sugar"
	"math/rand"
	"testing"
	"time"
)

func TestJob(t *testing.T) {

	// note: we're defining today as the first of the month so we can test an important edge case in the lastRun
	// calculation when a jobs lastRun time occured the previous month from when the job initialized
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), 1, now.Hour(), now.Minute(), now.Second(), 0, now.Location())
	aMinuteAgo := today.Add(-time.Minute)
	aMinuteFromNow := today.Add(time.Minute)
	aMinuteAgoAtTime := fmt.Sprintf("%02d:%02d", aMinuteAgo.Hour(), aMinuteAgo.Minute())
	aMinuteFromNowAtTime := fmt.Sprintf("%02d:%02d", aMinuteFromNow.Hour(), aMinuteFromNow.Minute())
	s := sugar.New(t)

	s.Title("Day")

	s.Assert("`Job.Every(...).Day().At(...)` set to the future causese lastRun to be `interval` days ago and nextRun to be today", func(log sugar.Log) bool {
		// try this with 20 random day intervals
		for i := 20; i > 0; i-- {

			// get a random interval of days [1, 356]
			rand.Seed(time.Now().UnixNano())
			interval := 1 + uint64(rand.Int())%356

			// create and init the job
			job := newJob(interval).Day().At(aMinuteFromNowAtTime)
			job.init(today)

			// jobs last run was not `daysInterval` ago
			aMinuteFromNowIntervalDaysAgo := aMinuteFromNow.Add(time.Duration(-24*int(interval)) * time.Hour)
			if !job.lastRun.Equal(aMinuteFromNowIntervalDaysAgo) {
				log("the last run did not occur %d days ago", interval)
				log(job.lastRun, aMinuteFromNowIntervalDaysAgo)
				return false
			}

			// jobs next run is not today
			if !job.nextRun.Equal(aMinuteFromNow) {
				log("the next run did will not happen a minute from now")
				log(job.nextRun, aMinuteFromNow)
				return false
			}
		}

		return true
	})

	s.Assert("`Job.Every(...).Day.At(...)` set to the past causes lastRun to be `interval` days from tomorrow and the nextRun to be tomorrow", func(log sugar.Log) bool {
		// try this with 20 random day intervals
		for i := 20; i > 0; i-- {

			// get a random interval of days [1, 365]
			rand.Seed(time.Now().UnixNano())
			interval := 1 + uint64(rand.Int())%356

			// create and init the job
			job := newJob(interval).Day().At(aMinuteAgoAtTime)
			job.init(today)

			// jobs last run is now interval days from tomorrow
			intervalDaysAMinuteFromTomorrow := aMinuteAgo.Add(24 * time.Hour).Add(-24 * time.Hour * time.Duration(interval))
			if !job.lastRun.Equal(intervalDaysAMinuteFromTomorrow) {
				log("the lastRun did not occur today")
				log(job.lastRun, intervalDaysAMinuteFromTomorrow)
				return false
			}

			// jobs next run is not tomorrow
			tomorrowAMinuteAgo := aMinuteAgo.Add(24 * time.Hour)
			if !job.nextRun.Equal(tomorrowAMinuteAgo) {
				log("the nextRun did not occur %d days from now", interval)
				log(job.nextRun, tomorrowAMinuteAgo)
				return false
			}
		}

		return true
	})

	s.Title("Week")

	s.Assert("`Job.Every(...).Weekday(...).At(...)` set to the future causes lastRun to have occured `interval` weeks from this week and nextRun to be scheduled for this week", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

	s.Assert("`Job.Every(...).Weekday(...).At(...)` set to the past causese lastRun to have occured `interval` weeks before `now` and nextRun to be scheduled for this week", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

	s.Title("Time")

	s.Assert("`Job.Hour()` causes lastRun to be now and nextRun to be `interval` * hour(s) from now", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

	s.Assert("`Job.Minute()` causes lastRun to be now and nextRun to be `interval` * minute(s) from now", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

	s.Assert("`Job.Second()` causes lastRun to be now and nextRun to be `interval` * second(s) from now", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})
}

func TestScheduler(t *testing.T) {

	s := sugar.New(t)

	s.Assert("`runPending(...)` runs all pending jobs", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

	s.Assert("`Start()`, `IsRunning()` and `Stop()` perform correctly in asynchrnous environments", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

	s.Assert("`Start()` triggers runPending(...) every second", func(log sugar.Log) bool {
		// TODO: implement test
		return false
	})

}
