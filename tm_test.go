package gocron

import (
	"math/rand"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	scheduler := NewTimerWheel(nil)

	scheduler.Every(1).Second().Do(task)
	scheduler.Every(1).Second().Do(taskWithParams, 1, "hello")
	stopped := scheduler.Start()

	time.Sleep(10 * time.Second)

	scheduler.Clear()

	stopped <- true
}

func TestOneTimeTimer(t *testing.T) {
	scheduler := NewTimerWheel(nil)

	scheduler.After(1).Second().Do(task)
	scheduler.After(1).Second().Do(taskWithParams, 1, "hello")
	stopped := scheduler.Start()

	time.Sleep(10 * time.Second)

	scheduler.Clear()

	stopped <- true
}

func BenchmarkSchedule(b *testing.B) {
	cfg := *DefaultTimerWheelConfig

	scheduler := NewTimerWheel(&cfg)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			scheduler.Every(uint64(rand.Intn(int(cfg.TimeSlotCount)))).Seconds().Do(task)
		}
	})
}

func BenchmarkPending(b *testing.B) {
	cfg := *DefaultTimerWheelConfig

	scheduler := NewTimerWheel(&cfg).(*timerWheel)

	jobs := make([]*Job, b.N)

	for i := 0; i < b.N; i++ {
		jobs[i] = scheduler.Every(uint64(rand.Intn(int(cfg.TimeSlotCount))))
		jobs[i].Seconds().Do(task)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.getPending(i % len(scheduler.slots))
	}
}

func BenchmarkCancel(b *testing.B) {
	cfg := *DefaultTimerWheelConfig

	scheduler := NewTimerWheel(&cfg).(*timerWheel)

	jobs := make([]*Job, b.N)

	for i := 0; i < b.N; i++ {
		jobs[i] = scheduler.Every(uint64(rand.Intn(int(cfg.TimeSlotCount))))
		jobs[i].Seconds().Do(task)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !scheduler.Cancel(jobs[i]) {
			b.Logf("missing %s @ slot #%v, %v", jobs[i], jobs[i].userData, scheduler.slots[jobs[i].userData.(int)].jobs)
			b.Fail()
		}
	}
}
