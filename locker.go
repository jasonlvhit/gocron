package gocron


// Locker provides a method to lock jobs from running
// at the same time on multiple instances of gocron.
// You can provide any locker implementation you wish.
type Locker interface {
    Lock(key string) (bool, error)
    Unlock(key string) error
}
