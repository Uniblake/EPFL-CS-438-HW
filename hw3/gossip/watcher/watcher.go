package watcher

import "sync"

// SimpleWatcher is a simple watcher
//
// - implements watcher.Watcher
type SimpleWatcher struct {
	sync.Mutex

	// a bag of observers
	observers map[observable]struct{}
}

// NewSimpleWatcher returns a new simple watcher
func NewSimpleWatcher() Watcher {
	return &SimpleWatcher{
		observers: make(map[observable]struct{}),
	}
}

// Add implements watcher.Watcher
func (w *SimpleWatcher) Add(o observable) {
	w.Lock()
	defer w.Unlock()

	w.observers[o] = struct{}{}
}

// Remove implements watcher.Watcher
func (w *SimpleWatcher) Remove(o observable) {
	w.Lock()
	defer w.Unlock()

	delete(w.observers, o)
}

// Notify implements watcher.Watcher
func (w *SimpleWatcher) Notify(i interface{}) {
	w.Lock()
	defer w.Unlock()

	for o := range w.observers {
		o.Notify(i)
	}
}
