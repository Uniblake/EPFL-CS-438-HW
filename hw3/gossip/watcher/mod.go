package watcher

type observable interface {
	Notify(interface{})
}

// Watcher describes the primitives for a watcher that can support an
// observer-like pattern.
type Watcher interface {
	Add(observable)
	Remove(observable)
	Notify(interface{})
}
