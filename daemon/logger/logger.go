package logger

import (
	"sync"
	"time"
)

// Message is the structure used to carry log messages.
type Message struct {
	Source    string
	Line      []byte
	Timestamp time.Time
	Attrs     []KeyValuePair
	Err       error
}

// KeyValuePair is a key-value pair.
type KeyValuePair struct {
	Key   string
	Value string
}

// LogWatcher is used to carry log messages and errors back to the caller.
type LogWatcher struct {
	Msg               chan *Message
	Err               chan error
	watchProducerGone chan struct{}
	closeChan         chan struct{}
	once              sync.Once
	closeOnce         sync.Once
}

// NewLogWatcher returns a new LogWatcher.
func NewLogWatcher() *LogWatcher {
	return &LogWatcher{
		Msg:               make(chan *Message, 100),
		Err:               make(chan error, 1),
		watchProducerGone: make(chan struct{}),
		closeChan:         make(chan struct{}),
	}
}

// WatchProducerGone returns a channel which is closed when the producer is gone.
func (w *LogWatcher) WatchProducerGone() <-chan struct{} {
	return w.watchProducerGone
}

// ProducerGone signals that the producer is gone.
func (w *LogWatcher) ProducerGone() {
	w.once.Do(func() {
		close(w.watchProducerGone)
	})
}

// WatchClose returns a channel which is closed when the watcher is closed.
func (w *LogWatcher) WatchClose() <-chan struct{} {
	return w.closeChan
}

// Close closes the LogWatcher.
func (w *LogWatcher) Close() {
	w.closeOnce.Do(func() {
		close(w.closeChan)
	})
}
