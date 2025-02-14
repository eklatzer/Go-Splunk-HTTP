package splunk

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const (
	bufferSize       = 100
	defaultInterval  = 2 * time.Second
	defaultThreshold = 10
	defaultRetries   = 2
)

// Writer is a threadsafe, aysnchronous splunk writer.
// It implements io.Writer for usage in logging libraries, or whatever you want to send to splunk :)
// Writer.Client's configuration determines what source, sourcetype & index will be used for events
// Example for logrus:
//    splunkWriter := &splunk.Writer {Client: client}
//    logrus.SetOutput(io.MultiWriter(os.Stdout, splunkWriter))
type Writer struct {
	Client *Client
	// How often the write buffer should be flushed to splunk
	FlushInterval time.Duration
	// How many Write()'s before buffer should be flushed to splunk
	FlushThreshold int
	// Max number of retries we should do when we flush the buffer
	MaxRetries    int
	dataChan      chan *message
	errors        chan error
	once          sync.Once
	messageBuffer messageBuffer
	wg            sync.WaitGroup
}

type messageBuffer struct {
	buffer []*message
	sync.RWMutex
}

// Associates some bytes with the time they were written
// Helpful if we have long flush intervals to more precisely record the time at which
// a message was written
type message struct {
	data      json.RawMessage
	writtenAt time.Time
}

// Writer asynchronously writes to splunk in batches
func (w *Writer) Write(b []byte) (int, error) {
	// only initialize once. Keep all of our buffering in one thread
	w.once.Do(func() {
		// synchronously set up dataChan
		w.dataChan = make(chan *message, bufferSize)
		// Spin up single goroutine to listen to our writes
		w.errors = make(chan error, bufferSize)
		go w.listen()
	})

	defer func() {
		if r := recover(); r != nil {
			w.error(fmt.Errorf("failed to write: %v", r))
		}
	}()

	w.wg.Add(1)
	// Make a local copy of the bytearray so it doesn't get overwritten by
	// the next call to Write()
	var b2 = make([]byte, len(b))
	copy(b2, b)
	// Send the data to the channel
	w.dataChan <- &message{
		data:      b2,
		writtenAt: time.Now(),
	}
	// We don't know if we've hit any errors yet, so just say we're good
	return len(b), nil
}

// Errors returns a buffered channel of errors. Might be filled over time, might not
// Useful if you want to record any errors hit when sending data to splunk
func (w *Writer) Errors() <-chan error {
	return w.errors
}

// Close closes the data-channel and flushes all messages.
func (w *Writer) Close() error {
	w.wg.Wait()
	if w.dataChan != nil {
		close(w.dataChan)
	}
	return w.flush()
}

// listen for messages
func (w *Writer) listen() {
	if w.FlushInterval <= 0 {
		w.FlushInterval = defaultInterval
	}
	if w.FlushThreshold == 0 {
		w.FlushThreshold = defaultThreshold
	}

	flush := func() {
		err := w.flush()
		if err != nil {
			w.error(err)
		}
	}

	go func() {
		ticker := time.NewTicker(w.FlushInterval)
		for range ticker.C {
			flush()
		}
	}()

	for d := range w.dataChan {
		w.messageBuffer.Lock()
		w.messageBuffer.buffer = append(w.messageBuffer.buffer, d)
		flushRequired := len(w.messageBuffer.buffer) >= w.FlushThreshold
		w.messageBuffer.Unlock()

		w.wg.Done()

		if flushRequired {
			flush()
		}
	}
}

func (w *Writer) error(err error) {
	select {
	case w.errors <- err:
	// Don't block in case no one is listening or our errors channel
	default:
	}
}

func (w *Writer) flush() error {
	w.messageBuffer.Lock()
	defer w.messageBuffer.Unlock()
	if len(w.messageBuffer.buffer) == 0 {
		return nil
	}

	err := w.send(w.messageBuffer.buffer, w.MaxRetries)
	// Make a new array since the old one is getting used by the splunk client now
	w.messageBuffer.buffer = make([]*message, 0)

	return err
}

// send sends data to splunk, retrying upon failure
func (w *Writer) send(messages []*message, retries int) error {
	// Create events from our data so we can send them to splunk
	events := make([]*Event, len(messages))
	for i, m := range messages {
		// Use the configuration of the Client for the event
		events[i] = w.Client.NewEventWithTime(m.writtenAt, m.data, w.Client.Source, w.Client.SourceType, w.Client.Index)
	}
	// Send the events to splunk
	err := w.Client.LogEvents(events)
	if err == nil {
		return nil
	}

	// If we had any failures, retry as many times as they requested
	for i := 0; i < retries; i++ {
		// retry
		err = w.Client.LogEvents(events)
		if err == nil {
			return nil
		}
	}

	return err
}
