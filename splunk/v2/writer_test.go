package splunk

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func server(lock *sync.Mutex, numMessages *int, notify chan bool) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		split := strings.Split(string(b), "\n")
		num := 0
		// Since we batch our logs up before we send them:
		// Increment our messages counter by one for each JSON object we got in this response
		// We don't know how many responses we'll get, we only care about the number of messages
		for _, line := range split {
			if strings.HasPrefix(line, "{") {
				num++
				notify <- true
			}
		}
		lock.Lock()
		*numMessages += num
		lock.Unlock()
	}
}

func TestWriter_Write(t *testing.T) {
	var tests = []*struct {
		name   string
		writer *Writer
	}{
		{
			name:   "flush based on ticker",
			writer: &Writer{FlushInterval: time.Nanosecond, FlushThreshold: 1000000000},
		},
		{
			name:   "flush based on threshold",
			writer: &Writer{FlushThreshold: 10, FlushInterval: time.Hour},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			numWrites := 1000
			numMessages := 0
			lock := &sync.Mutex{}
			notify := make(chan bool, numWrites)
			server := httptest.NewServer(http.HandlerFunc(server(lock, &numMessages, notify)))

			// Create a writer that's flushing constantly. We want this test to run quickly
			test.writer.Client = NewClient(server.Client(), server.URL, "", "", "", "")
			// Send a bunch of messages in separate goroutines to make sure we're properly
			// testing Writer's concurrency promise
			for i := 0; i < numWrites; i++ {
				go test.writer.Write([]byte(fmt.Sprintf("%d", i)))
			}

			// To notify our test we've collected everything we need.
			doneChan := make(chan bool)
			go func() {
				for i := 0; i < numWrites; i++ {
					// Do nothing, just loop through to the next one
					<-notify
				}
				doneChan <- true
			}()
			select {
			case <-doneChan:
				// Do nothing, we're good
			case <-time.After(1 * time.Second):
				t.Errorf("Timed out waiting for messages")
			}

			lock.Lock()
			assert.Equal(t, numWrites, numMessages)
			lock.Unlock()
		})
	}
}

func TestClose(t *testing.T) {
	numWrites := 17
	lock := &sync.Mutex{}
	notify := make(chan bool, numWrites)
	numMessages := 0
	server := httptest.NewServer(http.HandlerFunc(server(lock, &numMessages, notify)))

	// configure writer to not automatically flush
	writer := Writer{Client: NewClient(server.Client(), server.URL, "", "", "", ""), FlushThreshold: 25, FlushInterval: time.Hour}

	for i := 0; i < numWrites; i++ {
		writer.Write([]byte(fmt.Sprintf("%d", i)))
	}

	err := writer.Close()
	assert.Nil(t, err)

	lock.Lock()
	assert.Equal(t, numWrites, numMessages)
	lock.Unlock()

	// send on closed channel and check if error is correctly send to error channel
	var wg = sync.WaitGroup{}

	wg.Add(1)
	go func() {
		select {
		case e := <-writer.Errors():
			assert.ErrorContains(t, e, "send on closed channel")
		case <-time.After(1 * time.Second):
			t.Errorf("Timed out waiting for expected error")
		}
		wg.Done()
	}()

	writer.Write([]byte("test"))
	wg.Wait()
}

func TestWriter_Errors(t *testing.T) {
	numMessages := 1000
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, "bad request")
	}))
	writer := Writer{
		Client: NewClient(server.Client(), server.URL, "", "", "", ""),
		// Will flush after the last message is sent
		FlushThreshold: numMessages - 1,
		// Don't let the flush interval cause raciness
		FlushInterval: 5 * time.Minute,
	}
	for i := 0; i < numMessages; i++ {
		_, _ = writer.Write([]byte("some data"))
	}
	select {
	case <-writer.Errors():
		// good to go, got our error
	case <-time.After(1 * time.Second):
		t.Errorf("Timed out waiting for error, should have gotten 1 error")
	}
}
