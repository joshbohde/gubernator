package gubernator

import (
	"sync"
)

type Queue struct {
	buffer    []*request
	len       int
	offset    int
	batchSize int

	done chan struct{}

	Batch chan requestBatch

	sync.Mutex
}

func NewQueue(batchSize int) Queue {
	return Queue{
		buffer:    make([]*request, 2*batchSize),
		done:      make(chan struct{}),
		batchSize: batchSize,
		Batch:     make(chan requestBatch),
	}
}

func (q *Queue) Enqueue(r *request) chan struct{} {
	// Reset our done struct if necessary
	if q.done == nil {
		q.done = make(chan struct{})
	}

	index := q.offset + q.len
	q.buffer[index] = r
	q.len++

	done := q.done

	if q.len >= q.batchSize {
		q.send()
	}

	return done
}

func (q *Queue) reset() requestBatch {
	reqs := q.buffer[q.offset : q.offset+q.len]

	q.len = 0

	if q.offset == q.batchSize {
		q.offset = 0
	} else {
		q.offset = q.batchSize
	}

	done := q.done

	// This will be reset on next queue
	q.done = nil

	return requestBatch{
		requests: reqs,
		done:     done,
	}
}

func (q *Queue) send() {
	q.Batch <- q.reset()
}

func (q *Queue) Get() (requestBatch, bool) {
	if q.len == 0 {
		return requestBatch{}, false
	}

	return q.reset(), true
}

func (q *Queue) Close() {
	q.Lock()

	if q.len > 0 {
		q.Batch <- q.reset()
	}

	close(q.Batch)
	q.Unlock()
}
