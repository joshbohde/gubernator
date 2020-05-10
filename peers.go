/*
Copyright 2018-2019 Mailgun Technologies Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gubernator

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// ErrClosing is the error returned when the client is closing
var ErrClosing = errors.New("closing")

type PeerPicker interface {
	GetPeerByHost(host string) *PeerClient
	Peers() []*PeerClient
	Get(string) (*PeerClient, error)
	New() PeerPicker
	Add(*PeerClient)
	Size() int
}

type PeerClient struct {
	client  PeersV1Client
	conn    *grpc.ClientConn
	conf    BehaviorConfig
	host    string
	isOwner bool // true if this peer refers to this server instance

	queue     Queue
	interval  Interval
	mutex     sync.RWMutex // This mutex is for verifying the closing state of the client
	isClosing bool
	wg        sync.WaitGroup // This wait group is to monitor the number of in-flight requests
}

type response struct {
	rl  *RateLimitResp
	err error
}

type request struct {
	request *RateLimitReq
	resp    response
}

type requestBatch struct {
	requests []*request
	done     chan struct{}
}

func (r requestBatch) Copy() requestBatch {
	reqs := make([]*request, len(r.requests))
	copy(reqs, r.requests)
	r.requests = reqs
	return r
}

func NewPeerClient(conf BehaviorConfig, host string) (*PeerClient, error) {
	c := &PeerClient{
		queue:    NewQueue(conf.BatchLimit),
		host:     host,
		conf:     conf,
		interval: *NewInterval(conf.BatchWait),
	}

	if err := c.dialPeer(); err != nil {
		return nil, err
	}

	go c.run()

	return c, nil
}

// GetPeerRateLimit forwards a rate limit request to a peer. If the rate limit has `behavior == BATCHING` configured
// this method will attempt to batch the rate limits
func (c *PeerClient) GetPeerRateLimit(ctx context.Context, r *RateLimitReq) (*RateLimitResp, error) {
	// If config asked for no batching
	if HasBehavior(r.Behavior, Behavior_NO_BATCHING) {
		// Send a single low latency rate limit request
		resp, err := c.GetPeerRateLimits(ctx, &GetPeerRateLimitsReq{
			Requests: []*RateLimitReq{r},
		})
		if err != nil {
			return nil, err
		}
		return resp.RateLimits[0], nil
	}
	return c.getPeerRateLimitsBatch(ctx, r)
}

// GetPeerRateLimits requests a list of rate limit statuses from a peer
func (c *PeerClient) GetPeerRateLimits(ctx context.Context, r *GetPeerRateLimitsReq) (*GetPeerRateLimitsResp, error) {
	c.mutex.RLock()
	if c.isClosing {
		c.mutex.RUnlock()
		return nil, ErrClosing
	}
	c.mutex.RUnlock()

	c.wg.Add(1)
	defer c.wg.Done()

	resp, err := c.client.GetPeerRateLimits(ctx, r)
	if err != nil {
		return nil, err
	}

	// Unlikely, but this avoids a panic if something wonky happens
	if len(resp.RateLimits) != len(r.Requests) {
		return nil, errors.New("number of rate limits in peer response does not match request")
	}
	return resp, nil
}

// UpdatePeerGlobals sends global rate limit status updates to a peer
func (c *PeerClient) UpdatePeerGlobals(ctx context.Context, r *UpdatePeerGlobalsReq) (*UpdatePeerGlobalsResp, error) {
	c.mutex.RLock()
	if c.isClosing {
		c.mutex.RUnlock()
		return nil, ErrClosing
	}
	c.mutex.RUnlock()

	c.wg.Add(1)
	defer c.wg.Done()

	return c.client.UpdatePeerGlobals(ctx, r)
}

func (c *PeerClient) getPeerRateLimitsBatch(ctx context.Context, r *RateLimitReq) (*RateLimitResp, error) {
	req := request{request: r}

	c.mutex.RLock()
	if c.isClosing {
		c.mutex.RUnlock()
		return nil, ErrClosing
	}

	c.wg.Add(1)

	// Unlock to prevent the chan from being closed
	c.mutex.RUnlock()

	c.interval.Next()

	c.queue.Lock()
	done := c.queue.Enqueue(&req)
	c.queue.Unlock()

	select {
	case <-done:
		c.wg.Done()
		return req.resp.rl, req.resp.err
	case <-ctx.Done():
		c.wg.Done()
		return nil, ctx.Err()
	}
}

// dialPeer dials a peer and initializes the GRPC client
func (c *PeerClient) dialPeer() error {
	var err error
	c.conn, err = grpc.Dial(c.host, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "failed to dial peer %s", c.host)
	}

	c.client = NewPeersV1Client(c.conn)
	return nil
}

// run waits for requests to be queued, when either c.batchWait time
// has elapsed or the queue reaches c.batchLimit. Send what is in the queue.
func (c *PeerClient) run() {
	defer c.interval.Stop()

	for {
		select {
		case batch, ok := <-c.queue.Batch:
			c.interval.Reset()
			if !ok {
				return
			}
			go c.sendQueue(batch.Copy())

		case <-c.interval.C:
			c.queue.Lock()
			batch, ok := c.queue.Get()
			c.queue.Unlock()

			if ok {
				go c.sendQueue(batch.Copy())
			}

		}
	}
}

// sendQueue sends the queue provided and returns the responses to
// waiting go routines
func (c *PeerClient) sendQueue(batch requestBatch) {
	queue := batch.requests
	defer close(batch.done)

	req := GetPeerRateLimitsReq{
		Requests: make([]*RateLimitReq, len(queue)),
	}

	for i := range queue {
		req.Requests[i] = queue[i].request
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.conf.BatchTimeout)
	resp, err := c.client.GetPeerRateLimits(ctx, &req)
	cancel()

	// An error here indicates the entire request failed
	if err != nil {
		for _, r := range queue {
			r.resp.err = err
		}
		return
	}

	// Unlikely, but this avoids a panic if something wonky happens
	if len(resp.RateLimits) != len(queue) {
		err = errors.New("server responded with incorrect rate limit list size")
		for _, r := range queue {
			r.resp.err = err
		}
		return
	}

	// Provide responses to channels waiting in the queue
	for i, r := range queue {
		r.resp.rl = resp.RateLimits[i]
	}
}

// Shutdown will gracefully shutdown the client connection, until the context is cancelled
func (c *PeerClient) Shutdown(ctx context.Context) error {
	// Take the write lock since we're going to modify the closing state
	c.mutex.Lock()
	if c.isClosing {
		c.mutex.Unlock()
		return nil
	}

	c.isClosing = true

	// We need to close the chan here to prevent a possible race
	c.queue.Close()

	c.mutex.Unlock()

	defer func() {
		if c.conn != nil {
			c.conn.Close()
		}
	}()

	// This allows us to wait on the waitgroup, or until the context
	// has been cancelled. This doesn't leak goroutines, because
	// closing the connection will kill any outstanding requests.
	waitChan := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChan:
		return nil
	}
}
