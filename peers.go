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
	queue   chan *request
	mutex   sync.Mutex
	host    string
	isOwner bool // true if this peer refers to this server instance

	ctx    context.Context
	cancel context.CancelFunc
}

type response struct {
	rl  *RateLimitResp
	err error
}

type request struct {
	request *RateLimitReq
	resp    chan *response
}

func NewPeerClient(conf BehaviorConfig, host string) (*PeerClient, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &PeerClient{
		queue: make(chan *request, 1000),
		host:  host,
		conf:  conf,

		ctx:    ctx,
		cancel: cancel,
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

	// TODO: remove batching for global if we end up implementing a HIT aggregator
	// If config asked for batching or is global rate limit
	if r.Behavior == Behavior_BATCHING ||
		r.Behavior == Behavior_GLOBAL {
		return c.getPeerRateLimitsBatch(ctx, r)
	}

	// Send a single low latency rate limit request
	resp, err := c.GetPeerRateLimits(ctx, &GetPeerRateLimitsReq{
		Requests: []*RateLimitReq{r},
	})
	if err != nil {
		return nil, err
	}
	return resp.RateLimits[0], nil
}

// GetPeerRateLimits requests a list of rate limit statuses from a peer
func (c *PeerClient) GetPeerRateLimits(ctx context.Context, r *GetPeerRateLimitsReq) (*GetPeerRateLimitsResp, error) {
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
	return c.client.UpdatePeerGlobals(ctx, r)
}

func (c *PeerClient) getPeerRateLimitsBatch(ctx context.Context, r *RateLimitReq) (*RateLimitResp, error) {
	req := request{request: r, resp: make(chan *response, 1)}

	// Enqueue the request to be sent
	c.queue <- &req

	// Wait for a response or context cancel
	select {
	case resp := <-req.resp:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.rl, nil
	case <-ctx.Done():
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
	var interval = NewInterval(c.conf.BatchWait)
	defer interval.Stop()

	var queue []*request

	for {
		select {
		case r, ok := <-c.queue:
			// If the queue has shutdown, we need to send the rest of the queue
			if !ok {
				if len(queue) > 0 {
					c.sendQueue(queue)
				}
				c.cancel()
				return
			}

			queue = append(queue, r)

			// Send the queue if we reached our batch limit
			if len(queue) == c.conf.BatchLimit {
				c.sendQueue(queue)
				queue = nil
				continue
			}

			// If this is our first queued item since last send
			// queue the next interval
			if len(queue) == 1 {
				interval.Next()
			}

		case <-interval.C:
			if len(queue) != 0 {
				c.sendQueue(queue)
				queue = nil
			}

		}
	}
}

// sendQueue sends the queue provided and returns the responses to
// waiting go routines
func (c *PeerClient) sendQueue(queue []*request) {
	var req GetPeerRateLimitsReq
	for _, r := range queue {
		req.Requests = append(req.Requests, r.request)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.conf.BatchTimeout)
	resp, err := c.client.GetPeerRateLimits(ctx, &req)
	cancel()

	// An error here indicates the entire request failed
	if err != nil {
		for _, r := range queue {
			r.resp <- &response{err: err}
		}
		return
	}

	// Unlikely, but this avoids a panic if something wonky happens
	if len(resp.RateLimits) != len(queue) {
		err = errors.New("server responded with incorrect rate limit list size")
		for _, r := range queue {
			r.resp <- &response{err: err}
		}
		return
	}

	// Provide responses to channels waiting in the queue
	for i, r := range queue {
		r.resp <- &response{rl: resp.RateLimits[i]}
	}
}

// Shutdown will gracefully shutdown the client connection, until the context is cancelled
func (c *PeerClient) Shutdown(ctx context.Context) error {
	close(c.queue)
	defer func() {
		if c.conn != nil {
			c.conn.Close()
		}
	}()

	select {
	// Block until the caller signals we should hard shutdown
	case <-ctx.Done():
		return ctx.Err()
	// Cleanly shutdown
	case <-c.ctx.Done():
		return nil
	}
}
