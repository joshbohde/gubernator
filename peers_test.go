package gubernator

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestPeerClient(t *testing.T) {
	t.Run("Shutdown", func(t *testing.T) {
		t.Run("Empty queue", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			v1Client := NewMockPeersV1Client(ctrl)

			client := NewTestPeerClient(v1Client)

			err := client.Shutdown(context.Background())
			assert.NoError(t, err)
		})

		t.Run("Pending requests", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			v1Client := NewMockPeersV1Client(ctrl)

			peerLimitsRequest := GetPeerRateLimitsReq{
				Requests: []*RateLimitReq{&RateLimitReq{
					Behavior: Behavior_BATCHING,
					Hits:     1,
					Limit:    100,
				}},
			}
			peerLimitsResponse := GetPeerRateLimitsResp{
				RateLimits: []*RateLimitResp{&RateLimitResp{
					Limit:     100,
					Remaining: 99,
				}},
			}

			v1Client.EXPECT().
				GetPeerRateLimits(gomock.Any(), &peerLimitsRequest).
				Return(&peerLimitsResponse, nil)

			peerClient := NewTestPeerClient(v1Client)

			req := request{
				resp:    make(chan *response, 1),
				request: peerLimitsRequest.Requests[0],
			}
			peerClient.queue <- &req

			err := peerClient.Shutdown(context.Background())
			assert.NoError(t, err)

			resp := <-req.resp
			assert.NotNil(t, resp)
			assert.NoError(t, resp.err)
			assert.Equal(t, resp.rl, peerLimitsResponse.RateLimits[0])
		})
	})

}

func NewTestPeerClient(v1Client PeersV1Client) PeerClient {
	ctx, cancel := context.WithCancel(context.Background())

	peer := PeerClient{
		host:   "localhost:81",
		client: v1Client,
		queue:  make(chan *request, 1),
		ctx:    ctx,
		cancel: cancel,
	}

	go peer.run()

	return peer
}
