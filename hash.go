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
	"hash/crc32"
	"sort"

	"github.com/pkg/errors"
)

type HashFunc func(data []byte) uint32

// Implements PeerPicker
type ConsistantHash struct {
	hashFunc HashFunc
	peerKeys []*PeerClient
}

func NewConsistantHash(fn HashFunc) *ConsistantHash {
	ch := &ConsistantHash{
		hashFunc: fn,
	}

	if ch.hashFunc == nil {
		ch.hashFunc = crc32.ChecksumIEEE
	}
	return ch
}

func (ch *ConsistantHash) New() PeerPicker {
	return &ConsistantHash{
		hashFunc: ch.hashFunc,
	}
}

func (ch *ConsistantHash) Peers() []*PeerClient {
	return ch.peerKeys
}

// Adds a peer to the hash
func (ch *ConsistantHash) Add(peer *PeerClient) {
	ch.peerKeys = append(ch.peerKeys, peer)
	sort.SliceStable(ch.peerKeys, func(i, j int) bool {
		return ch.hashFunc([]byte(ch.peerKeys[i].host)) < ch.hashFunc([]byte(ch.peerKeys[j].host))
	})
}

// Returns number of peers in the picker
func (ch *ConsistantHash) Size() int {
	return len(ch.peerKeys)
}

// Returns the peer by hostname
func (ch *ConsistantHash) GetPeerByHost(host string) *PeerClient {
	for _, p := range ch.peerKeys {
		if p.host == host {
			return p
		}
	}
	return nil
}

// Given a key, return the peer that key is assigned too
func (ch *ConsistantHash) Get(key string) (*PeerClient, error) {
	if ch.Size() == 0 {
		return nil, errors.New("unable to pick a peer; pool is empty")
	}

	hash := int(ch.hashFunc([]byte(key)))

	idx := hash % len(ch.peerKeys)

	return ch.peerKeys[idx], nil
}
