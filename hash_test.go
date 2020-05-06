package gubernator

import (
	"math/rand"
	"net"
	"testing"
	"time"
)

func TestConsistantHash(t *testing.T) {
	const cases = 1024
	rand.Seed(time.Now().Unix())

	hash := NewConsistantHash(nil)

	hosts := []string{"a.svc.local", "b.svc.local", "c.svc.local"}
	hostMap := map[string]int{}

	for _, h := range hosts {
		hash.Add(&PeerClient{host: h})
		hostMap[h] = 0
	}

	for i := 0; i < cases; i++ {
		r := rand.Int31()
		ip := net.IPv4(192, byte(r>>16), byte(r>>8), byte(r))
		peer, _ := hash.Get(ip.String())
		hostMap[peer.host]++
	}

	max := 0
	for _, a := range hostMap {
		for _, b := range hostMap {
			diff := b - a
			if diff < 0 {
				diff = diff * -1
			}
			if diff > max {
				max = diff
			}
		}
	}

	t.Log("% difference", float64(max)/cases)
}

func BenchmarkConsistantHash(b *testing.B) {
	hash := NewConsistantHash(nil)

	hosts := []string{"a.svc.local", "b.svc.local", "c.svc.local"}
	for _, h := range hosts {
		hash.Add(&PeerClient{host: h})
	}

	ips := make([]string, b.N)
	for i := range ips {
		ips[i] = net.IPv4(byte(i>>24), byte(i>>16), byte(i>>8), byte(i)).String()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(ips[i])
	}

}
