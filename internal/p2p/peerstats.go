package p2p

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// peerStats tracks per-peer download throughput and uses a UCB1 multi-armed
// bandit score to determine pipeline depth (backlog). Faster peers receive
// a larger backlog, letting them saturate their bandwidth without choking
// slower peers with excessive in-flight requests.
//
// Reference: Karras et al. "Multi-armed Bandit Strategies for Adaptive
// BitTorrent Peer Selection", ICR 2022.
type peerStats struct {
	assigned int64 // total pieces assigned to this peer (accessed atomically)

	mu       sync.Mutex
	ewmaTput float64 // bytes/sec, exponential moving average (alpha = 0.2)
}

// update records a completed piece download and refreshes the EWMA throughput.
func (ps *peerStats) update(bytes int, elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	tput := float64(bytes) / elapsed.Seconds()
	ps.mu.Lock()
	if ps.ewmaTput == 0 {
		ps.ewmaTput = tput
	} else {
		ps.ewmaTput = 0.2*tput + 0.8*ps.ewmaTput
	}
	ps.mu.Unlock()
}

// ucbBacklog computes the pipeline depth for this peer using a UCB1-inspired
// formula. The score balances exploitation (known-fast peers get bigger
// backlogs) with exploration (under-used peers get a bonus to test their
// speed). The result is clamped to [initBacklog, maxBacklog].
//
//	score = ewmaTput + C * sqrt( ln(totalAssigned) / assigned )
func (ps *peerStats) ucbBacklog(totalAssigned int64) int {
	assigned := atomic.LoadInt64(&ps.assigned)
	if assigned == 0 || totalAssigned == 0 {
		return initBacklog // unexplored peer gets a fair starting depth
	}

	ps.mu.Lock()
	mean := ps.ewmaTput
	ps.mu.Unlock()

	if mean <= 0 {
		return initBacklog
	}

	// C is tuned so that the exploration bonus equals ~1 MB/s when a peer has
	// been used once and the total is 100 — a reasonable warm-up incentive.
	const C = 2e6 // bytes/sec
	ucb := mean + C*math.Sqrt(math.Log(float64(totalAssigned))/float64(assigned))

	// Normalize against a 50 MB/s reference to map to [initBacklog, maxBacklog].
	const refTput = 50 * 1024 * 1024.0
	norm := ucb / refTput
	if norm > 1 {
		norm = 1
	}
	return initBacklog + int(norm*float64(maxBacklog-initBacklog))
}
