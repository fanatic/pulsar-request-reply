package timing

import (
	"fmt"
	"math"
	"sync"
	"time"
)

var globalMetrics = map[string]*Metric{}

func init() {
	globalMetrics = make(map[string]*Metric)
}

type Metric struct {
	Name       string
	subMetrics []SubMetric

	mutex sync.RWMutex
	min   uint64
	avg   uint64
	max   uint64
	ssum  uint64
	sqsum uint64
}

type SubMetric struct {
	Duration  uint64
	startTime time.Time
	m         *Metric
}

func New(name string) *Metric {
	if _, ok := globalMetrics[name]; !ok {
		globalMetrics[name] = &Metric{Name: name, min: math.MaxInt64}
	}

	return globalMetrics[name]
}

func (m *Metric) Start() *SubMetric {
	sm := SubMetric{m: m}
	m.subMetrics = append(m.subMetrics, sm)
	sm.startTime = time.Now()
	return &sm
}

func (sm *SubMetric) Stop() {
	if !sm.startTime.IsZero() {
		sm.Duration = uint64(time.Since(sm.startTime).Nanoseconds())

		sm.m.mutex.Lock()
		defer sm.m.mutex.Unlock()

		sm.m.min = min(sm.m.min, sm.Duration)
		sm.m.max = max(sm.m.max, sm.Duration)
		sm.m.ssum += sm.Duration
		sm.m.sqsum += sm.Duration * sm.Duration
	}
}

func (m *Metric) String() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	min := float64(min(m.min, m.max)) / float64(time.Millisecond)
	avg := float64(m.ssum/uint64(len(m.subMetrics))) / float64(time.Millisecond)
	max := float64(m.max) / float64(time.Millisecond)
	mdev := math.Sqrt(float64(m.sqsum)/float64(len(m.subMetrics))) / float64(time.Millisecond)

	return fmt.Sprintf("%d reqs min/avg/max/mdev = %.3f/%.3f/%.3f/%.3f ms", len(m.subMetrics), min, avg, max, mdev)
}

// max returns the max number between a and b
func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// min returns the min number between a and b
func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func Results() {
	for n, m := range globalMetrics {
		fmt.Println(n + ": " + m.String())
	}
}
