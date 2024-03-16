package sparkplughost

import (
	"fmt"
	"sync"
)

// CachingHandler is a MetricHandler that stores the latest known state
// of every metric in memory.
// It allows clients to get a snapshot view of all the current Edge Nodes
// and Devices on-demand.
// All operations in this handler are safe for concurrent use.
type CachingHandler struct {
	mu      sync.RWMutex
	metrics map[string]HostMetric

	// reverse indexes from group/node/device id
	// to all metric keys for that component in the
	// metrics map above
	byGroup    map[string]metricKeySet
	byEdgeNode map[EdgeNodeDescriptor]metricKeySet
	byDevice   map[string]metricKeySet
}

type metricKeySet map[string]struct{}

// NewCachingHandler returns a new handler ready to be used.
func NewCachingHandler() *CachingHandler {
	return &CachingHandler{
		metrics:    make(map[string]HostMetric),
		byGroup:    make(map[string]metricKeySet),
		byEdgeNode: make(map[EdgeNodeDescriptor]metricKeySet),
		byDevice:   make(map[string]metricKeySet),
	}
}

// HandleMetric is a MetricHandler function that can be passed to
// WithMetricHandler when creating the HostApplication.
func (c *CachingHandler) HandleMetric(metric HostMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	metricKey := metricKey(metric)
	deviceKey := deviceKey(metric.EdgeNodeDescriptor, metric.DeviceID)

	c.metrics[metricKey] = metric

	if _, ok := c.byGroup[metric.EdgeNodeDescriptor.GroupID]; !ok {
		c.byGroup[metric.EdgeNodeDescriptor.GroupID] = make(metricKeySet)
	}

	c.byGroup[metric.EdgeNodeDescriptor.GroupID][metricKey] = struct{}{}

	if _, ok := c.byEdgeNode[metric.EdgeNodeDescriptor]; !ok {
		c.byEdgeNode[metric.EdgeNodeDescriptor] = make(metricKeySet)
	}

	c.byEdgeNode[metric.EdgeNodeDescriptor][metricKey] = struct{}{}

	if _, ok := c.byDevice[deviceKey]; !ok {
		c.byDevice[deviceKey] = make(metricKeySet)
	}

	c.byDevice[deviceKey][metricKey] = struct{}{}
}

// AllMetrics returns all currently known metrics in the MQTT infrastructure.
func (c *CachingHandler) AllMetrics() []HostMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res := make([]HostMetric, 0, len(c.metrics))
	for _, metric := range c.metrics {
		res = append(res, metric)
	}

	return res
}

// DeviceMetrics returns all known metrics for a specific Device.
func (c *CachingHandler) DeviceMetrics(descriptor EdgeNodeDescriptor, deviceID string) []HostMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()

	deviceMetrics, found := c.byDevice[deviceKey(descriptor, deviceID)]
	if !found {
		return nil
	}

	return c.collectMetrics(deviceMetrics)
}

// EdgeNodeMetrics returns all known metrics for a specific Edge Node.
// This includes the metrics for Devices associated with that Edge Node.
func (c *CachingHandler) EdgeNodeMetrics(descriptor EdgeNodeDescriptor) []HostMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeMetrics, found := c.byEdgeNode[descriptor]
	if !found {
		return nil
	}

	return c.collectMetrics(nodeMetrics)
}

// GroupMetrics returns all known metrics for a specific Group.
// This includes all Edge Nodes and Devices in that group.
func (c *CachingHandler) GroupMetrics(groupID string) []HostMetric {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groupMetrics, found := c.byGroup[groupID]
	if !found {
		return nil
	}

	return c.collectMetrics(groupMetrics)
}

func metricKey(metric HostMetric) string {
	return fmt.Sprintf("%s/%s/%s", metric.EdgeNodeDescriptor, metric.DeviceID, metric.Metric.GetName())
}

func deviceKey(descriptor EdgeNodeDescriptor, deviceID string) string {
	return fmt.Sprintf("%s/%s", descriptor, deviceID)
}

func (c *CachingHandler) collectMetrics(metricKeys metricKeySet) []HostMetric {
	res := make([]HostMetric, 0, len(metricKeys))

	for i := range metricKeys {
		metric, ok := c.metrics[i]
		if !ok {
			continue
		}

		res = append(res, metric)
	}

	return res
}
