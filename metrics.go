package sparkplughost

import (
	"fmt"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
)

// MetricQuality will be "STALE" when a given edge node or
// device looses connection to the MQTT broker.
// This represents that the data was accurate at a time, but now
// that the MQTT session has been lost can no longer be considered
// current or up to date.
type MetricQuality string

const (
	MetricQualityGood  MetricQuality = "GOOD"
	MetricQualityStale MetricQuality = "STALE"
)

// HostMetric represents the view this Host Application
// has of a particular edge node or device metric.
type HostMetric struct {
	EdgeNodeDescriptor EdgeNodeDescriptor
	DeviceID           string
	Metric             *protobuf.Payload_Metric
	Quality            MetricQuality
}

// manages all metrics for a specific edge node
// and its associated devices.
type edgeNodeMetrics struct {
	edgeNodeDescriptor EdgeNodeDescriptor
	aliasToName        map[uint64]string
	nodeMetrics        map[string]HostMetric
	deviceMetrics      map[string]map[string]HostMetric
}

func newEdgeNodeMetrics(edgeNodeDescriptor EdgeNodeDescriptor) *edgeNodeMetrics {
	return &edgeNodeMetrics{
		edgeNodeDescriptor: edgeNodeDescriptor,
		aliasToName:        make(map[uint64]string),
		nodeMetrics:        make(map[string]HostMetric),
		deviceMetrics:      make(map[string]map[string]HostMetric),
	}
}

func (m *edgeNodeMetrics) addNodeBirthMetrics(metricsProto []*protobuf.Payload_Metric) error {
	// on edge node birth we reset all aliases/metrics.
	aliasToName := make(map[uint64]string)
	metrics := make(map[string]HostMetric, len(metricsProto))

	for i, metric := range metricsProto {
		metricName := metric.GetName()

		if len(metricName) == 0 {
			return fmt.Errorf("metric name is required: metrics[%d]", i)
		}

		if alias := metric.Alias; alias != nil {
			// make sure, if supplied, that the alias is
			// unique across this Edge Node’s entire set of metrics
			if !isUniqueAlias(*alias, metricName, aliasToName) {
				return errOutOfSync
			}

			aliasToName[*alias] = metricName
		}

		metrics[metricName] = HostMetric{
			EdgeNodeDescriptor: m.edgeNodeDescriptor,
			Metric:             metric,
			Quality:            MetricQualityGood,
		}
	}

	m.aliasToName = aliasToName
	m.nodeMetrics = metrics

	return nil
}

func (m *edgeNodeMetrics) addDeviceBirthMetrics(deviceID string, metricsProto []*protobuf.Payload_Metric) error {
	deviceMetrics := make(map[string]HostMetric, len(metricsProto))

	for i, metric := range metricsProto {
		metricName := metric.GetName()

		if len(metricName) == 0 {
			return fmt.Errorf("metric name is required: metrics[%d]", i)
		}

		if alias := metric.Alias; alias != nil {
			// make sure, if supplied, that the alias is
			// unique across this Edge Node’s entire set of metrics
			if !isUniqueAlias(*alias, metricName, m.aliasToName) {
				return errOutOfSync
			}

			m.aliasToName[*alias] = metricName
		}

		deviceMetrics[metricName] = HostMetric{
			EdgeNodeDescriptor: m.edgeNodeDescriptor,
			DeviceID:           deviceID,
			Metric:             metric,
			Quality:            MetricQualityGood,
		}
	}

	m.deviceMetrics[deviceID] = deviceMetrics

	return nil
}

// check that the alias is a unique number across this Edge Node’s entire set of metrics.
func isUniqueAlias(alias uint64, metricName string, aliasToName map[uint64]string) bool {
	existingName, found := aliasToName[alias]
	if !found {
		return true
	}

	return existingName == metricName
}

func (m *edgeNodeMetrics) addNodeMetrics(metricsProto []*protobuf.Payload_Metric) error {
	for _, metric := range metricsProto {
		if metric.Name == nil && metric.Alias != nil {
			name, aliasFound := m.aliasToName[metric.GetAlias()]
			if !aliasFound {
				return errOutOfSync
			}

			metric.Name = &name
		}

		_, metricFound := m.nodeMetrics[metric.GetName()]
		if !metricFound {
			return errOutOfSync
		}

		m.nodeMetrics[metric.GetName()] = HostMetric{
			EdgeNodeDescriptor: m.edgeNodeDescriptor,
			Metric:             metric,
			Quality:            MetricQualityGood,
		}
	}

	return nil
}

func (m *edgeNodeMetrics) addDeviceMetrics(deviceID string, metricsProto []*protobuf.Payload_Metric) error {
	for _, metric := range metricsProto {
		if metric.Name == nil && metric.Alias != nil {
			name, aliasFound := m.aliasToName[metric.GetAlias()]
			if !aliasFound {
				return errOutOfSync
			}

			metric.Name = &name
		}

		if _, ok := m.deviceMetrics[deviceID]; !ok {
			return errOutOfSync
		}

		_, metricFound := m.deviceMetrics[deviceID][metric.GetName()]
		if !metricFound {
			return errOutOfSync
		}

		m.deviceMetrics[deviceID][metric.GetName()] = HostMetric{
			EdgeNodeDescriptor: m.edgeNodeDescriptor,
			DeviceID:           deviceID,
			Metric:             metric,
			Quality:            MetricQualityGood,
		}
	}

	return nil
}

func (m *edgeNodeMetrics) setNodeMetricsAsStale() {
	for k, v := range m.nodeMetrics {
		v.Quality = MetricQualityStale
		m.nodeMetrics[k] = v
	}

	// when an edge node comes offline all metrics associated with
	// its devices must also be set to STALE
	for deviceID := range m.deviceMetrics {
		m.setDeviceMetricsAsStale(deviceID)
	}
}

func (m *edgeNodeMetrics) setDeviceMetricsAsStale(deviceID string) {
	for k, v := range m.deviceMetrics[deviceID] {
		v.Quality = MetricQualityStale
		m.deviceMetrics[deviceID][k] = v
	}
}

func (m *edgeNodeMetrics) getDeviceMetrics(deviceID string) []HostMetric {
	metrics, ok := m.deviceMetrics[deviceID]
	if !ok {
		return nil
	}

	res := make([]HostMetric, 0, len(metrics))
	for _, metric := range metrics {
		res = append(res, metric)
	}

	return res
}
