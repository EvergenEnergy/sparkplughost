package sparkplughost_test

import (
	"testing"

	"github.com/EvergenEnergy/sparkplughost"
	"github.com/EvergenEnergy/sparkplughost/protobuf"
	"google.golang.org/protobuf/proto"
)

var metrics = []sparkplughost.HostMetric{
	{
		EdgeNodeDescriptor: sparkplughost.EdgeNodeDescriptor{GroupID: "group-1", EdgeNodeID: "node-1"},
		Metric:             &protobuf.Payload_Metric{Name: proto.String("node-metric-1")},
	},
	{
		EdgeNodeDescriptor: sparkplughost.EdgeNodeDescriptor{GroupID: "group-1", EdgeNodeID: "node-1"},
		DeviceID:           "device-1",
		Metric:             &protobuf.Payload_Metric{Name: proto.String("device-metric-1")},
	},
	{
		EdgeNodeDescriptor: sparkplughost.EdgeNodeDescriptor{GroupID: "group-2", EdgeNodeID: "node-2"},
		DeviceID:           "device-2",
		Metric:             &protobuf.Payload_Metric{Name: proto.String("device-metric-2")},
	},
}

func TestCachingHandler(t *testing.T) {
	handler := sparkplughost.NewCachingHandler()

	for _, metric := range metrics {
		handler.HandleMetric(metric)
	}

	groupMetrics := handler.GroupMetrics("group-2")
	if len(groupMetrics) != 1 {
		t.Errorf("expected 1 metric for group-2 but got %d", len(groupMetrics))
	}

	groupMetrics = handler.GroupMetrics("group-1")
	if len(groupMetrics) != 2 {
		t.Errorf("expected 2 metrics for group-1 but got %d", len(groupMetrics))
	}

	groupMetrics = handler.GroupMetrics("group-3")
	if len(groupMetrics) != 0 {
		t.Errorf("expected 0 metrics for group-3 but got %d", len(groupMetrics))
	}

	edgeNodeMetrics := handler.EdgeNodeMetrics(sparkplughost.EdgeNodeDescriptor{
		GroupID:    "group-1",
		EdgeNodeID: "node-1",
	})
	if len(edgeNodeMetrics) != 2 {
		t.Errorf("expected 2 metric for node-1 but got %d", len(edgeNodeMetrics))
	}

	deviceMetrics := handler.DeviceMetrics(sparkplughost.EdgeNodeDescriptor{
		GroupID:    "group-1",
		EdgeNodeID: "node-1",
	}, "device-1")
	if len(deviceMetrics) != 1 {
		t.Errorf("expected 1 metric for device-1 but got %d", len(deviceMetrics))
	}

	allMetrics := handler.AllMetrics()
	if len(allMetrics) != 3 {
		t.Errorf("expected 3 metric but got %d", len(allMetrics))
	}
}
