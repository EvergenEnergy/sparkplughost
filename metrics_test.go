package sparkplughost

import (
	"testing"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
	"google.golang.org/protobuf/proto"
)

func TestAddNodeBirthMetricsMissingName(t *testing.T) {
	manager := newEdgeNodeMetrics(EdgeNodeDescriptor{})
	metrics := []*protobuf.Payload_Metric{
		{
			Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
			Value:    &protobuf.Payload_Metric_LongValue{LongValue: 11},
		},
	}

	err := manager.addNodeBirthMetrics(metrics)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

func TestAddNodeBirthMetricsDuplicatedAlias(t *testing.T) {
	manager := newEdgeNodeMetrics(EdgeNodeDescriptor{})
	metrics := []*protobuf.Payload_Metric{
		{
			Name:     proto.String("foo"),
			Alias:    proto.Uint64(1),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
			Value:    &protobuf.Payload_Metric_LongValue{LongValue: 11},
		},
		{
			Name:     proto.String("bar"),
			Alias:    proto.Uint64(1),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
			Value:    &protobuf.Payload_Metric_LongValue{LongValue: 11},
		},
	}

	err := manager.addNodeBirthMetrics(metrics)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

func TestAddNodeBirthMetricsValidMetrics(t *testing.T) {
	manager := newEdgeNodeMetrics(EdgeNodeDescriptor{})
	metrics := []*protobuf.Payload_Metric{
		{
			Name:     proto.String("foo"),
			Alias:    proto.Uint64(1),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
			Value:    &protobuf.Payload_Metric_LongValue{LongValue: 11},
		},
		{
			Name:     proto.String("bar"),
			Alias:    proto.Uint64(2),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
			Value:    &protobuf.Payload_Metric_LongValue{LongValue: 11},
		},
	}

	err := manager.addNodeBirthMetrics(metrics)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got, want := manager.aliasToName[1], "foo"; got != want {
		t.Errorf("got %s, wanted %s", got, want)
	}

	if got, want := manager.aliasToName[2], "bar"; got != want {
		t.Errorf("got %s, wanted %s", got, want)
	}

	if got, want := manager.nodeMetrics["foo"].Quality, MetricQualityGood; got != want {
		t.Errorf("got %s, wanted %s", got, want)
	}

	if got, want := manager.nodeMetrics["bar"].Quality, MetricQualityGood; got != want {
		t.Errorf("got %s, wanted %s", got, want)
	}
}
