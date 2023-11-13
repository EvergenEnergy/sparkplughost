package sparkplughost

import (
	"fmt"
	"time"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"
)

const (
	nodeRebirthMetricName = "Node Control/Rebirth"
)

type commandPublisher struct {
	mqttClient mqtt.Client
}

func newCommandPublisher(mqttClient mqtt.Client) *commandPublisher {
	return &commandPublisher{mqttClient: mqttClient}
}

func (c *commandPublisher) requestRebirth(descriptor EdgeNodeDescriptor) error {
	return c.writeEdgeNodeMetrics(descriptor, []*protobuf.Payload_Metric{
		{
			Name:     proto.String(nodeRebirthMetricName),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Boolean.Number())),
			Value:    &protobuf.Payload_Metric_BooleanValue{BooleanValue: true},
		},
	})
}

func (c *commandPublisher) writeEdgeNodeMetrics(descriptor EdgeNodeDescriptor, metrics []*protobuf.Payload_Metric) error {
	topic := fmt.Sprintf("%s/%s/NCMD/%s", sparkplugbNamespace, descriptor.GroupID, descriptor.EdgeNodeID)
	ts := proto.Uint64(uint64(time.Now().UnixMilli()))

	payload := &protobuf.Payload{
		Timestamp: ts,
		Metrics:   metrics,
	}

	bytes, err := proto.Marshal(payload)
	if err != nil {
		return err
	}

	if t := c.mqttClient.Publish(topic, byte(0), false, bytes); t.Wait() && t.Error() != nil {
		return err
	}

	return nil
}
