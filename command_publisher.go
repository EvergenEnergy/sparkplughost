package sparkplughost

import (
	"fmt"
	"time"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"
)

const (
	nodeRebirthMetricName   = "Node Control/Rebirth"
	deviceRebirthMetricName = "Device Control/Rebirth"
)

type commandPublisher struct {
	mqttClient mqtt.Client
}

func newCommandPublisher(mqttClient mqtt.Client) *commandPublisher {
	return &commandPublisher{mqttClient: mqttClient}
}

func (c *commandPublisher) requestNodeRebirth(descriptor EdgeNodeDescriptor) error {
	return c.writeEdgeNodeMetrics(descriptor, []*protobuf.Payload_Metric{
		{
			Name:     proto.String(nodeRebirthMetricName),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Boolean.Number())),
			Value:    &protobuf.Payload_Metric_BooleanValue{BooleanValue: true},
		},
	})
}

func (c *commandPublisher) requestDeviceRebirth(descriptor EdgeNodeDescriptor, deviceID string) error {
	return c.writeDeviceMetrics(descriptor, deviceID, []*protobuf.Payload_Metric{
		{
			Name:     proto.String(deviceRebirthMetricName),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Boolean.Number())),
			Value:    &protobuf.Payload_Metric_BooleanValue{BooleanValue: true},
		},
	})
}

func (c *commandPublisher) writeEdgeNodeMetrics(descriptor EdgeNodeDescriptor, metrics []*protobuf.Payload_Metric) error {
	topic := fmt.Sprintf("%s/%s/NCMD/%s", sparkplugbNamespace, descriptor.GroupID, descriptor.EdgeNodeID)
	return c.publish(topic, metrics)
}

func (c *commandPublisher) writeDeviceMetrics(descriptor EdgeNodeDescriptor, deviceID string, metrics []*protobuf.Payload_Metric) error {
	topic := fmt.Sprintf("%s/%s/DCMD/%s/%s", sparkplugbNamespace, descriptor.GroupID, descriptor.EdgeNodeID, deviceID)
	return c.publish(topic, metrics)
}

func (c *commandPublisher) publish(topic string, metrics []*protobuf.Payload_Metric) error {
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
