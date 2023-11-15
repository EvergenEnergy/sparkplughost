package sparkplughost

import (
	"fmt"
	"log/slog"
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
	mqttClients map[string]mqtt.Client
	logger      *slog.Logger
}

func newCommandPublisher(mqttClients map[string]mqtt.Client, logger *slog.Logger) *commandPublisher {
	return &commandPublisher{mqttClients: mqttClients, logger: logger}
}

func (c *commandPublisher) requestNodeRebirth(descriptor EdgeNodeDescriptor) error {
	c.logger.Debug("Requesting Rebirth for edge node",
		"edge_node_descriptor", descriptor.String())

	return c.writeEdgeNodeMetrics(descriptor, []*protobuf.Payload_Metric{
		{
			Name:     proto.String(nodeRebirthMetricName),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Boolean.Number())),
			Value:    &protobuf.Payload_Metric_BooleanValue{BooleanValue: true},
		},
	})
}

func (c *commandPublisher) requestDeviceRebirth(descriptor EdgeNodeDescriptor, deviceID string) error {
	c.logger.Debug("Requesting Rebirth for device",
		"edge_node_descriptor", descriptor.String(),
		"device_id", deviceID)

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

	for _, client := range c.mqttClients {
		if t := client.Publish(topic, byte(0), false, bytes); t.Wait() && t.Error() != nil {
			return err
		}
	}

	return nil
}
