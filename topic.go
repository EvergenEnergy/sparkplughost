package sparkplughost

import (
	"fmt"
	"strings"
)

const (
	sparkplugbNamespace = "spBv1.0"
)

type topic struct {
	namespace   string
	messageType messageType
	groupID     string
	edgeNodeID  string
	deviceID    string
	hostID      string
}

func (t topic) edgeNodeDescriptor() EdgeNodeDescriptor {
	return EdgeNodeDescriptor{
		GroupID:    t.groupID,
		EdgeNodeID: t.edgeNodeID,
	}
}

func parseTopic(rawTopic string) (topic, error) {
	topicParts := strings.Split(rawTopic, "/")

	if len(topicParts) < 3 {
		return topic{}, fmt.Errorf("invalid topic: %s", rawTopic)
	}

	if topicParts[0] != sparkplugbNamespace {
		return topic{}, fmt.Errorf("invalid namespace: %s. Only spBv1.0 is allowed", topicParts[0])
	}

	// check if this is a STATE message first since that format is different from all other
	// message types.
	if topicParts[1] == "STATE" {
		if len(topicParts) != 3 || len(topicParts[2]) == 0 {
			return topic{}, fmt.Errorf("invalid topic (%s) for STATE message. It should follow the form spBv1.0/STATE/sparkplug_host_id", rawTopic)
		}

		return topic{
			namespace:   sparkplugbNamespace,
			messageType: messageTypeSTATE,
			hostID:      topicParts[2],
		}, nil
	}

	messageType, err := parseMessageType(topicParts[2])
	if err != nil {
		return topic{}, err
	}

	switch {
	case messageType.isEdgeNodeMessage():
		return parseEdgeNodeTopic(topicParts, messageType)
	case messageType.isDeviceMessage():
		return parseDeviceTopic(topicParts, messageType)
	default:
		return topic{}, fmt.Errorf("invalid topic: %s", rawTopic)
	}
}

func parseDeviceTopic(topicParts []string, messageType messageType) (topic, error) {
	rawTopic := strings.Join(topicParts, "/")

	if len(topicParts) != 5 {
		return topic{}, fmt.Errorf("invalid topic (%s) for device message. It should follow the form spBv1.0/%s/edge_node_id/device_id", rawTopic, messageType)
	}

	groupID := topicParts[1]
	if len(groupID) == 0 {
		return topic{}, fmt.Errorf("invalid topic (%s) for edge node message. Group ID is empty", rawTopic)
	}

	edgeNodeID := topicParts[3]
	if len(edgeNodeID) == 0 {
		return topic{}, fmt.Errorf("invalid topic (%s) for edge node message. Edge Node ID is empty", rawTopic)
	}

	deviceID := topicParts[4]
	if len(deviceID) == 0 {
		return topic{}, fmt.Errorf("invalid topic (%s) for edge node message. Device ID is empty", rawTopic)
	}

	return topic{
		namespace:   sparkplugbNamespace,
		messageType: messageType,
		groupID:     groupID,
		edgeNodeID:  edgeNodeID,
		deviceID:    deviceID,
	}, nil
}

func parseEdgeNodeTopic(topicParts []string, messageType messageType) (topic, error) {
	rawTopic := strings.Join(topicParts, "/")

	if len(topicParts) != 4 {
		return topic{}, fmt.Errorf("invalid topic (%s) for edge node message. It should follow the form spBv1.0/%s/edge_node_id", rawTopic, messageType)
	}

	groupID := topicParts[1]
	if len(groupID) == 0 {
		return topic{}, fmt.Errorf("invalid topic (%s) for edge node message. Group ID is empty", rawTopic)
	}

	edgeNodeID := topicParts[3]
	if len(edgeNodeID) == 0 {
		return topic{}, fmt.Errorf("invalid topic (%s) for edge node message. Edge Node ID is empty", rawTopic)
	}

	return topic{
		namespace:   sparkplugbNamespace,
		messageType: messageType,
		groupID:     groupID,
		edgeNodeID:  edgeNodeID,
	}, nil
}

func parseMessageType(messageType string) (messageType, error) {
	switch messageType {
	case "STATE":
		return messageTypeSTATE, nil
	case "NDEATH":
		return messageTypeNDEATH, nil
	case "NBIRTH":
		return messageTypeNBIRTH, nil
	case "NDATA":
		return messageTypeNDATA, nil
	case "NCMD":
		return messageTypeNCMD, nil
	case "DBIRTH":
		return messageTypeDBIRTH, nil
	case "DDEATH":
		return messageTypeDDEATH, nil
	case "DDATA":
		return messageTypeDDATA, nil
	case "DCMD":
		return messageTypeDCMD, nil
	default:
		return "", fmt.Errorf("invalid message type: %s", messageType)
	}
}
