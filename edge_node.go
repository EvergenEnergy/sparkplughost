package sparkplughost

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
)

// EdgeNodeDescriptor is the combination of the
// Group ID and Edge Node ID.
// No two Edge Nodes within a Sparkplug environment can have the same
// Group ID and same Edge Node ID.
type EdgeNodeDescriptor struct {
	GroupID    string
	EdgeNodeID string
}

func (e EdgeNodeDescriptor) String() string {
	return fmt.Sprintf("%s/%s", e.GroupID, e.EdgeNodeID)
}

type edgeNode struct {
	descriptor          EdgeNodeDescriptor
	online              bool
	lastOnlineAt        time.Time
	lastOfflineAt       time.Time
	birthSequenceNumber int64
	devices             map[string]device
}

type device struct {
	deviceID      string
	online        bool
	lastOnlineAt  time.Time
	lastOfflineAt time.Time
}

type edgeNodeManager struct {
	mu               sync.Mutex
	nodes            map[EdgeNodeDescriptor]edgeNode
	metrics          map[EdgeNodeDescriptor]*edgeNodeMetrics
	metricHandler    MetricHandler
	rebirthRequester rebirthRequester
	logger           *slog.Logger
}

func newEdgeNodeManager(
	metricHandler MetricHandler,
	commandPublisher rebirthRequester,
	logger *slog.Logger,
) *edgeNodeManager {
	return &edgeNodeManager{
		nodes:            make(map[EdgeNodeDescriptor]edgeNode),
		metrics:          make(map[EdgeNodeDescriptor]*edgeNodeMetrics),
		metricHandler:    metricHandler,
		rebirthRequester: commandPublisher,
		logger:           logger,
	}
}

func (m *edgeNodeManager) processMessage(msg sparkplugMessage) {
	msgTopic := msg.topic

	logger := m.logger.With(
		"message_type", msgTopic.messageType,
		"group_id", msgTopic.groupID,
		"edge_node_id", msgTopic.edgeNodeID,
		"device_id", msgTopic.deviceID,
	)
	logger.Debug("Processing message")

	var err error

	switch msgTopic.messageType {
	case messageTypeNBIRTH:
		err = m.edgeNodeOnline(msgTopic.edgeNodeDescriptor(), msg.payload)
	case messageTypeNDEATH:
		err = m.edgeNodeOffline(msgTopic.edgeNodeDescriptor(), msg.payload)
	case messageTypeNDATA:
		err = m.edgeNodeData(msgTopic.edgeNodeDescriptor(), msg.payload.Metrics)
	case messageTypeDBIRTH:
		err = m.deviceOnline(msgTopic.edgeNodeDescriptor(), msgTopic.deviceID, msg.payload)
	case messageTypeDDEATH:
		m.deviceOffline(msgTopic.edgeNodeDescriptor(), msgTopic.deviceID, msg.payload)
	case messageTypeDDATA:
		err = m.deviceData(msgTopic.edgeNodeDescriptor(), msgTopic.deviceID, msg.payload.Metrics)
	}

	if err != nil {
		logger.Error(
			"Error processing message",
			"error", err.Error(),
		)
	}
}

func (m *edgeNodeManager) edgeNodeOnline(edgeNodeDescriptor EdgeNodeDescriptor, payload *protobuf.Payload) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bdSeq, err := birthSequenceNumber(payload)
	if err != nil {
		return err
	}

	if payload.Seq == nil || payload.GetSeq() != 0 {
		return fmt.Errorf("the NBIRTH message MUST include a sequence number in the payload and it MUST have a value of 0")
	}

	metrics := newEdgeNodeMetrics(edgeNodeDescriptor)

	err = metrics.addNodeBirthMetrics(payload.GetMetrics())
	if err != nil {
		if errors.Is(err, errOutOfSync) {
			return m.rebirthRequester.requestNodeRebirth(edgeNodeDescriptor)
		}

		return err
	}

	newNode := edgeNode{
		descriptor:          edgeNodeDescriptor,
		online:              true,
		lastOnlineAt:        time.UnixMilli(int64(payload.GetTimestamp())),
		birthSequenceNumber: bdSeq,
		devices:             make(map[string]device),
	}

	m.nodes[newNode.descriptor] = newNode
	m.metrics[newNode.descriptor] = metrics

	for _, metric := range metrics.nodeMetrics {
		m.metricHandler(metric)
	}

	return nil
}

func (m *edgeNodeManager) deviceOnline(edgeNodeDescriptor EdgeNodeDescriptor, deviceID string, payload *protobuf.Payload) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, found := m.nodes[edgeNodeDescriptor]
	if !found || !node.online {
		return m.rebirthRequester.requestNodeRebirth(edgeNodeDescriptor)
	}

	nodeMetrics, found := m.metrics[edgeNodeDescriptor]
	if !found {
		return m.rebirthRequester.requestNodeRebirth(edgeNodeDescriptor)
	}

	err := nodeMetrics.addDeviceBirthMetrics(deviceID, payload.GetMetrics())
	if err != nil {
		if errors.Is(err, errOutOfSync) {
			return m.rebirthRequester.requestNodeRebirth(edgeNodeDescriptor)
		}

		return err
	}

	node.devices[deviceID] = device{
		deviceID:     deviceID,
		online:       true,
		lastOnlineAt: time.UnixMilli(int64(payload.GetTimestamp())),
	}

	// notify about the new metrics
	m.notifyDeviceMetrics(edgeNodeDescriptor, deviceID)

	return nil
}

func (m *edgeNodeManager) edgeNodeOffline(edgeNodeDescriptor EdgeNodeDescriptor, payload *protobuf.Payload) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bdSeq, err := birthSequenceNumber(payload)
	if err != nil {
		return err
	}

	node, found := m.nodes[edgeNodeDescriptor]
	if !found {
		// we received a death certificate for a node we knew nothing about
		// ignore...
		return nil
	}

	if bdSeq != node.birthSequenceNumber {
		// timing with Will Messages may result in NDEATH messages arriving after a new/next NBIRTH message
		// has been received.
		// if the birth sequences don't match it should be safe to ignore this message
		return nil
	}

	// after receiving a Node death message we should set the node and all its devices status as offline
	// using the current host application UTC timestamp.
	// All metrics (both for the node and its devices) should also be set to STALE.
	currentTime := time.Now().UTC()
	node.online = false
	node.lastOfflineAt = currentTime

	metrics, found := m.metrics[edgeNodeDescriptor]
	if found {
		metrics.setNodeMetricsAsStale()

		for _, metric := range metrics.nodeMetrics {
			m.metricHandler(metric)
		}
	}

	for deviceID, device := range node.devices {
		device.online = false
		device.lastOfflineAt = currentTime

		node.devices[deviceID] = device

		m.notifyDeviceMetrics(edgeNodeDescriptor, deviceID)
	}

	m.nodes[edgeNodeDescriptor] = node

	return nil
}

func (m *edgeNodeManager) notifyDeviceMetrics(edgeNodeDescriptor EdgeNodeDescriptor, deviceID string) {
	metrics, found := m.metrics[edgeNodeDescriptor]
	if !found {
		return
	}

	for _, metric := range metrics.getDeviceMetrics(deviceID) {
		m.metricHandler(metric)
	}
}

func (m *edgeNodeManager) deviceOffline(edgeNodeDescriptor EdgeNodeDescriptor, deviceID string, payload *protobuf.Payload) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, found := m.nodes[edgeNodeDescriptor]
	if !found {
		return
	}

	device, found := node.devices[deviceID]
	if !found {
		return
	}

	deathTimestamp := time.UnixMilli(int64(payload.GetTimestamp()))

	// The DDEATH is sent on behalf of a Sparkplug Device by a Sparkplug Edge Node. Because of this, the
	// Sparkplug payload timestamp associated with a DDEATH is considered accurate and must be used as
	// the timestamp for a Sparkplug Device being marked as offline
	device.online = false
	device.lastOfflineAt = deathTimestamp
	node.devices[deviceID] = device

	metrics, found := m.metrics[edgeNodeDescriptor]
	if !found {
		return
	}

	metrics.setDeviceMetricsAsStale(deviceID)

	m.notifyDeviceMetrics(edgeNodeDescriptor, deviceID)
}

func (m *edgeNodeManager) edgeNodeData(descriptor EdgeNodeDescriptor, newDataMetrics []*protobuf.Payload_Metric) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, found := m.nodes[descriptor]
	if !found || !node.online {
		return m.rebirthRequester.requestNodeRebirth(descriptor)
	}

	metrics, found := m.metrics[descriptor]
	if !found {
		return m.rebirthRequester.requestNodeRebirth(descriptor)
	}

	err := metrics.addNodeMetrics(newDataMetrics)
	if err != nil {
		if errors.Is(err, errOutOfSync) {
			return m.rebirthRequester.requestNodeRebirth(descriptor)
		}

		return err
	}

	for _, metric := range newDataMetrics {
		m.metricHandler(metrics.nodeMetrics[metric.GetName()])
	}

	return nil
}

func (m *edgeNodeManager) deviceData(descriptor EdgeNodeDescriptor, deviceID string, newDataMetrics []*protobuf.Payload_Metric) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, found := m.nodes[descriptor]
	if !found || !node.online {
		return m.rebirthRequester.requestNodeRebirth(descriptor)
	}

	device, found := node.devices[deviceID]
	if !found || !device.online {
		return m.rebirthRequester.requestNodeRebirth(descriptor)
	}

	metrics, found := m.metrics[descriptor]
	if !found {
		return m.rebirthRequester.requestNodeRebirth(descriptor)
	}

	err := metrics.addDeviceMetrics(deviceID, newDataMetrics)
	if err != nil {
		if errors.Is(err, errOutOfSync) {
			return m.rebirthRequester.requestNodeRebirth(descriptor)
		}

		return err
	}

	deviceMetrics := metrics.deviceMetrics[deviceID]

	for _, metric := range newDataMetrics {
		m.metricHandler(deviceMetrics[metric.GetName()])
	}

	return nil
}

func birthSequenceNumber(payload *protobuf.Payload) (int64, error) {
	for _, metric := range payload.Metrics {
		if metric.GetName() == "bdSeq" {
			return int64(metric.GetLongValue()), nil
		}
	}

	return 0, errors.New("bdSeq metric not found")
}
