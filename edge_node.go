package sparkplughost

import (
	"errors"
	"fmt"
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

type edgeNode struct {
	descriptor          EdgeNodeDescriptor
	online              bool
	lastOnlineAt        time.Time
	lastOfflineAt       time.Time
	birthSequenceNumber int64
	lastSequenceNumber  int64
}

type edgeNodeManager struct {
	mu            sync.Mutex
	nodes         map[EdgeNodeDescriptor]edgeNode
	metrics       map[EdgeNodeDescriptor]*edgeNodeMetrics
	metricHandler MetricHandler
}

func newEdgeNodeManager(metricHandler MetricHandler) *edgeNodeManager {
	return &edgeNodeManager{
		nodes:         make(map[EdgeNodeDescriptor]edgeNode),
		metrics:       make(map[EdgeNodeDescriptor]*edgeNodeMetrics),
		metricHandler: metricHandler,
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
		return err
	}

	newNode := edgeNode{
		descriptor:          edgeNodeDescriptor,
		online:              true,
		lastOnlineAt:        time.UnixMilli(int64(payload.GetTimestamp())),
		birthSequenceNumber: bdSeq,
		lastSequenceNumber:  0,
	}

	m.nodes[newNode.descriptor] = newNode
	m.metrics[newNode.descriptor] = metrics

	for _, metric := range metrics.nodeMetrics {
		m.metricHandler(metric)
	}

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

	m.nodes[edgeNodeDescriptor] = node

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
