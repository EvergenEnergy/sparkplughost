package sparkplughost

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
	"google.golang.org/protobuf/proto"
)

func TestForwardsMessagesArrivingInOrder(t *testing.T) {
	next := newAppendProcessor()
	processor := newInOrderProcessor(time.Second, next, newMockRebirther())

	descriptor := EdgeNodeDescriptor{
		GroupID:    "test-group",
		EdgeNodeID: "test-node",
	}

	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(1)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	expected := []sparkplugMessage{messages[0], messages[1]}
	got := next.messagesByEdgeNode[descriptor]
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", descriptor.GroupID, descriptor.EdgeNodeID)
	}
}

func TestBuffersOutOfOrderMessages(t *testing.T) {
	next := newAppendProcessor()
	processor := newInOrderProcessor(time.Second, next, newMockRebirther())

	descriptor := EdgeNodeDescriptor{
		GroupID:    "test-group",
		EdgeNodeID: "test-node",
	}
	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(2)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(4)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(1)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	// we have messages with seq number 0, 2 and 4.
	// The first one if forwarded as soon as it arrives but 2 and 4 are buffered.
	// When the message with seq == 1 arrives then we can forward, 1 and 2 but not 4
	// yet because we are still missing 3.
	expected := []sparkplugMessage{messages[0], messages[3], messages[1]}
	got := next.messagesByEdgeNode[descriptor]
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", descriptor.GroupID, descriptor.EdgeNodeID)
	}

	seq3Message := sparkplugMessage{
		topic: topic{
			messageType: messageTypeNDATA,
			groupID:     descriptor.GroupID,
			edgeNodeID:  descriptor.EdgeNodeID,
		},
		payload: &protobuf.Payload{Seq: proto.Uint64(3)},
	}
	processor.processMessage(seq3Message)

	// now that we processed the msg with seq == 3 we can finally forward
	// that one and the one with seq == 4
	expected = []sparkplugMessage{messages[0], messages[3], messages[1], seq3Message, messages[2]}
	got = next.messagesByEdgeNode[descriptor]
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", descriptor.GroupID, descriptor.EdgeNodeID)
	}
}

func TestHandlesSeqNumbersWrappingAround255(t *testing.T) {
	next := newAppendProcessor()
	processor := newInOrderProcessor(time.Second, next, newMockRebirther())

	descriptor := EdgeNodeDescriptor{
		GroupID:    "test-group",
		EdgeNodeID: "test-node",
	}

	testTopic := topic{
		messageType: messageTypeNDATA,
		groupID:     descriptor.GroupID,
		edgeNodeID:  descriptor.EdgeNodeID,
	}

	messages := make([]sparkplugMessage, 257)
	for i := 0; i < 256; i++ {
		messages[i] = sparkplugMessage{
			topic:   testTopic,
			payload: &protobuf.Payload{Seq: proto.Uint64(uint64(i))},
		}
	}

	// we got messages 0 - 255 included in the slice. Add the next one which should
	// have seq 0 again
	messages[256] = sparkplugMessage{
		topic:   testTopic,
		payload: &protobuf.Payload{Seq: proto.Uint64(uint64(0))},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	got := next.messagesByEdgeNode[descriptor]
	if !reflect.DeepEqual(messages, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", descriptor.GroupID, descriptor.EdgeNodeID)
	}
}

func TestReceivingAnNBIRTHMessageShouldResetAllState(t *testing.T) {
	next := newAppendProcessor()
	processor := newInOrderProcessor(time.Second, next, newMockRebirther())

	descriptor := EdgeNodeDescriptor{
		GroupID:    "test-group",
		EdgeNodeID: "test-node",
	}
	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(2)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(3)},
		},
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	// in this scenario we got the initial NBIRTH, which we forward
	// we then expect the next message to have seq = 1, but we get 2 and 3.
	// Those are buffered but after a while we get a new NBIRTH message (due to a rebirth request).
	// This should reset the state of the inOrderProcessor since the birth message gives us the current
	// state of all metrics.
	// So messages with seq 2 and 3 are never forwarded.
	expected := []sparkplugMessage{messages[0], messages[3]}
	got := next.messagesByEdgeNode[descriptor]
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", descriptor.GroupID, descriptor.EdgeNodeID)
	}
}

func TestHandlesMessagesFromDifferentEdgeNodesIndependently(t *testing.T) {
	next := newAppendProcessor()
	processor := newInOrderProcessor(time.Second, next, newMockRebirther())

	edgeNode1 := EdgeNodeDescriptor{
		GroupID:    "test-group",
		EdgeNodeID: "test-node-1",
	}
	edgeNode2 := EdgeNodeDescriptor{
		GroupID:    "test-group",
		EdgeNodeID: "test-node-2",
	}

	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     edgeNode1.GroupID,
				edgeNodeID:  edgeNode1.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     edgeNode1.GroupID,
				edgeNodeID:  edgeNode1.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(1)},
		},
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     edgeNode2.GroupID,
				edgeNodeID:  edgeNode2.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     edgeNode1.GroupID,
				edgeNodeID:  edgeNode1.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(2)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     edgeNode2.GroupID,
				edgeNodeID:  edgeNode2.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(1)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	expected := []sparkplugMessage{messages[0], messages[1], messages[3]}
	got := next.messagesByEdgeNode[edgeNode1]
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", edgeNode1.GroupID, edgeNode1.EdgeNodeID)
	}

	expected = []sparkplugMessage{messages[2], messages[4]}
	got = next.messagesByEdgeNode[edgeNode2]
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected messages for edge node %s/%s to be in order but weren't", edgeNode2.GroupID, edgeNode2.EdgeNodeID)
	}
}

func TestRequestsANodeRebirthIfTheOrderIsNotRestoredBeforeTheTimeout(t *testing.T) {
	rebirther := newMockRebirther()
	reorderTimeout := 50 * time.Millisecond
	processor := newInOrderProcessor(reorderTimeout, newAppendProcessor(), rebirther)

	descriptor := EdgeNodeDescriptor{GroupID: "test-group", EdgeNodeID: "test-node"}
	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(2)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	time.Sleep(reorderTimeout + 10*time.Millisecond)

	if got := rebirther.rebirthCountForEdgeNode(descriptor); got != 1 {
		t.Errorf("expected 1 rebirth request for node %s but got %d", descriptor.String(), got)
	}
}

func TestDoesntRequestRebirthIfOrderIsRestoredBeforeTimeout(t *testing.T) {
	rebirther := newMockRebirther()
	reorderTimeout := 50 * time.Millisecond
	processor := newInOrderProcessor(reorderTimeout, newAppendProcessor(), rebirther)

	descriptor := EdgeNodeDescriptor{GroupID: "test-group", EdgeNodeID: "test-node"}
	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(2)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	time.Sleep(10 * time.Millisecond)

	processor.processMessage(sparkplugMessage{
		topic: topic{
			messageType: messageTypeNDATA,
			groupID:     descriptor.GroupID,
			edgeNodeID:  descriptor.EdgeNodeID,
		},
		payload: &protobuf.Payload{Seq: proto.Uint64(1)},
	})

	// sleep for the timeout duration to make sure the timer doesn't fire
	// after processing the message.
	time.Sleep(reorderTimeout)

	if got := rebirther.rebirthCountForEdgeNode(descriptor); got != 0 {
		t.Errorf("expected 0 rebirth request for node %s but got %d", descriptor.String(), got)
	}
}

func TestShouldNotRequestRebirthIfNBIRTHArrives(t *testing.T) {
	rebirther := newMockRebirther()
	reorderTimeout := 50 * time.Millisecond
	processor := newInOrderProcessor(reorderTimeout, newAppendProcessor(), rebirther)

	descriptor := EdgeNodeDescriptor{GroupID: "test-group", EdgeNodeID: "test-node"}
	messages := []sparkplugMessage{
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
		{
			topic: topic{
				messageType: messageTypeNDATA,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(2)},
		},
		{
			topic: topic{
				messageType: messageTypeNBIRTH,
				groupID:     descriptor.GroupID,
				edgeNodeID:  descriptor.EdgeNodeID,
			},
			payload: &protobuf.Payload{Seq: proto.Uint64(0)},
		},
	}

	for _, msg := range messages {
		processor.processMessage(msg)
	}

	time.Sleep(reorderTimeout + 10*time.Millisecond)

	if got := rebirther.rebirthCountForEdgeNode(descriptor); got != 0 {
		t.Errorf("expected 0 rebirth request for node %s but got %d", descriptor.String(), got)
	}
}

type appendProcessor struct {
	messagesByEdgeNode map[EdgeNodeDescriptor][]sparkplugMessage
}

func newAppendProcessor() *appendProcessor {
	return &appendProcessor{messagesByEdgeNode: make(map[EdgeNodeDescriptor][]sparkplugMessage)}
}

func (a *appendProcessor) processMessage(msg sparkplugMessage) {
	a.messagesByEdgeNode[msg.topic.edgeNodeDescriptor()] = append(a.messagesByEdgeNode[msg.topic.edgeNodeDescriptor()], msg)
}

type mockRebirther struct {
	rebirthsPerNode map[EdgeNodeDescriptor]int
	mu              sync.Mutex
}

func newMockRebirther() *mockRebirther {
	return &mockRebirther{rebirthsPerNode: make(map[EdgeNodeDescriptor]int)}
}

func (m *mockRebirther) requestNodeRebirth(descriptor EdgeNodeDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.rebirthsPerNode[descriptor]++
	return nil
}

func (m *mockRebirther) rebirthCountForEdgeNode(descriptor EdgeNodeDescriptor) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.rebirthsPerNode[descriptor]
}
