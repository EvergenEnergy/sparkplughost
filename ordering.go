package sparkplughost

import (
	"sync"
	"time"
)

type rebirthRequester interface {
	requestNodeRebirth(descriptor EdgeNodeDescriptor) error
}

// inOrderProcessor is a messageProcessor that handles the message ordering
// requirements of the Sparkplug spec.
// If messages arrive out of order it will buffer them internally until
// the order is restored and then forward them to the next processor.
// If the order is not restored within the reorderTimeout config it will
// request a Rebirth from the node in order to reset its state.
type inOrderProcessor struct {
	// the ordering guarantees apply in the context of a specific Edge Node.
	// So each one will have independent buffers, timers, etc.
	edgeNodeProcessors map[EdgeNodeDescriptor]*edgeNodeProcessor
	reorderTimeout     time.Duration
	next               messageProcessor
	rebirthRequester   rebirthRequester
	mu                 sync.Mutex
}

func newInOrderProcessor(
	reorderTimeout time.Duration,
	next messageProcessor,
	requester rebirthRequester,
) *inOrderProcessor {
	return &inOrderProcessor{
		edgeNodeProcessors: make(map[EdgeNodeDescriptor]*edgeNodeProcessor),
		reorderTimeout:     reorderTimeout,
		next:               next,
		rebirthRequester:   requester,
	}
}

type edgeNodeProcessor struct {
	// The expected sequence number of the next message.
	expectedSeq uint64
	// A map to store messages that are out of order. The key is the sequence number and the value is the message.
	buffer map[uint64]sparkplugMessage
	// A timer to track the reordering timeout.
	reorderTimer *time.Timer
}

func (e *edgeNodeProcessor) reset() {
	e.buffer = make(map[uint64]sparkplugMessage)
	e.expectedSeq = 0
	e.resetTimer()
}

func (e *edgeNodeProcessor) resetTimer() {
	if e.reorderTimer != nil {
		e.reorderTimer.Stop()

		e.reorderTimer = nil
	}
}

func (p *inOrderProcessor) processMessage(msg sparkplugMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// if the msg doesn't have a seq number this processor can't do anything
	// with it so just forward to the next
	if msg.payload.Seq == nil {
		p.next.processMessage(msg)
		return
	}

	edgeNodeDescriptor := msg.topic.edgeNodeDescriptor()
	msgSeqNumber := msg.payload.GetSeq()
	nodeProcessor := p.getEdgeNodeProcessor(edgeNodeDescriptor)

	// If an NBIRTH message arrives we are effectively resetting the host app
	// view of this particular edge node, so we can clear all buffers and stop
	// any running timers
	if msg.topic.messageType == messageTypeNBIRTH {
		nodeProcessor.reset()
	}

	// Adjust the expected sequence number to account for wrapping around at 255.
	if nodeProcessor.expectedSeq > 255 {
		nodeProcessor.expectedSeq %= 256
	}

	// If the message is in the correct order, process it immediately and reset the timer.
	if msgSeqNumber == nodeProcessor.expectedSeq {
		p.next.processMessage(msg)
		nodeProcessor.expectedSeq++

		nodeProcessor.resetTimer()
	} else {
		// The message is out of order. Store it in the buffer.
		nodeProcessor.buffer[msgSeqNumber] = msg

		// if the timer is not running yet (i.e., this is the first time we've seen
		// an out of order msg), start it.
		if nodeProcessor.reorderTimer == nil {
			nodeProcessor.reorderTimer = time.AfterFunc(p.reorderTimeout, func() {
				_ = p.rebirthRequester.requestNodeRebirth(edgeNodeDescriptor)
			})
		}
	}

	// Process any buffered messages in order.
	for seq := nodeProcessor.expectedSeq; ; seq++ {
		msg, ok := nodeProcessor.buffer[seq]
		if !ok {
			// There is a gap in the sequence numbers. Stop processing buffered messages.
			break
		}

		p.next.processMessage(msg)
		delete(nodeProcessor.buffer, seq)

		nodeProcessor.expectedSeq++
	}
}

func (p *inOrderProcessor) getEdgeNodeProcessor(edgeNodeDescriptor EdgeNodeDescriptor) *edgeNodeProcessor {
	nodeProcessor, ok := p.edgeNodeProcessors[edgeNodeDescriptor]
	if !ok {
		nodeProcessor = &edgeNodeProcessor{
			expectedSeq: 0,
			buffer:      make(map[uint64]sparkplugMessage),
		}

		p.edgeNodeProcessors[edgeNodeDescriptor] = nodeProcessor
	}

	return nodeProcessor
}
