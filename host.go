package sparkplughost

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/EvergenEnergy/sparkplughost/protobuf"
	"github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"
)

type messageProcessor interface {
	processMessage(msg sparkplugMessage)
}

type HostApplication struct {
	mqttClient               mqtt.Client
	hostID                   string
	brokerURL                string
	lastWillMessageTimestamp time.Time
	logger                   *slog.Logger
	metricHandler            MetricHandler
	messageProcessor         messageProcessor
	disconnectTimeout        time.Duration
	reorderTimeout           time.Duration
}

func NewHostApplication(brokerURL, hostID string, opts ...Option) *HostApplication {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &HostApplication{
		hostID:            hostID,
		brokerURL:         brokerURL,
		logger:            cfg.logger,
		metricHandler:     cfg.metricHandler,
		disconnectTimeout: cfg.disconnectTimeout,
		reorderTimeout:    cfg.reorderTimeout,
	}
}

// Run will connect to the mqtt broker and block until
// ctx is canceled.
func (h *HostApplication) Run(ctx context.Context) error {
	if h.mqttClient != nil && h.mqttClient.IsConnected() {
		h.mqttClient.Disconnect(uint(h.disconnectTimeout.Milliseconds()))
	}

	h.initClient()

	if token := h.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	// wait until the context is cancelled
	<-ctx.Done()

	// disconnect gracefully by publishing the death certificate before
	// sending the disconnect command to the MQTT broker.
	token := h.publishOnlineStatus(false, time.Now().UTC())
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	h.mqttClient.Disconnect(uint(h.disconnectTimeout.Milliseconds()))
	return nil
}

func (h *HostApplication) initClient() {
	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(h.brokerURL)
	mqttOpts.SetClientID(h.hostID)
	mqttOpts.SetCleanSession(true)
	mqttOpts.SetAutoReconnect(true)
	mqttOpts.SetOrderMatters(true)
	mqttOpts.SetOnConnectHandler(h.onConnect)
	mqttOpts.SetReconnectingHandler(h.onReconnect)
	mqttOpts.SetDefaultPublishHandler(func(_ mqtt.Client, message mqtt.Message) {
		h.logger.Info("received unexpected message", "topic", message.Topic())
	})

	h.setLastWill(mqttOpts)

	h.mqttClient = mqtt.NewClient(mqttOpts)
	commandPublisher := newCommandPublisher(h.mqttClient)

	h.messageProcessor = newEdgeNodeManager(h.metricHandler, commandPublisher, h.logger)
	if h.reorderTimeout > 0 {
		h.messageProcessor = newInOrderProcessor(h.reorderTimeout, h.messageProcessor, commandPublisher)
	}
}

func (h *HostApplication) stateTopic() string {
	return fmt.Sprintf("%s/%s/%s", sparkplugbNamespace, messageTypeSTATE, h.hostID)
}

func (h *HostApplication) onConnect(client mqtt.Client) {
	hostStateTopic := h.stateTopic()

	if token := client.Subscribe(hostStateTopic, byte(1), h.stateHandler); token.Wait() && token.Error() != nil {
		h.logger.Error(
			"error subscribing to STATE topic",
			"topic", hostStateTopic,
			"error", token.Error().Error(),
		)
	}

	msgTypes := []messageType{
		messageTypeNBIRTH,
		messageTypeNDATA,
		messageTypeNDEATH,
		messageTypeDBIRTH,
		messageTypeDDEATH,
		messageTypeDDATA,
	}

	for _, msgType := range msgTypes {
		var topic string

		if msgType.isEdgeNodeMessage() {
			topic = fmt.Sprintf("%s/+/%s/+", sparkplugbNamespace, msgType)
		} else {
			topic = fmt.Sprintf("%s/+/%s/+/+", sparkplugbNamespace, msgType)
		}

		token := client.Subscribe(topic, byte(0), h.msgHandler)
		if token.Wait() && token.Error() != nil {
			h.logger.Error(
				"error subscribing to topic",
				"topic", topic,
				"error", token.Error().Error(),
			)
		}
	}

	// The Sparkplug Host Application MUST publish a
	// Sparkplug Host Application BIRTH message to the MQTT Server immediately after
	// successfully subscribing its own spBv1.0/STATE/sparkplug_host_id topic.
	h.publishOnlineStatus(true, h.lastWillMessageTimestamp)
}

func (h *HostApplication) onReconnect(_ mqtt.Client, options *mqtt.ClientOptions) {
	h.setLastWill(options)
}

func (h *HostApplication) stateHandler(_ mqtt.Client, message mqtt.Message) {
	// the STATE topic uses QOS 1, so we need to make sure we ACK the message
	defer message.Ack()

	var status statusPayload
	if err := json.Unmarshal(message.Payload(), &status); err != nil {
		h.logger.Error("failed to unmarshal STATE payload", "error", err.Error())
		return
	}

	// if at any point the Host Application is delivered a STATE message on
	// its own Host Application ID with an online value of false, it MUST immediately republish its STATE
	// message to the same MQTT Server with an online value of true and the timestamp set to the same
	// value that was used for the timestamp in its own prior MQTT CONNECT packet Will Message
	// payload
	if !status.Online {
		// this runs on a separate goroutine due to paho's mqtt recommendation
		// that "callback functions must not block or call functions within this
		// package that may block (e.g. Publish)"
		go h.publishOnlineStatus(true, h.lastWillMessageTimestamp)
	}
}

func (h *HostApplication) setLastWill(mqttOpts *mqtt.ClientOptions) {
	h.lastWillMessageTimestamp = time.Now().UTC()

	payload, _ := json.Marshal(statusPayload{
		Online:    false,
		Timestamp: h.lastWillMessageTimestamp.UnixMilli(),
	})

	mqttOpts.SetWill(h.stateTopic(), string(payload), byte(1), true)
}

func (h *HostApplication) publishOnlineStatus(online bool, timestamp time.Time) mqtt.Token {
	payload, _ := json.Marshal(statusPayload{
		Online:    online,
		Timestamp: timestamp.UnixMilli(),
	})

	return h.mqttClient.Publish(h.stateTopic(), byte(1), true, payload)
}

func (h *HostApplication) msgHandler(_ mqtt.Client, message mqtt.Message) {
	defer message.Ack()

	sparkplugMsg, err := parseSparkplugMessage(message)
	if err != nil {
		h.logger.Error(
			"Failed to unmarshal protobuf payload",
			"error", err.Error(),
			"topic", message.Topic(),
		)
		return
	}

	h.messageProcessor.processMessage(sparkplugMsg)
}

type sparkplugMessage struct {
	topic   topic
	payload *protobuf.Payload
}

func parseSparkplugMessage(msg mqtt.Message) (sparkplugMessage, error) {
	msgTopic, err := parseTopic(msg.Topic())
	if err != nil {
		return sparkplugMessage{}, err
	}

	var payload protobuf.Payload

	err = proto.Unmarshal(msg.Payload(), &payload)
	if err != nil {
		return sparkplugMessage{}, err
	}

	return sparkplugMessage{topic: msgTopic, payload: &payload}, nil
}

type statusPayload struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
}
