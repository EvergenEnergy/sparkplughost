package sparkplughost

import (
	"context"
	"encoding/json"
	"errors"
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
	mqttClients               map[string]mqtt.Client
	brokers                   []MqttBrokerConfig
	lastWillMessageTimestamps map[string]time.Time
	hostID                    string
	logger                    *slog.Logger
	metricHandler             MetricHandler
	messageProcessor          messageProcessor
	disconnectTimeout         time.Duration
	reorderTimeout            time.Duration
	commandPublisher          *commandPublisher
}

func NewHostApplication(brokerConfigs []MqttBrokerConfig, hostID string, opts ...Option) (*HostApplication, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if len(brokerConfigs) == 0 {
		return nil, errors.New("at least 1 MQTT Broker config needs to be provided")
	}

	for _, brokerConfig := range brokerConfigs {
		if len(brokerConfig.BrokerURL) == 0 {
			return nil, errors.New("broker URL is required")
		}
	}

	return &HostApplication{
		hostID:                    hostID,
		brokers:                   brokerConfigs,
		logger:                    cfg.logger,
		metricHandler:             cfg.metricHandler,
		disconnectTimeout:         cfg.disconnectTimeout,
		reorderTimeout:            cfg.reorderTimeout,
		mqttClients:               make(map[string]mqtt.Client, len(brokerConfigs)),
		lastWillMessageTimestamps: make(map[string]time.Time, len(brokerConfigs)),
	}, nil
}

// Run will connect to the mqtt broker and block until
// ctx is canceled.
func (h *HostApplication) Run(ctx context.Context) error {
	h.initClients()

	for brokerURL, client := range h.mqttClients {
		h.logger.Info("Connecting to MQTT Broker", "broker_url", brokerURL)

		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}

	// wait until the context is cancelled
	<-ctx.Done()

	// disconnect gracefully by publishing the death certificate before
	// sending the disconnect command to the MQTT broker.
	tokens := h.publishOnlineStatus(false, time.Now().UTC())

	for _, token := range tokens {
		if token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}

	for _, client := range h.mqttClients {
		client.Disconnect(uint(h.disconnectTimeout.Milliseconds()))
	}
	return nil
}

func (h *HostApplication) SendEdgeNodeRebirthRequest(descriptor EdgeNodeDescriptor) error {
	return h.commandPublisher.requestNodeRebirth(descriptor)
}

func (h *HostApplication) SendDeviceRebirthRequest(descriptor EdgeNodeDescriptor, deviceID string) error {
	return h.commandPublisher.requestDeviceRebirth(descriptor, deviceID)
}

func (h *HostApplication) SendEdgeNodeCommand(descriptor EdgeNodeDescriptor, metrics []*protobuf.Payload_Metric) error {
	return h.commandPublisher.writeEdgeNodeMetrics(descriptor, metrics)
}

func (h *HostApplication) SendDeviceCommand(descriptor EdgeNodeDescriptor, deviceID string, metrics []*protobuf.Payload_Metric) error {
	return h.commandPublisher.writeDeviceMetrics(descriptor, deviceID, metrics)
}

func (h *HostApplication) initClients() {
	for _, brokerConfig := range h.brokers {
		brokerURL := brokerConfig.BrokerURL

		client, found := h.mqttClients[brokerURL]
		if found && client.IsConnected() {
			client.Disconnect(uint(h.disconnectTimeout.Milliseconds()))
		}

		mqttOpts := mqtt.NewClientOptions()
		mqttOpts.AddBroker(brokerURL)
		mqttOpts.SetClientID(h.hostID)
		mqttOpts.SetCleanSession(true)
		mqttOpts.SetAutoReconnect(true)
		mqttOpts.SetOrderMatters(true)
		mqttOpts.SetOnConnectHandler(h.onConnect(brokerURL))
		mqttOpts.SetReconnectingHandler(h.onReconnect(brokerURL))
		mqttOpts.SetDefaultPublishHandler(func(_ mqtt.Client, message mqtt.Message) {})

		if len(brokerConfig.Username) > 0 {
			mqttOpts.SetUsername(brokerConfig.Username)
		}
		if len(brokerConfig.Password) > 0 {
			mqttOpts.SetPassword(brokerConfig.Password)
		}
		if brokerConfig.TLSConfig != nil {
			mqttOpts.SetTLSConfig(brokerConfig.TLSConfig)
		}

		h.setLastWill(brokerURL, mqttOpts)

		h.mqttClients[brokerURL] = mqtt.NewClient(mqttOpts)
	}

	commandPublisher := newCommandPublisher(h.mqttClients, h.logger)

	h.messageProcessor = newEdgeNodeManager(h.metricHandler, commandPublisher, h.logger)
	if h.reorderTimeout > 0 {
		h.messageProcessor = newInOrderProcessor(h.reorderTimeout, h.messageProcessor, commandPublisher)
	}

	h.commandPublisher = commandPublisher
}

func (h *HostApplication) stateTopic() string {
	return fmt.Sprintf("%s/%s/%s", sparkplugbNamespace, messageTypeSTATE, h.hostID)
}

func (h *HostApplication) onConnect(brokerURL string) mqtt.OnConnectHandler {
	return func(client mqtt.Client) {
		h.logger.Info("Connected to MQTT Broker. Starting subscriptions", "broker_url", brokerURL)

		for _, msgType := range []messageType{
			messageTypeNBIRTH,
			messageTypeNDATA,
			messageTypeNDEATH,
			messageTypeDBIRTH,
			messageTypeDDEATH,
			messageTypeDDATA,
		} {
			var topic string

			if msgType.isEdgeNodeMessage() {
				topic = fmt.Sprintf("%s/+/%s/+", sparkplugbNamespace, msgType)
			} else {
				topic = fmt.Sprintf("%s/+/%s/+/+", sparkplugbNamespace, msgType)
			}

			client.AddRoute(topic, h.msgHandler)
		}

		if token := client.Subscribe(
			fmt.Sprintf("%s/#", sparkplugbNamespace),
			byte(0),
			nil,
		); token.Wait() && token.Error() != nil {
			h.logger.Error(
				"error subscribing to spBv1.0/# topic",
				"error", token.Error().Error(),
				"broker_url", brokerURL,
			)
		}

		hostStateTopic := h.stateTopic()

		// even though the wildcard subscription to spBv1.0/# should already encompass the host STATE topic,
		// the TCK spec requires an explicit subscription here.
		if token := client.Subscribe(hostStateTopic, byte(1), h.stateHandler(brokerURL)); token.Wait() && token.Error() != nil {
			h.logger.Error(
				"error subscribing to STATE topic",
				"topic", hostStateTopic,
				"error", token.Error().Error(),
				"broker_url", brokerURL,
			)
		}

		// The Sparkplug Host Application MUST publish a
		// Sparkplug Host Application BIRTH message to the MQTT Server immediately after
		// successfully subscribing its own spBv1.0/STATE/sparkplug_host_id topic.
		h.publishOnlineStatus(true, h.lastWillMessageTimestamps[brokerURL])
	}
}

func (h *HostApplication) onReconnect(brokerURL string) mqtt.ReconnectHandler {
	return func(client mqtt.Client, options *mqtt.ClientOptions) {
		h.setLastWill(brokerURL, options)
	}
}

func (h *HostApplication) stateHandler(brokerURL string) mqtt.MessageHandler {
	return func(client mqtt.Client, message mqtt.Message) {
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
			go h.publishOnlineStatus(true, h.lastWillMessageTimestamps[brokerURL])
		}
	}
}

func (h *HostApplication) setLastWill(brokerURL string, mqttOpts *mqtt.ClientOptions) {
	lastWillMessageTimestamp := time.Now().UTC()
	h.lastWillMessageTimestamps[brokerURL] = lastWillMessageTimestamp

	payload, _ := json.Marshal(statusPayload{
		Online:    false,
		Timestamp: lastWillMessageTimestamp.UnixMilli(),
	})

	mqttOpts.SetWill(h.stateTopic(), string(payload), byte(1), true)
}

func (h *HostApplication) publishOnlineStatus(online bool, timestamp time.Time) []mqtt.Token {
	payload, _ := json.Marshal(statusPayload{
		Online:    online,
		Timestamp: timestamp.UnixMilli(),
	})

	tokens := make([]mqtt.Token, 0, len(h.mqttClients))

	for _, client := range h.mqttClients {
		token := client.Publish(h.stateTopic(), byte(1), true, payload)
		tokens = append(tokens, token)
	}
	return tokens
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
