package sparkplughost

import (
	"crypto/tls"
	"io"
	"log/slog"
	"time"
)

// MetricHandler is a callback type which can be set to be
// executed upon the change of any of the known Edge Node
// or Device metrics.
// This includes when a metric is first received during BIRTH
// messages as well as updates through DATA or DEATH messages.
type MetricHandler func(HostMetric)

func defaultMetricHandler(_ HostMetric) {}

type config struct {
	logger            *slog.Logger
	metricHandler     MetricHandler
	disconnectTimeout time.Duration
	reorderTimeout    time.Duration
	mqttKeepAlive     time.Duration
	mqttWriteTimeout  time.Duration
}

// Option allows clients to configure the Host Application.
type Option func(*config)

func defaultConfig() *config {
	return &config{
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		metricHandler:     defaultMetricHandler,
		disconnectTimeout: 5 * time.Second,
		reorderTimeout:    5 * time.Second,
		mqttKeepAlive:     30 * time.Second,
		// A non-zero write timeout is required for AutoReconnect to work against
		// a half-open connection: without it the keepalive PING write blocks
		// forever on a dead socket, the client never observes ConnectionLost, and
		// AutoReconnect never fires. See WithMqttWriteTimeout for details.
		mqttWriteTimeout: 10 * time.Second,
	}
}

// WithMetricHandler sets a MetricHandler to be called when metrics are
// created or updated by this Host Application.
func WithMetricHandler(metricHandler MetricHandler) Option {
	return func(c *config) {
		c.metricHandler = metricHandler
	}
}

// WithLogger sets a `*slog.Logger` instance to use by the Host application.
// This allows clients to enable/disable DEBUG and INFO messages.
// The default logger sends everything to `io.Discard`.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

// WithReorderTimeout sets a timeout on how long to wait before requesting a Rebirth
// when receiving messages out of order.
// Default: 5 seconds.
func WithReorderTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.reorderTimeout = timeout
	}
}

// WithMqttKeepAlive sets the MQTT keepalive interval on the underlying paho
// client. The client sends a PINGREQ after this much idle time; combined with a
// write timeout this is how a broken (e.g. half-open) connection is detected so
// that AutoReconnect can kick in.
// Default: 30 seconds.
func WithMqttKeepAlive(keepAlive time.Duration) Option {
	return func(c *config) {
		c.mqttKeepAlive = keepAlive
	}
}

// WithMqttWriteTimeout sets the MQTT write timeout on the underlying paho client.
//
// This matters for reconnection robustness: paho's AutoReconnect only triggers
// after the client observes that the connection is lost. On a half-open TCP
// connection (for example when the broker is recreated behind a proxy/sidecar
// and no RST reaches the client) the keepalive PING is sent via a socket write.
// With the paho default of no write timeout that write can block indefinitely on
// the dead socket, so ConnectionLost is never reported and AutoReconnect never
// fires — the client sits silently disconnected. A non-zero write timeout makes
// the stalled PING write fail, which surfaces the lost connection and lets
// AutoReconnect recover.
// Default: 10 seconds. Set to 0 to restore paho's blocking behaviour.
func WithMqttWriteTimeout(writeTimeout time.Duration) Option {
	return func(c *config) {
		c.mqttWriteTimeout = writeTimeout
	}
}

// MqttBrokerConfig contains the configuration
// parameters for each of the MQTT Brokers to be
// used.
type MqttBrokerConfig struct {
	// URL of the broker. The format should be scheme://host:port
	// (e.g., tcp://localhost:1883). Required.
	BrokerURL string
	// Username if required by the broker. Optional.
	Username string
	// Password if required by the broker. Optional.
	Password string
	// SSL/TLS configuration to be used when connecting to an MQTT broker.
	// This can be used for brokers where the authentication needs to happen
	// via client certificates instead of username + password. Optional.
	TLSConfig *tls.Config
}
