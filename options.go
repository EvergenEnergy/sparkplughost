package sparkplughost

import (
	"crypto/tls"
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
	mqttPingTimeout   time.Duration
	mqttWriteTimeout  time.Duration
}

// Option allows clients to configure the Host Application.
type Option func(*config)

func defaultConfig() *config {
	return &config{
		logger:            slog.New(slog.DiscardHandler),
		metricHandler:     defaultMetricHandler,
		disconnectTimeout: 5 * time.Second,
		reorderTimeout:    5 * time.Second,
		mqttKeepAlive:     30 * time.Second,
		mqttPingTimeout:   10 * time.Second,
		// A non-zero write timeout keeps a blocked outbound write (publish/
		// subscribe/ack into a dead socket whose send buffer has filled) from
		// stalling forever, so connection-lost handling can run and AutoReconnect
		// can recover. See WithMqttWriteTimeout. All three default to paho's own
		// defaults except that WriteTimeout, which paho leaves at 0 (block
		// forever), is set to a finite value here.
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
// The default logger discards all log output.
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
// client: the client sends a PINGREQ after this much idle time, and a missing
// PINGRESP (see WithMqttPingTimeout) is how an idle half-open connection is
// detected so AutoReconnect can kick in.
// Default: 30 seconds.
//
// Note: paho stores the keepalive as whole seconds, so a sub-second value
// truncates to 0. A value of 0 disables keepalive entirely — no PINGs are sent
// and an idle half-open connection can no longer be detected — so avoid 0 unless
// that is genuinely intended.
func WithMqttKeepAlive(keepAlive time.Duration) Option {
	return func(c *config) {
		c.mqttKeepAlive = keepAlive
	}
}

// WithMqttPingTimeout sets how long the client waits for a PINGRESP after
// sending a keepalive PINGREQ before treating the connection as lost. This is
// the knob that governs idle half-open detection latency: with an idle
// connection the small PINGREQ is written without blocking and the missing
// PINGRESP triggers reconnection after this timeout.
// Default: 10 seconds (paho's default).
func WithMqttPingTimeout(pingTimeout time.Duration) Option {
	return func(c *config) {
		c.mqttPingTimeout = pingTimeout
	}
}

// WithMqttWriteTimeout sets the MQTT write timeout on the underlying paho client.
//
// This matters for reconnection robustness. paho applies this timeout to
// outbound application writes (publishes, subscriptions, acknowledgements) in its
// outgoing worker. On a half-open connection whose kernel send buffer has filled
// — e.g. the host keeps publishing STATE/commands into a dead socket — such a
// write blocks indefinitely with paho's default of no write timeout. That stalls
// the outgoing worker (and can stall the keepalive goroutine on its own PING
// write, preventing the ping-timeout check from ever running), so ConnectionLost
// is never reported and AutoReconnect never fires — the client sits silently
// disconnected. A non-zero write timeout makes the blocked write fail, which
// surfaces the lost connection and lets AutoReconnect recover.
//
// Note: an idle connection is instead covered by keepalive + ping timeout above;
// this timeout is specifically for the blocked-outbound-write case.
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
