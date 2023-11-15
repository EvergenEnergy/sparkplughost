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
}

// Option allows clients to configure the Host Application.
type Option func(*config)

func defaultConfig() *config {
	return &config{
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		metricHandler:     defaultMetricHandler,
		disconnectTimeout: 5 * time.Second,
		reorderTimeout:    5 * time.Second,
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
