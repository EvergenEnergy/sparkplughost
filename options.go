package sparkplughost

import (
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
}

// Option allows clients to configure the Host Application.
type Option func(*config)

func defaultConfig() *config {
	return &config{
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		metricHandler:     defaultMetricHandler,
		disconnectTimeout: 5 * time.Second,
	}
}

// WithMetricHandler sets a MetricHandler to be called when metrics are
// created or updated by this Host Application.
func WithMetricHandler(metricHandler MetricHandler) Option {
	return func(c *config) {
		c.metricHandler = metricHandler
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}
