package main

import (
	"log/slog"

	"github.com/EvergenEnergy/sparkplughost"
	"github.com/EvergenEnergy/sparkplughost/protobuf"
)

type loggingFilters struct {
	groupID    string
	edgeNodeID string
	deviceID   string
	metricName string
}

func loggingHandler(logger *slog.Logger, filters loggingFilters) sparkplughost.MetricHandler {
	return func(metric sparkplughost.HostMetric) {
		if filter := filters.groupID; filter != "" && metric.EdgeNodeDescriptor.GroupID != filter {
			return
		}

		if filter := filters.edgeNodeID; filter != "" && metric.EdgeNodeDescriptor.EdgeNodeID != filter {
			return
		}

		if filter := filters.deviceID; filter != "" && metric.DeviceID != filter {
			return
		}

		if filter := filters.metricName; filter != "" && metric.Metric.GetName() != filter {
			return
		}

		logger.Info(
			"Received metric callback",
			"metric_name", metric.Metric.GetName(),
			"edge_node_descriptor", metric.EdgeNodeDescriptor.String(),
			"metric_type", protobuf.DataType_name[int32(metric.Metric.GetDatatype())],
			"device_id", metric.DeviceID,
			"quality", metric.Quality,
			"value", metric.Metric.GetValue(),
		)
	}
}
