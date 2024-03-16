package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/EvergenEnergy/sparkplughost"
	"github.com/EvergenEnergy/sparkplughost/protobuf"
	"google.golang.org/protobuf/proto"
)

type metricWriteRequest struct {
	Name     string `json:"name"`
	Value    any    `json:"value"`
	DataType string `json:"data_type"`
}

func (mwr metricWriteRequest) toProtoMetric() (*protobuf.Payload_Metric, error) {
	dataTypeInt, ok := protobuf.DataType_value[mwr.DataType]
	if !ok {
		return nil, fmt.Errorf("invalid data_type %s", mwr.DataType)
	}

	protoMetric := &protobuf.Payload_Metric{
		Name:     proto.String(mwr.Name),
		Datatype: proto.Uint32(uint32(dataTypeInt)),
	}

	switch protobuf.DataType(dataTypeInt) {
	case protobuf.DataType_Int8, protobuf.DataType_Int16, protobuf.DataType_Int32,
		protobuf.DataType_UInt8, protobuf.DataType_UInt16, protobuf.DataType_UInt32:
		protoMetric.Value = &protobuf.Payload_Metric_IntValue{IntValue: uint32(mwr.Value.(float64))}
	case protobuf.DataType_Int64, protobuf.DataType_UInt64:
		protoMetric.Value = &protobuf.Payload_Metric_LongValue{LongValue: uint64(mwr.Value.(float64))}
	case protobuf.DataType_Float:
		protoMetric.Value = &protobuf.Payload_Metric_FloatValue{FloatValue: float32(mwr.Value.(float64))}
	case protobuf.DataType_Double:
		protoMetric.Value = &protobuf.Payload_Metric_DoubleValue{DoubleValue: mwr.Value.(float64)}
	case protobuf.DataType_Boolean:
		protoMetric.Value = &protobuf.Payload_Metric_BooleanValue{BooleanValue: mwr.Value.(bool)}
	case protobuf.DataType_String:
		protoMetric.Value = &protobuf.Payload_Metric_StringValue{StringValue: mwr.Value.(string)}
	}

	return protoMetric, nil
}

func handleMetricWriteRequest(host *sparkplughost.HostApplication, logger *slog.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		queryParams := req.URL.Query()

		groupID := queryParams.Get("group_id")
		edgeNodeID := queryParams.Get("edge_node_id")
		deviceID := queryParams.Get("device_id")

		var writeReq []metricWriteRequest

		err := json.NewDecoder(req.Body).Decode(&writeReq)
		if err != nil {
			logger.Error("failed to unmarshal metrics payload", "error", err.Error())
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		if len(writeReq) == 0 {
			return
		}

		protoMetrics := make([]*protobuf.Payload_Metric, len(writeReq))

		for i, wr := range writeReq {
			protoMetric, err := wr.toProtoMetric()
			if err != nil {
				logger.Error("failed to parse metric", "error", err.Error())
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			protoMetrics[i] = protoMetric
		}

		if len(deviceID) == 0 {
			err = host.SendEdgeNodeCommand(sparkplughost.EdgeNodeDescriptor{
				GroupID:    groupID,
				EdgeNodeID: edgeNodeID,
			}, protoMetrics)
		} else {
			err = host.SendDeviceCommand(sparkplughost.EdgeNodeDescriptor{
				GroupID:    groupID,
				EdgeNodeID: edgeNodeID,
			}, deviceID, protoMetrics)
		}
		if err != nil {
			logger.Error("failed to send device command", "error", err.Error())
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func handleRebirthRequest(host *sparkplughost.HostApplication, logger *slog.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		queryParams := req.URL.Query()

		groupID := queryParams.Get("group_id")
		edgeNodeID := queryParams.Get("edge_node_id")
		deviceID := queryParams.Get("device_id")

		var err error

		if len(deviceID) == 0 {
			err = host.SendEdgeNodeRebirthRequest(sparkplughost.EdgeNodeDescriptor{
				GroupID:    groupID,
				EdgeNodeID: edgeNodeID,
			})
		} else {
			err = host.SendDeviceRebirthRequest(sparkplughost.EdgeNodeDescriptor{
				GroupID:    groupID,
				EdgeNodeID: edgeNodeID,
			}, deviceID)
		}

		if err != nil {
			logger.Error("failed to send rebirth request", "error", err.Error())
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
