package sparkplughost_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/EvergenEnergy/sparkplughost"
	"github.com/EvergenEnergy/sparkplughost/protobuf"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

func TestReturnsErrIfNoBrokersConfigured(t *testing.T) {
	_, err := sparkplughost.NewHostApplication(nil, "hostID")
	if err == nil {
		t.Error("expected error but got none")
	}
}

func TestReturnsErrIfNoURLSpecifiedForBroker(t *testing.T) {
	brokerCfg := sparkplughost.MqttBrokerConfig{BrokerURL: ""}

	_, err := sparkplughost.NewHostApplication([]sparkplughost.MqttBrokerConfig{brokerCfg}, "hostID")
	if err == nil {
		t.Error("expected error but got none")
	}
}

func TestHostConnectsAndSendsBirthCertificate(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	// keep the host running until we've seen its birth certificate:
	// shutting it down earlier replaces the retained STATE message with
	// online=false and the test would never observe online=true.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hostID := fmt.Sprintf("test-host-%d", rand.Int())
	host, _ := sparkplughost.NewHostApplication([]sparkplughost.MqttBrokerConfig{testBrokerConfig()}, hostID)

	runErr := make(chan error, 1)

	go func() {
		runErr <- host.Run(ctx)
	}()

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	waitForHostStatus(t, mqttClient, hostID, true)

	cancel()

	if err := <-runErr; err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHostPublishesDeathCertificateWhenStoppingGracefully(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hostID := fmt.Sprintf("test-host-%d", rand.Int())
	host, _ := sparkplughost.NewHostApplication([]sparkplughost.MqttBrokerConfig{testBrokerConfig()}, hostID)

	runErr := make(chan error, 1)

	go func() {
		runErr <- host.Run(ctx)
	}()

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	// wait until the host is online, then stop it gracefully and expect
	// the death certificate to replace the retained birth certificate.
	waitForHostStatus(t, mqttClient, hostID, true)

	cancel()

	if err := <-runErr; err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	waitForHostStatus(t, mqttClient, hostID, false)
}

func TestHandlesMetricsOnNodeBirth(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)

	if len(receivedMetrics) != 2 {
		t.Errorf("received %d metrics on node birth but expected 2", len(receivedMetrics))
	}

	fooMetric := receivedMetrics["foo"][0]

	if got := fooMetric.EdgeNodeDescriptor.GroupID; got != "test-group" {
		t.Errorf("got metric with Group %s but expected test-group", got)
	}
	if got := fooMetric.EdgeNodeDescriptor.EdgeNodeID; got != "test-node" {
		t.Errorf("got metric with Edge Node ID %s but expected test-node", got)
	}
}

func TestHandlesMetricsOnNodeDeath(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		// send an initial BIRTH message, quickly followed by a DEATH one
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		publishNodeDeath(client)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)
	fooMetric := receivedMetrics["foo"]

	// we should have 2 callbacks for metric "foo": one after birth with quality: good
	// and one after the death with quality: stale
	if len(fooMetric) != 2 {
		t.Errorf("received %d callbacks for metric but expected 2", len(fooMetric))
	}

	if got := fooMetric[0].Quality; got != sparkplughost.MetricQualityGood {
		t.Errorf("got metric with Quality %s but expected GOOD", got)
	}
	if got := fooMetric[1].Quality; got != sparkplughost.MetricQualityStale {
		t.Errorf("got metric with Quality %s but expected STALE", got)
	}
}

func TestHandlesMetricsOnNodeData(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		publishNodeData(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 1)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)
	fooMetric := receivedMetrics["foo"]

	// we should have 2 callbacks for metric "foo": one after birth with quality: good
	// and one after the death with quality: stale
	if len(fooMetric) != 2 {
		t.Errorf("received %d callbacks for metric but expected 2", len(fooMetric))
	}

	if got := fooMetric[0].Metric.GetLongValue(); got != 1 {
		t.Errorf("got metric with value %d but expected 1", got)
	}
	if got := fooMetric[1].Metric.GetLongValue(); got != 99 {
		t.Errorf("got metric with value %d but expected 99", got)
	}
}

func TestHandlesMetricsWithAliasOnNodeData(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		publishNodeData(client, []*protobuf.Payload_Metric{
			{
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 1)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)
	fooMetric := receivedMetrics["foo"]

	// we should have 2 callbacks for metric "foo": one after birth with quality: good
	// and one after the death with quality: stale
	if len(fooMetric) != 2 {
		t.Errorf("received %d callbacks for metric but expected 2", len(fooMetric))
	}

	if got := fooMetric[0].Metric.GetLongValue(); got != 1 {
		t.Errorf("got metric with value %d but expected 1", got)
	}
	if got := fooMetric[1].Metric.GetLongValue(); got != 99 {
		t.Errorf("got metric with value %d but expected 99", got)
	}
}

func TestRequestsRebirthWhenReceivingDataWithoutPreviousBirth(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeData(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 0)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthWhenReceivingUnknownAlias(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		publishNodeData(client, []*protobuf.Payload_Metric{
			{
				Alias:    proto.Uint64(99),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 1)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthWhenReceivingUnknownMetric(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		publishNodeData(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("bar"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 1)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthWhenReceivingDuplicatedAlias(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
			{
				Name:     proto.String("bar"),
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestHandlesMetricsOnDeviceBirth(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)

	deviceMetric := receivedMetrics["device-metric"]

	if len(deviceMetric) != 1 {
		t.Errorf("received %d metrics on device birth but expected 1", len(receivedMetrics))
	}

	if got := deviceMetric[0].EdgeNodeDescriptor.GroupID; got != "test-group" {
		t.Errorf("got device metric with Group %s but expected test-group", got)
	}
	if got := deviceMetric[0].EdgeNodeDescriptor.EdgeNodeID; got != "test-node" {
		t.Errorf("got metric with Edge Node ID %s but expected test-node", got)
	}
	if got := deviceMetric[0].DeviceID; got != "test-device" {
		t.Errorf("got metric with Device ID %s but expected test-device", got)
	}
}

func TestHandlesMetricsOnDeviceDeath(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)
		publishDeviceDeath(client)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)

	deviceMetric := receivedMetrics["device-metric"]
	if len(deviceMetric) != 2 {
		t.Errorf("received %d metrics on device death but expected 2", len(receivedMetrics))
	}

	if got := deviceMetric[0].Quality; got != sparkplughost.MetricQualityGood {
		t.Errorf("got metric with Quality %s but expected GOOD", got)
	}
	if got := deviceMetric[1].Quality; got != sparkplughost.MetricQualityStale {
		t.Errorf("got metric with Quality %s but expected STALE", got)
	}
}

func TestDeviceMetricsAreSetToStaleWhenEdgeNodeGoesOffline(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)

		publishNodeDeath(client)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)

	deviceMetric := receivedMetrics["device-metric"]
	if len(deviceMetric) != 2 {
		t.Errorf("received %d metrics on device death but expected 2", len(receivedMetrics))
	}

	if got := deviceMetric[0].Quality; got != sparkplughost.MetricQualityGood {
		t.Errorf("got metric with Quality %s but expected GOOD", got)
	}
	if got := deviceMetric[1].Quality; got != sparkplughost.MetricQualityStale {
		t.Errorf("got metric with Quality %s but expected STALE", got)
	}
}

func TestHandlesMetricsOnDeviceData(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)
		publishDeviceData(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 2)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)

	deviceMetric := receivedMetrics["device-metric"]

	if len(deviceMetric) != 2 {
		t.Errorf("received %d metrics on device birth but expected 2", len(receivedMetrics))
	}

	if got := deviceMetric[0].Metric.GetLongValue(); got != 1 {
		t.Errorf("got metric with value %d but expected 1", got)
	}
	if got := deviceMetric[1].Metric.GetLongValue(); got != 99 {
		t.Errorf("got metric with value %d but expected 99", got)
	}
}

func TestHandlesMetricsWithAliasOnDeviceData(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)
		publishDeviceData(client, []*protobuf.Payload_Metric{
			{
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 2)
	}

	receivedMetrics := runAndCollectAllMetrics(t, testFn)

	deviceMetric := receivedMetrics["device-metric"]

	if len(deviceMetric) != 2 {
		t.Errorf("received %d metrics on device birth but expected 1", len(receivedMetrics))
	}

	if got := deviceMetric[0].Metric.GetLongValue(); got != 1 {
		t.Errorf("got metric with value %d but expected 1", got)
	}
	if got := deviceMetric[1].Metric.GetLongValue(); got != 99 {
		t.Errorf("got metric with value %d but expected 99", got)
	}
}

func TestRequestsRebirthWhenReceivingDeviceBirthWithoutPreviousNodeBirth(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 0)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthWhenReceivingDeviceDataWithoutPreviousDeviceBirth(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceData(client, []*protobuf.Payload_Metric{
			{
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 1)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthOnDuplicatedDeviceMetricAlias(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
			{
				Name:     proto.String("device-metric-2"),
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthOnDeviceUnknownAlias(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)
		publishDeviceData(client, []*protobuf.Payload_Metric{
			{
				Alias:    proto.Uint64(11),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 2)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

func TestRequestsRebirthOnReorderTimeoutExpiration(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	testFn := func(client mqtt.Client) {
		waitForRebirth := expectRebirthRequest(t, client)

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
		publishDeviceBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		}, 1)
		publishDeviceData(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("device-metric"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
			},
		}, 3)

		waitForRebirth()
	}

	runAndCollectAllMetrics(t, testFn)
}

// sentinelEdgeNodeID identifies a throwaway edge node whose birth certificate
// is published after a test's own messages to detect when they have all been
// processed by the host. Its metrics are excluded from the collected results.
const sentinelEdgeNodeID = "shutdown-sentinel"

func publishSentinelNodeBirth(mqttClient mqtt.Client) {
	birthPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics: []*protobuf.Payload_Metric{
			{
				Name:     proto.String("bdSeq"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 0},
			},
		},
		Seq: proto.Uint64(0),
	}
	protoPayload, _ := proto.Marshal(birthPayload)

	token := mqttClient.Publish("spBv1.0/test-group/NBIRTH/"+sentinelEdgeNodeID, byte(0), false, protoPayload)
	token.Wait()
}

func publishNodeBirth(mqttClient mqtt.Client, metrics []*protobuf.Payload_Metric) {
	birthMetrics := []*protobuf.Payload_Metric{
		{
			Name:     proto.String("bdSeq"),
			Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
			Value:    &protobuf.Payload_Metric_LongValue{LongValue: 0},
		},
	}
	birthMetrics = append(birthMetrics, metrics...)

	birthPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics:   birthMetrics,
		Seq:       proto.Uint64(0),
	}
	protoPayload, _ := proto.Marshal(birthPayload)

	token := mqttClient.Publish("spBv1.0/test-group/NBIRTH/test-node", byte(0), false, protoPayload)
	token.Wait()
}

func publishNodeDeath(mqttClient mqtt.Client) {
	deadPayload := &protobuf.Payload{
		Metrics: []*protobuf.Payload_Metric{
			{
				Name:     proto.String("bdSeq"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 0},
			},
		},
	}
	protoPayload, _ := proto.Marshal(deadPayload)
	mqttClient.Publish("spBv1.0/test-group/NDEATH/test-node", byte(0), false, protoPayload)
}

func publishNodeData(mqttClient mqtt.Client, metrics []*protobuf.Payload_Metric, seq uint64) {
	dataPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics:   metrics,
		Seq:       proto.Uint64(seq),
	}
	protoPayload, _ := proto.Marshal(dataPayload)

	token := mqttClient.Publish("spBv1.0/test-group/NDATA/test-node", byte(0), false, protoPayload)
	token.Wait()
}

func publishDeviceBirth(mqttClient mqtt.Client, metrics []*protobuf.Payload_Metric, seq uint64) {
	birthPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics:   metrics,
		Seq:       proto.Uint64(seq),
	}
	protoPayload, _ := proto.Marshal(birthPayload)

	token := mqttClient.Publish("spBv1.0/test-group/DBIRTH/test-node/test-device", byte(0), false, protoPayload)
	token.Wait()
}

func publishDeviceDeath(mqttClient mqtt.Client) {
	deadPayload := &protobuf.Payload{
		Seq: proto.Uint64(2),
	}
	protoPayload, _ := proto.Marshal(deadPayload)
	mqttClient.Publish("spBv1.0/test-group/DDEATH/test-node/test-device", byte(0), false, protoPayload)
}

func publishDeviceData(mqttClient mqtt.Client, metrics []*protobuf.Payload_Metric, seq uint64) {
	dataPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics:   metrics,
		Seq:       proto.Uint64(seq),
	}
	protoPayload, _ := proto.Marshal(dataPayload)

	token := mqttClient.Publish("spBv1.0/test-group/DDATA/test-node/test-device", byte(0), false, protoPayload)
	token.Wait()
}

// helper func to run a new Host instance in the background, collecting all metrics that arrive until ctx
// is cancelled.
// The testFn parameter takes a mqtt client so that each test can simulate the flow of expected messages from an
// edge node/device point of view.
func runAndCollectAllMetrics(t *testing.T, testFn func(mqtt.Client)) map[string][]sparkplughost.HostMetric {
	// keep the host running until testFn's messages have been processed:
	// a fixed host lifetime races against connect/subscribe latency since
	// the retained STATE message is replaced with online=false on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	hostID := fmt.Sprintf("test-host-%d", rand.Int())

	var wg sync.WaitGroup

	receivedMetrics := make(map[string][]sparkplughost.HostMetric)
	sentinelSeen := make(chan struct{}, 1)
	metricHandler := func(metric sparkplughost.HostMetric) {
		if metric.EdgeNodeDescriptor.EdgeNodeID == sentinelEdgeNodeID {
			select {
			case sentinelSeen <- struct{}{}:
			default:
			}

			return
		}

		receivedMetrics[metric.Metric.GetName()] = append(receivedMetrics[metric.Metric.GetName()], metric)
	}

	host, _ := sparkplughost.NewHostApplication(
		[]sparkplughost.MqttBrokerConfig{testBrokerConfig()},
		hostID,
		sparkplughost.WithMetricHandler(metricHandler),
		sparkplughost.WithLogger(logger),
		sparkplughost.WithReorderTimeout(100*time.Millisecond),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := host.Run(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	waitForHostStatus(t, mqttClient, hostID, true)
	testFn(mqttClient)

	// publish a birth certificate for a sentinel edge node and wait for its
	// metric callback: the broker preserves publish order from a single client,
	// so once it arrives every message testFn published has been processed.
	publishSentinelNodeBirth(mqttClient)

	select {
	case <-sentinelSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("timed-out waiting for the sentinel birth certificate to be processed")
	}

	cancel()

	// make sure to wait for all host callbacks to finish
	wg.Wait()

	return receivedMetrics
}

func checkIntegrationTestEnvVar(t *testing.T) {
	t.Helper()

	if os.Getenv("MQTT_BROKER_URL") == "" {
		t.Skip("skipping integration tests: set MQTT_BROKER_URL environment variable (e.g., tcp://localhost:1883).")
	}
}

func testMqttClient(t *testing.T) mqtt.Client {
	t.Helper()

	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(testBrokerConfig().BrokerURL)
	mqttOpts.SetClientID(fmt.Sprintf("test-client-%d", rand.Int()))
	mqttClient := mqtt.NewClient(mqttOpts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	return mqttClient
}

func testBrokerConfig() sparkplughost.MqttBrokerConfig {
	brokerURL := os.Getenv("MQTT_BROKER_URL")

	return sparkplughost.MqttBrokerConfig{BrokerURL: brokerURL}
}

func waitForHostStatus(t *testing.T, mqttClient mqtt.Client, hostID string, online bool) {
	statusChan := make(chan struct{}, 1)

	token := mqttClient.Subscribe("spBv1.0/STATE/"+hostID, byte(1), func(client mqtt.Client, msg mqtt.Message) {
		defer msg.Ack()

		var payload map[string]interface{}

		err := json.Unmarshal(msg.Payload(), &payload)
		if err != nil {
			t.Error(err)
		}

		if payload["online"].(bool) == online {
			statusChan <- struct{}{}
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	select {
	case <-statusChan:
		return
	case <-time.NewTicker(5 * time.Second).C:
		t.Fatalf("timed-out waiting for host status online=%v", online)
	}
}

// expectRebirthRequest subscribes to the edge node's command topic and returns
// a function which blocks until a rebirth request arrives. Subscribing must
// happen before publishing the messages which trigger the rebirth: the command
// is not retained, so a late subscriber would miss it.
func expectRebirthRequest(t *testing.T, mqttClient mqtt.Client) func() {
	t.Helper()

	rebirthChan := make(chan struct{}, 1)

	token := mqttClient.Subscribe("spBv1.0/test-group/NCMD/test-node", byte(0), func(_ mqtt.Client, message mqtt.Message) {
		var payload protobuf.Payload
		_ = proto.Unmarshal(message.Payload(), &payload)

		for _, metric := range payload.Metrics {
			if metric.GetName() == "Node Control/Rebirth" && metric.GetBooleanValue() {
				select {
				case rebirthChan <- struct{}{}:
				default:
				}

				return
			}
		}
	})
	if token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	return func() {
		select {
		case <-rebirthChan:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed-out waiting for rebirth request")
		}
	}
}
