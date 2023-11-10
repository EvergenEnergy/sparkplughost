package sparkplughost_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/EvergenEnergy/sparkplughost"
	"github.com/EvergenEnergy/sparkplughost/protobuf"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"
)

func TestHostConnectsAndSendsBirthCertificate(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hostID := fmt.Sprintf("test-host-%d", rand.Int())
	host := sparkplughost.NewHostApplication(testBrokerURL(), hostID)

	go func() {
		err := host.Run(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	waitForHostStatus(t, mqttClient, hostID, true)
}

func TestHostPublishesDeathCertificateWhenStoppingGracefully(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hostID := fmt.Sprintf("test-host-%d", rand.Int())
	host := sparkplughost.NewHostApplication(testBrokerURL(), hostID)

	go func() {
		err := host.Run(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	waitForHostStatus(t, mqttClient, hostID, false)
}

func TestHandlesMetricsOnNodeBirth(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	receivedMetrics := make(map[string]sparkplughost.HostMetric)
	metricHandler := func(metric sparkplughost.HostMetric) {
		receivedMetrics[metric.Metric.GetName()] = metric
	}

	hostID := fmt.Sprintf("test-host-%d", rand.Int())
	host := sparkplughost.NewHostApplication(testBrokerURL(), hostID, sparkplughost.WithMetricHandler(metricHandler))

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	go func() {
		err := host.Run(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	waitForHostStatus(t, mqttClient, hostID, true)

	birthPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics: []*protobuf.Payload_Metric{
			{
				Name:     proto.String("bdSeq"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 0},
			},
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		},
		Seq: proto.Uint64(0),
	}
	protoPayload, _ := proto.Marshal(birthPayload)
	mqttClient.Publish("spBv1.0/test-group/NBIRTH/test-node", byte(0), false, protoPayload)

	// Give the host some time to process the message
	<-ctx.Done()

	if len(receivedMetrics) != 2 {
		t.Errorf("received %d metrics on node birth but expected 2", len(receivedMetrics))
	}

	fooMetric := receivedMetrics["foo"]

	if got := fooMetric.EdgeNodeDescriptor.GroupID; got != "test-group" {
		t.Errorf("got metric with Group %s but expected test-group", got)
	}
	if got := fooMetric.EdgeNodeDescriptor.EdgeNodeID; got != "test-node" {
		t.Errorf("got metric with Edge Node ID %s but expected test-node", got)
	}
}

func TestHandlesMetricsOnNodeDeath(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	receivedMetrics := make(map[string][]sparkplughost.HostMetric)
	metricHandler := func(metric sparkplughost.HostMetric) {
		receivedMetrics[metric.Metric.GetName()] = append(receivedMetrics[metric.Metric.GetName()], metric)
	}

	hostID := fmt.Sprintf("test-host-%d", rand.Int())
	host := sparkplughost.NewHostApplication(testBrokerURL(), hostID, sparkplughost.WithMetricHandler(metricHandler))

	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	go func() {
		err := host.Run(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	waitForHostStatus(t, mqttClient, hostID, true)

	// send an initial BIRTH message, quickly followed by a DEATH one
	birthPayload := &protobuf.Payload{
		Timestamp: proto.Uint64(uint64(time.Now().UnixMilli())),
		Metrics: []*protobuf.Payload_Metric{
			{
				Name:     proto.String("bdSeq"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 0},
			},
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		},
		Seq: proto.Uint64(0),
	}
	protoPayload, _ := proto.Marshal(birthPayload)
	mqttClient.Publish("spBv1.0/test-group/NBIRTH/test-node", byte(0), false, protoPayload)

	deadPayload := &protobuf.Payload{
		Metrics: []*protobuf.Payload_Metric{
			{
				Name:     proto.String("bdSeq"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 0},
			},
		},
	}
	protoPayload, _ = proto.Marshal(deadPayload)
	mqttClient.Publish("spBv1.0/test-group/NDEATH/test-node", byte(0), false, protoPayload)

	// Give the host some time to process the messages
	<-ctx.Done()

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

func checkIntegrationTestEnvVar(t *testing.T) {
	t.Helper()

	if os.Getenv("MQTT_BROKER_URL") == "" {
		t.Skip("skipping integration tests: set MQTT_BROKER_URL environment variable (e.g., tcp://localhost:1883).")
	}
}

func testMqttClient(t *testing.T) mqtt.Client {
	t.Helper()

	mqttOpts := mqtt.NewClientOptions()
	mqttOpts.AddBroker(testBrokerURL())
	mqttOpts.SetClientID(fmt.Sprintf("test-client-%d", rand.Int()))
	mqttClient := mqtt.NewClient(mqttOpts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatal(token.Error())
	}

	return mqttClient
}

func testBrokerURL() string {
	return os.Getenv("MQTT_BROKER_URL")
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
		t.Fatalf("timed-out waiting for death certificate")
	}
}
