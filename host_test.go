package sparkplughost_test

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestHostConnectsAndSendsBirthCertificate(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})
	}

	receivedMetrics := runAndCollectAllMetrics(ctx, t, testFn)

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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	testFn := func(client mqtt.Client) {
		// send an initial BIRTH message, quickly followed by a DEATH one
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

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
		client.Publish("spBv1.0/test-group/NDEATH/test-node", byte(0), false, protoPayload)
	}

	receivedMetrics := runAndCollectAllMetrics(ctx, t, testFn)
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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		nDataPayload := &protobuf.Payload{
			Metrics: []*protobuf.Payload_Metric{
				{
					Name:     proto.String("foo"),
					Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
					Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
				},
			},
		}
		protoPayload, _ := proto.Marshal(nDataPayload)
		client.Publish("spBv1.0/test-group/NDATA/test-node", byte(0), false, protoPayload)
	}

	receivedMetrics := runAndCollectAllMetrics(ctx, t, testFn)
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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	testFn := func(client mqtt.Client) {
		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		nDataPayload := &protobuf.Payload{
			Metrics: []*protobuf.Payload_Metric{
				{
					Alias:    proto.Uint64(15),
					Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
					Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
				},
			},
		}
		protoPayload, _ := proto.Marshal(nDataPayload)
		client.Publish("spBv1.0/test-group/NDATA/test-node", byte(0), false, protoPayload)
	}

	receivedMetrics := runAndCollectAllMetrics(ctx, t, testFn)
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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	receivedRebirthRequest := false

	testFn := func(client mqtt.Client) {
		client.Subscribe("spBv1.0/test-group/NCMD/test-node", byte(0), func(_ mqtt.Client, message mqtt.Message) {
			var payload protobuf.Payload
			_ = proto.Unmarshal(message.Payload(), &payload)

			for _, metric := range payload.Metrics {
				if metric.GetName() == "Node Control/Rebirth" {
					receivedRebirthRequest = metric.GetBooleanValue()
					return
				}
			}
		})

		nDataPayload := &protobuf.Payload{
			Metrics: []*protobuf.Payload_Metric{
				{
					Name:     proto.String("foo"),
					Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
					Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
				},
			},
		}
		protoPayload, _ := proto.Marshal(nDataPayload)
		client.Publish("spBv1.0/test-group/NDATA/test-node", byte(0), false, protoPayload)
	}

	runAndCollectAllMetrics(ctx, t, testFn)

	if !receivedRebirthRequest {
		t.Error("expected a rebirth request to have arrived but got none")
	}
}

func TestRequestsRebirthWhenReceivingUnknownAlias(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	receivedRebirthRequest := false

	testFn := func(client mqtt.Client) {
		client.Subscribe("spBv1.0/test-group/NCMD/test-node", byte(0), func(_ mqtt.Client, message mqtt.Message) {
			var payload protobuf.Payload
			_ = proto.Unmarshal(message.Payload(), &payload)

			for _, metric := range payload.Metrics {
				if metric.GetName() == "Node Control/Rebirth" {
					receivedRebirthRequest = metric.GetBooleanValue()
					return
				}
			}
		})

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Alias:    proto.Uint64(15),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		nDataPayload := &protobuf.Payload{
			Metrics: []*protobuf.Payload_Metric{
				{
					Alias:    proto.Uint64(99),
					Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
					Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
				},
			},
		}
		protoPayload, _ := proto.Marshal(nDataPayload)
		client.Publish("spBv1.0/test-group/NDATA/test-node", byte(0), false, protoPayload)
	}

	runAndCollectAllMetrics(ctx, t, testFn)

	if !receivedRebirthRequest {
		t.Error("expected a rebirth request to have arrived but got none")
	}
}

func TestRequestsRebirthWhenReceivingUnknownMetric(t *testing.T) {
	checkIntegrationTestEnvVar(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	receivedRebirthRequest := false

	testFn := func(client mqtt.Client) {
		client.Subscribe("spBv1.0/test-group/NCMD/test-node", byte(0), func(_ mqtt.Client, message mqtt.Message) {
			var payload protobuf.Payload
			_ = proto.Unmarshal(message.Payload(), &payload)

			for _, metric := range payload.Metrics {
				if metric.GetName() == "Node Control/Rebirth" {
					receivedRebirthRequest = metric.GetBooleanValue()
					return
				}
			}
		})

		publishNodeBirth(client, []*protobuf.Payload_Metric{
			{
				Name:     proto.String("foo"),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
				Value:    &protobuf.Payload_Metric_LongValue{LongValue: 1},
			},
		})

		nDataPayload := &protobuf.Payload{
			Metrics: []*protobuf.Payload_Metric{
				{
					Name:     proto.String("bar"),
					Datatype: proto.Uint32(uint32(protobuf.DataType_Int64.Number())),
					Value:    &protobuf.Payload_Metric_LongValue{LongValue: 99},
				},
			},
		}
		protoPayload, _ := proto.Marshal(nDataPayload)
		client.Publish("spBv1.0/test-group/NDATA/test-node", byte(0), false, protoPayload)
	}

	runAndCollectAllMetrics(ctx, t, testFn)

	if !receivedRebirthRequest {
		t.Error("expected a rebirth request to have arrived but got none")
	}
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

// helper func to run a new Host instance in the background, collecting all metrics that arrive until ctx
// is cancelled.
// The testFn parameter takes a mqtt client so that each test can simulate the flow of expected messages from an
// edge node/device point of view.
func runAndCollectAllMetrics(ctx context.Context, t *testing.T, testFn func(mqtt.Client)) map[string][]sparkplughost.HostMetric {
	mqttClient := testMqttClient(t)
	defer mqttClient.Disconnect(250)

	hostID := fmt.Sprintf("test-host-%d", rand.Int())

	var wg sync.WaitGroup

	receivedMetrics := make(map[string][]sparkplughost.HostMetric)
	metricHandler := func(metric sparkplughost.HostMetric) {
		receivedMetrics[metric.Metric.GetName()] = append(receivedMetrics[metric.Metric.GetName()], metric)
	}

	host := sparkplughost.NewHostApplication(testBrokerURL(), hostID, sparkplughost.WithMetricHandler(metricHandler))

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
