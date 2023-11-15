package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/EvergenEnergy/sparkplughost"
	"github.com/EvergenEnergy/sparkplughost/protobuf"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var brokerURL, certFile, keyFile string

	flag.StringVar(&brokerURL, "broker-url", "", "URL of the mqtt broker. E.g., ssl://localhost:1883")
	flag.StringVar(&certFile, "cert", "", "Path to the client certificate file")
	flag.StringVar(&keyFile, "key", "", "Path to the private key file")
	flag.Parse()

	if brokerURL == "" {
		panic("Broker URL is required")
	}

	if certFile == "" || keyFile == "" {
		panic("Both client certificate and private key file paths are required")
	}

	brokerConfig, err := buildBrokerConfig(brokerURL, certFile, keyFile)

	host, err := sparkplughost.NewHostApplication(
		[]sparkplughost.MqttBrokerConfig{brokerConfig},
		"my-host-id",
		sparkplughost.WithReorderTimeout(5*time.Second),
		sparkplughost.WithLogger(logger),
		sparkplughost.WithMetricHandler(func(metric sparkplughost.HostMetric) {
			logger.Info(
				"Received metric callback",
				"metric_name", metric.Metric.GetName(),
				"edge_node_descriptor", metric.EdgeNodeDescriptor.String(),
				"metric_type", protobuf.DataType_name[int32(metric.Metric.GetDatatype())],
				"device_id", metric.DeviceID,
				"quality", metric.Quality,
				"value", metric.Metric.GetValue(),
			)
		}),
	)
	if err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Interrupt)
	defer cancel()

	go func() {
		if err := host.Run(ctx); err != nil {
			panic(err)
		}
	}()

	// Start a simple server to receive write requests and forward those using NCMD or DCMD
	// messages.
	mux := http.NewServeMux()
	mux.HandleFunc("/send-edge-node-rebirth-request", handleRebirthRequest(host, logger))
	mux.HandleFunc("/send-device-rebirth-request", handleRebirthRequest(host, logger))
	mux.HandleFunc("/write-edge-node-metric", handleMetricWriteRequest(host, logger))
	mux.HandleFunc("/write-device-metric", handleMetricWriteRequest(host, logger))

	srv := http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}

	go func() {
		_ = srv.ListenAndServe()
	}()

	<-ctx.Done()
	srv.Shutdown(context.Background())
}

func buildBrokerConfig(brokerURL, certFile, keyFile string) (sparkplughost.MqttBrokerConfig, error) {
	cert, err := os.ReadFile(certFile)
	if err != nil {
		return sparkplughost.MqttBrokerConfig{}, err
	}

	key, err := os.ReadFile(keyFile)
	if err != nil {
		return sparkplughost.MqttBrokerConfig{}, err
	}

	certificates, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return sparkplughost.MqttBrokerConfig{}, err
	}

	return sparkplughost.MqttBrokerConfig{
		BrokerURL: brokerURL,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{certificates},
		},
	}, nil
}
