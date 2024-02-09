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
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var (
		brokerURL, certFile, keyFile, hostID string
		// optional filters
		groupID, edgeNodeID, deviceID, metricName string
	)

	flag.StringVar(&brokerURL, "broker-url", "", "URL of the mqtt broker. E.g., ssl://localhost:1883 (Required)")
	flag.StringVar(&certFile, "cert", "", "Path to the client certificate file (Required)")
	flag.StringVar(&keyFile, "key", "", "Path to the private key file (Required)")
	flag.StringVar(&hostID, "host-id", "", "This ID will be used as the client ID for the MQTT connection (Required)")

	flag.StringVar(&groupID, "group-id", "", "Only log messages from this group ID (Optional)")
	flag.StringVar(&edgeNodeID, "edge-node-id", "", "Only log messages from this edge node ID (Optional)")
	flag.StringVar(&deviceID, "device-id", "", "Only log messages from this device ID (Optional)")
	flag.StringVar(&metricName, "metric-name", "", "Only log messages for this metric (Optional)")
	flag.Parse()

	if brokerURL == "" {
		panic("Broker URL is required")
	}

	if certFile == "" || keyFile == "" {
		panic("Both client certificate and private key file paths are required")
	}

	if hostID == "" {
		panic("Host ID is required")
	}

	brokerConfig, err := buildBrokerConfig(brokerURL, certFile, keyFile)
	if err != nil {
		panic(err)
	}

	metricHandler := loggingHandler(logger, loggingFilters{
		groupID:    groupID,
		edgeNodeID: edgeNodeID,
		deviceID:   deviceID,
		metricName: metricName,
	})

	host, err := sparkplughost.NewHostApplication(
		[]sparkplughost.MqttBrokerConfig{brokerConfig},
		hostID,
		sparkplughost.WithReorderTimeout(5*time.Second),
		sparkplughost.WithLogger(logger),
		sparkplughost.WithMetricHandler(metricHandler),
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
