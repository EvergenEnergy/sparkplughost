package sparkplughost

import (
	"testing"
	"time"
)

func TestDefaultMqttTimeouts(t *testing.T) {
	cfg := defaultConfig()
	if cfg.mqttWriteTimeout != 10*time.Second {
		t.Errorf("default mqttWriteTimeout = %v, want 10s", cfg.mqttWriteTimeout)
	}
	if cfg.mqttKeepAlive != 30*time.Second {
		t.Errorf("default mqttKeepAlive = %v, want 30s", cfg.mqttKeepAlive)
	}
	if cfg.mqttPingTimeout != 10*time.Second {
		t.Errorf("default mqttPingTimeout = %v, want 10s", cfg.mqttPingTimeout)
	}
}

func TestWithMqttTimeoutOptions(t *testing.T) {
	cfg := defaultConfig()
	WithMqttWriteTimeout(3 * time.Second)(cfg)
	WithMqttKeepAlive(15 * time.Second)(cfg)
	WithMqttPingTimeout(6 * time.Second)(cfg)

	if cfg.mqttWriteTimeout != 3*time.Second {
		t.Errorf("mqttWriteTimeout = %v, want 3s", cfg.mqttWriteTimeout)
	}
	if cfg.mqttKeepAlive != 15*time.Second {
		t.Errorf("mqttKeepAlive = %v, want 15s", cfg.mqttKeepAlive)
	}
	if cfg.mqttPingTimeout != 6*time.Second {
		t.Errorf("mqttPingTimeout = %v, want 6s", cfg.mqttPingTimeout)
	}
}

func TestNewHostApplicationDefaultsMqttTimeouts(t *testing.T) {
	h, err := NewHostApplication(
		[]MqttBrokerConfig{{BrokerURL: "tcp://localhost:1883"}},
		"hostID",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.mqttWriteTimeout != 10*time.Second {
		t.Errorf("default h.mqttWriteTimeout = %v, want 10s", h.mqttWriteTimeout)
	}
	if h.mqttKeepAlive != 30*time.Second {
		t.Errorf("default h.mqttKeepAlive = %v, want 30s", h.mqttKeepAlive)
	}
	if h.mqttPingTimeout != 10*time.Second {
		t.Errorf("default h.mqttPingTimeout = %v, want 10s", h.mqttPingTimeout)
	}
}

// TestInitClientsAppliesMqttTimeoutsToPahoClient closes the loop: it asserts that
// initClients() actually forwards the configured timeouts to the underlying paho
// client (read back via OptionsReader), not just that they land on the
// HostApplication struct.
func TestInitClientsAppliesMqttTimeoutsToPahoClient(t *testing.T) {
	const url = "tcp://localhost:1883"
	h, err := NewHostApplication(
		[]MqttBrokerConfig{{BrokerURL: url}},
		"hostID",
		WithMqttKeepAlive(15*time.Second),
		WithMqttPingTimeout(4*time.Second),
		WithMqttWriteTimeout(7*time.Second),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	h.initClients()

	client := h.mqttClients[url]
	if client == nil {
		t.Fatal("initClients did not create a client for the broker")
	}
	r := client.OptionsReader()
	if got := r.KeepAlive(); got != 15*time.Second {
		t.Errorf("paho KeepAlive = %v, want 15s", got)
	}
	if got := r.PingTimeout(); got != 4*time.Second {
		t.Errorf("paho PingTimeout = %v, want 4s", got)
	}
	if got := r.WriteTimeout(); got != 7*time.Second {
		t.Errorf("paho WriteTimeout = %v, want 7s", got)
	}
}
