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
}

func TestWithMqttTimeoutOptions(t *testing.T) {
	cfg := defaultConfig()
	WithMqttWriteTimeout(3 * time.Second)(cfg)
	WithMqttKeepAlive(15 * time.Second)(cfg)

	if cfg.mqttWriteTimeout != 3*time.Second {
		t.Errorf("mqttWriteTimeout = %v, want 3s", cfg.mqttWriteTimeout)
	}
	if cfg.mqttKeepAlive != 15*time.Second {
		t.Errorf("mqttKeepAlive = %v, want 15s", cfg.mqttKeepAlive)
	}
}

func TestNewHostApplicationPropagatesMqttTimeouts(t *testing.T) {
	h, err := NewHostApplication(
		[]MqttBrokerConfig{{BrokerURL: "tcp://localhost:1883"}},
		"hostID",
		WithMqttWriteTimeout(7*time.Second),
		WithMqttKeepAlive(12*time.Second),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.mqttWriteTimeout != 7*time.Second {
		t.Errorf("h.mqttWriteTimeout = %v, want 7s", h.mqttWriteTimeout)
	}
	if h.mqttKeepAlive != 12*time.Second {
		t.Errorf("h.mqttKeepAlive = %v, want 12s", h.mqttKeepAlive)
	}
}

// TestNewHostApplicationDefaultsMqttTimeouts guards the reconnection-robustness
// default: a fresh host must carry a non-zero write timeout so AutoReconnect can
// recover from a half-open connection out of the box.
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
}
