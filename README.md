# Sparkplug B Host Application Go library
Go based package to use for implementing Sparkplug B Host Applications

# Sparkplug Host Library for Go

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/EvergenEnergy/sparkplughost)](https://goreportcard.com/report/github.com/EvergenEnergy/sparkplughost)
[![Coverage Status](https://coveralls.io/repos/github/EvergenEnergy/sparkplug-host/badge.svg?branch=coverall)](https://coveralls.io/github/EvergenEnergy/sparkplug-host?branch=coverall)
[![Go Reference](https://pkg.go.dev/badge/github.com/EvergenEnergy/sparkplughost.svg)](https://pkg.go.dev/github.com/EvergenEnergy/sparkplughost)
[![ci](https://github.com/EvergenEnergy/sparkplughost/actions/workflows/ci.yaml/badge.svg)](https://github.com/EvergenEnergy/sparkplughost/actions/workflows/ci.yaml)


The Sparkplug Host Library for Go is designed to assist developers in creating custom Host Applications adhering to the Sparkplug standard. 
It simplifies the implementation of MQTT-based communication for industrial IoT environments, 
allowing seamless integration with Sparkplug-compliant edge nodes and devices.

## Features

- **Sparkplug B Standard Support**: Implement Sparkplug B standard communication effortlessly.
- **Customizable Callbacks**: Receive real-time updates with customizable callback functions for metrics, births, commands, and other events.
- **Command Writing Support**: Use the library to write commands to both edge nodes and devices.
- **Flexible Design**: Create dynamic and tailored Host Applications suited to your specific use case.

## Installation

```bash
go get -u github.com/EvergenEnergy/sparkplughost
```

This package requires Go version 1.21 or higher.

## Usage

```go
// 1. Configure your MQTT Brokers
brokerConfig := sparkplughost.MqttBrokerConfig{BrokerURL: "tcp://broker.hivemq.com:1883"}

// 2. Create a handler function that will be called whenever a metric is added or updated
metricHandler := func(metric sparkplughost.HostMetric) {
    log.Printf("Received metric callback:%v\n", metric)
}

// 3. Create the `HostApplication`. See options.go for further configuration options.
host, err := sparkplughost.NewHostApplication(
    []sparkplughost.MqttBrokerConfig{brokerConfig},
    "my-host-id",
    sparkplughost.WithMetricHandler(metricHandler),
)
if err != nil {
    panic(err)
}

// 4. Start the application. The `Run` function will block until
// `ctx` is cancelled.
go func() {
    if err := host.Run(ctx); err != nil {
        panic(err)
    }
}()

// 5. Optionally, use the `HostApplication` methods to send commands
// to Edge Nodes and Devices
// host.SendEdgeNodeCommand(sparkplughost.EdgeNodeDescriptor{GroupID:"group-id", EdgeNodeID:"edge-node-id"}, metrics)
// host.SendDeviceCommand(sparkplughost.EdgeNodeDescriptor{GroupID:"group-id", EdgeNodeID:"edge-node-id"},deviceID, metrics)
```

Samples are available in the [examples](examples) directory for reference.

## Important Notice

**This library is under heavy development, and the API may change at any point.**

We are actively working towards stabilizing the API, and once it reaches a stable state, a v1.0.0 version will be released. 
From that point forward, the library will adhere to the normal guarantees of semantic versioning.

## Contributing

We welcome contributions from the community! Please check our [Contribution Guidelines](CONTRIBUTING.md) for details on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
