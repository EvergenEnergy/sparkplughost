# Example TCK compliant Sparkplug B Host Application

This example demonstrates how this package might be used to connect to a MQTT Broker
using client certificates in order to read and write metrics.

To run it you can use:
```bash
go run . <flags>:

  -broker-url string
        URL of the mqtt broker. E.g., ssl://localhost:1883 (Required)
  -cert string
        Path to the client certificate file (Required)
  -device-id string
        Only log messages from this device ID (Optional)
  -edge-node-id string
        Only log messages from this edge node ID (Optional)
  -group-id string
        Only log messages from this group ID (Optional)
  -host-id string
        This ID will be used as the client ID for the MQTT connection (Required)
  -key string
        Path to the private key file (Required)
  -metric-name string
        Only log messages for this metric (Optional)
```

## Reading metrics
The application will connect to the MQTT Broker and listen for BIRTH and DATA messages, logging
them to std out like:

```
time=2023-11-15T20:22:57.647-05:00 level=INFO msg="Received metric callback" metric_name=WriteableDouble2 edge_node_descriptor=TestGroup/E0 metric_type=Double device_id=Writeable quality=GOOD value=&{DoubleValue:789}
```

It also handles message reordering and will send `Node Control/Rebirth` requests in the event of invalid or out of order messages.

## Writing metrics

The application is also capable of sending `NCMD` and `DCMD` messages to change the value of metrics. 
In order to do this it will start a simple http server on port 8080 with 4 endpoints:
- `/send-edge-node-rebirth-request`
- `/send-device-rebirth-request`
- `/write-edge-node-metric`
- `/write-device-metric`

All of them take the same 3 query parameters to determine which Edge Node or Device to send the command to:
- `group_id`
- `edge_node_id`
- `device_id` (not needed if sending an Edge Node command)

### Examples

Writing device metrics of different types:

```bash
$> curl --json '[{"name":"WriteableDouble2","value":15.4, "data_type": "Double"}]' "localhost:8080/write-device-metric?group_id=TestGroup&edge_node_id=E0&device_id=Writeable"

$> curl --json '[{"name":"WriteableBoolean1","value":true, "data_type": "Boolean"}]' "localhost:8080/write-device-metric?group_id=TestGroup&edge_node_id=E0&device_id=Writeable"

$> curl --json '[{"name":"WriteableInteger1","value":10, "data_type": "Int32"}]' "localhost:8080/write-device-metric?group_id=TestGroup&edge_node_id=E0&device_id=Writeable"
```

Request a node rebirth:
```bash
curl -X POST "localhost:8080/send-edge-node-rebirth-request?group_id=TestGroup&edge_node_id=E0"
```

Request a device rebirth:
```bash
curl -X POST "localhost:8080/send-device-rebirth-request?group_id=TestGroup&edge_node_id=E0&device_id=Writeable"
```