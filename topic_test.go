package sparkplughost

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseTopic(t *testing.T) {
	tests := []struct {
		topic     string
		wantErr   bool
		wantTopic topic
	}{
		{
			// wrong namespace
			topic:   "spAv1.0/STATE/host_id",
			wantErr: true,
		},
		{
			topic: "spBv1.0/STATE/test-host-id",
			wantTopic: topic{
				namespace:   "spBv1.0",
				messageType: messageTypeSTATE,
				hostID:      "test-host-id",
			},
		},
		{
			// missing host id
			topic:   "spBv1.0/STATE",
			wantErr: true,
		},
		{
			// missing host id
			topic:   "spBv1.0/STATE/foo/bar",
			wantErr: true,
		},
		{
			// empty host id
			topic:   "spBv1.0/STATE/",
			wantErr: true,
		},
		{
			// invalid message type
			topic:   "spBv1.0/group/NFOO/edge",
			wantErr: true,
		},
		{
			topic: "spBv1.0/test-group-id/NBIRTH/test-edge-node-id",
			wantTopic: topic{
				namespace:   "spBv1.0",
				messageType: messageTypeNBIRTH,
				groupID:     "test-group-id",
				edgeNodeID:  "test-edge-node-id",
			},
		},
		{
			topic:   "spBv1.0/test-group-id/NBIRTH",
			wantErr: true,
		},
		{
			topic:   "spBv1.0/test-group-id/NBIRTH/edge/device",
			wantErr: true,
		},
		{
			topic:   "spBv1.0/test-group-id/NBIRTH/",
			wantErr: true,
		},
		{
			topic:   "spBv1.0//NBIRTH/test-edge-node-id",
			wantErr: true,
		},
		{
			topic: "spBv1.0/test-group-id/DBIRTH/test-edge-node-id/test-device-id",
			wantTopic: topic{
				namespace:   "spBv1.0",
				messageType: messageTypeDBIRTH,
				groupID:     "test-group-id",
				edgeNodeID:  "test-edge-node-id",
				deviceID:    "test-device-id",
			},
		},
		{
			topic:   "spBv1.0/test-group-id/DBIRTH/test-edge-node-id",
			wantErr: true,
		},
		{
			topic:   "spBv1.0/test-group-id/DBIRTH/test-edge-node-id/",
			wantErr: true,
		},
		{
			topic:   "spBv1.0/test-group-id/DBIRTH//test-device-id",
			wantErr: true,
		},
		{
			topic:   "spBv1.0//DBIRTH/test-edge-node-id/test-device-id",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.topic, func(t *testing.T) {
			got, err := parseTopic(test.topic)
			if test.wantErr != (err != nil) {
				t.Errorf("wantErr was %t but err was %s", test.wantErr, err)
			}

			if diff := cmp.Diff(test.wantTopic, got, cmp.AllowUnexported(topic{})); diff != "" {
				t.Errorf("parseTopic() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
