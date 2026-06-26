package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/aaronbittel/pool/internal/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGossipExtraMessages(t *testing.T) {
	tests := []struct {
		name           string
		extra          int
		wantLen        int
		expectElements []int
	}{
		{
			name:           "no extra sent",
			extra:          0,
			wantLen:        3,
			expectElements: []int{3, 4, 5},
		},
		{
			name:    "one extra",
			extra:   1,
			wantLen: 4,
			// cant say exactly what elemnt will be added, because its ranging over a map
			// but these must be contained
			expectElements: []int{3, 4, 5},
		},
		{
			name:           "all",
			extra:          2,
			wantLen:        5,
			expectElements: []int{3, 4, 5, 1, 2},
		},
		{
			name:           "extra greater list len",
			extra:          100,
			wantLen:        5,
			expectElements: []int{3, 4, 5, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node0 := BroadcastNode{
				name:     "node0",
				topology: map[string][]string{"node0": []string{"node1"}},
				messages: newSet(1, 2, 3, 4, 5),
				known: map[string]set[int]{
					"node1": newSet(1, 2),
				},
				extra: tt.extra,
			}

			var buf bytes.Buffer
			bytesEncoder := json.NewEncoder(&buf)
			require.NoError(t, node0.Step(node.Event{Kind: node.Injected}, bytesEncoder))

			var msg node.Msg
			require.NoError(t, json.Unmarshal(buf.Bytes(), &msg))

			var msgBody node.MsgBody
			require.NoError(t, json.Unmarshal(msg.RawBody, &msgBody))

			require.Equal(t, msgBody.Type, "gossip")
			var gossipBody GossipBody
			require.NoError(t, json.Unmarshal(msg.RawBody, &gossipBody))

			assert.Equal(t, tt.wantLen, len(gossipBody.Messages))

			for _, elem := range tt.expectElements {
				assert.Contains(t, gossipBody.Messages, elem)
			}
		})
	}
}

func newSet[T comparable](values ...T) set[T] {
	s := make(set[T], len(values))
	for _, v := range values {
		s[v] = struct{}{}
	}
	return s
}
