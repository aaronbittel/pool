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
		extraPerc      int
		wantLen        int
		expectElements []int
	}{
		{
			name:           "without extra",
			extraPerc:      0,
			wantLen:        5,
			expectElements: []int{6, 7, 8, 9, 10},
		},
		{
			name:      "one extra",
			extraPerc: 20,
			wantLen:   6,
			// cant say exactly what elemnt will be added, because its ranging over a map
			// but these must be contained
			expectElements: []int{6, 7, 8, 9, 10},
		},
		{
			name:           "2 more",
			extraPerc:      50,
			wantLen:        7,
			expectElements: []int{6, 7, 8, 9, 10},
		},
		{
			name:           "double the size",
			extraPerc:      100,
			wantLen:        10,
			expectElements: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node0 := BroadcastNode{
				name:     "node0",
				topology: map[string][]string{"node0": []string{"node1"}},
				messages: newSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
				known: map[string]set[int]{
					"node1": newSet(1, 2, 3, 4, 5),
				},
				extraPerc: tt.extraPerc,
			}

			var buf bytes.Buffer
			bytesEncoder := json.NewEncoder(&buf)
			require.NoError(t, node0.Step(node.Event{Kind: node.KindInjected}, bytesEncoder))

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
