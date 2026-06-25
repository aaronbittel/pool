package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aaronbittel/pool/internal/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEchoNode(t *testing.T) {
	for _, id := range []int{1, 10, 100} {
		t.Run(fmt.Sprintf("id:%d", id), func(t *testing.T) {
			initMsg := NewInitMsg(t, "source", "destination", node.InitBody{
				MsgBody: node.MsgBody{
					Type: "init",
					ID:   id,
				},
				NodeID:  "node1",
				NodeIDs: []string{"node1", "node2", "node3"},
			})

			wantInitOkMsg := NewInitOkMsg(t, "destination", "source", node.MsgBody{
				Type:      "init_ok",
				ID:        0,
				InReplyTo: id,
			})

			jsonRecorder := newTestJsonRecorder(&EchoNode{})
			gotInitOkMsg := jsonRecorder.step(t, initMsg)
			assert.Equal(t, wantInitOkMsg, gotInitOkMsg)
		})
	}
}

type jsonRecorder struct {
	node node.Node
	buf  *bytes.Buffer
}

func newTestJsonRecorder(node node.Node) *jsonRecorder {
	return &jsonRecorder{
		node: node,
		buf:  new(bytes.Buffer),
	}
}

func (jr *jsonRecorder) step(t *testing.T, msg node.Msg) node.Msg {
	require.NoError(t, jr.node.Step(msg, json.NewEncoder(jr.buf)))
	var resp node.Msg
	require.NoError(t, json.NewDecoder(jr.buf).Decode(&resp))
	return resp
}

func NewInitMsg(t *testing.T, src, dst string, init node.InitBody) node.Msg {
	rawInitBody, err := json.Marshal(init)
	require.NoError(t, err)

	return node.Msg{
		Src:     src,
		Dst:     dst,
		RawBody: rawInitBody,
	}
}

func NewInitOkMsg(t *testing.T, src, dst string, msgBody node.MsgBody) node.Msg {
	rawInitBody, err := json.Marshal(node.InitOKBody{msgBody})
	require.NoError(t, err)

	return node.Msg{
		Src:     src,
		Dst:     dst,
		RawBody: rawInitBody,
	}
}
