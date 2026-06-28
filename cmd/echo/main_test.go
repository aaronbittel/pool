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
			initMsg := newTestMsg(t, "source", "destination", node.MsgBody{
				Type: "init",
				ID:   id,
			}, node.InitBody{
				NodeID:  "node1",
				NodeIDs: []string{"node1", "node2", "node3"},
			})

			msgBody := node.MsgBody{
				Type:      "init_ok",
				ID:        1,
				InReplyTo: id,
			}
			wantInitOkMsg := newTestOkMsg(t, "destination", "source", msgBody, node.InitOKBody{
				MsgBody: msgBody,
			})

			jsonRecorder := newTestJsonRecorder(&EchoNode{})
			gotInitOkMsg := jsonRecorder.step(t, node.Event{Kind: node.KindMessage, Msg: &initMsg})
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

func (jr *jsonRecorder) step(t *testing.T, event node.Event) node.Msg {
	require.NoError(t, jr.node.Step(event, json.NewEncoder(jr.buf)))
	var resp node.Msg
	require.NoError(t, json.NewDecoder(jr.buf).Decode(&resp))
	return resp
}

func newTestMsg(t *testing.T, src, dst string, msgBody node.MsgBody, payload any) node.Msg {
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	return node.Msg{
		Src:     src,
		Dst:     dst,
		MsgBody: msgBody,
		RawBody: raw,
	}
}

func newTestOkMsg(t *testing.T, src, dst string, body node.MsgBody, payload any) node.Msg {
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	return node.Msg{
		Src:     src,
		Dst:     dst,
		MsgBody: body,
		RawBody: raw,
	}
}
