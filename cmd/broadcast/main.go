package main

import (
	"encoding/json"
	"fmt"

	"github.com/aaronbittel/pool/internal/node"
)

type BroadcastBody struct {
	node.MsgBody
	Message int `json:"message"`
}

type BroadcastOkBody struct {
	node.MsgBody
}

type ReadBody struct {
	node.MsgBody
}

type ReadOkBody struct {
	node.MsgBody
	Messages []int `json:"messages"`
}

func NewReadOkBody(msgID int, messages []int) ReadOkBody {
	return ReadOkBody{
		MsgBody: node.MsgBody{
			Type:      "read_ok",
			InReplyTo: msgID,
		},
		Messages: messages,
	}
}

type TopologyBody struct {
	node.MsgBody
	Topology map[string][]string `json:"topology"`
}

type TopologyOkBody struct {
	node.MsgBody
}

func NewTopologyOkBody(messages []int) TopologyOkBody {
	return TopologyOkBody{
		MsgBody: node.MsgBody{
			Type: "read_ok",
		},
	}
}

type BroadcastNode struct {
	messages []int
}

func (b *BroadcastNode) Step(msg node.Msg, encoder *json.Encoder) error {
	var body node.MsgBody

	if err := json.Unmarshal(msg.RawBody, &body); err != nil {
		return fmt.Errorf("could not unmarshal msg in body %v: %v", msg, err)
	}

	switch body.Type {
	case "init":
		if err := node.ReplayToInit(msg, body.ID, encoder); err != nil {
			fmt.Errorf("could not reply to init: %v", err)
		}
	case "broadcast":
		var broadcastBody BroadcastBody
		if err := json.Unmarshal(msg.RawBody, &broadcastBody); err != nil {
			return fmt.Errorf("could not unmarshal %v: %v", msg.RawBody, err)
		}
		b.messages = append(b.messages, broadcastBody.Message)
		broadcastOkMsg, err := node.NewOkReply(msg, body.ID, "broadcast_ok")
		if err != nil {
			return fmt.Errorf("could not create BroadcastOk msg: %v", err)
		}
		if err := encoder.Encode(broadcastOkMsg); err != nil {
			return fmt.Errorf("could not encode BroadcastOk msg: %v", err)
		}
	case "read":
		var readBody ReadBody
		if err := json.Unmarshal(msg.RawBody, &readBody); err != nil {
			return fmt.Errorf("could not unmarshal %v: %v", msg.RawBody, err)
		}
		rawReadOkBody, err := json.Marshal(NewReadOkBody(body.ID, b.messages))
		if err != nil {
			return fmt.Errorf("could not marshal readOkBody: %v", err)
		}
		if err := encoder.Encode(node.NewReply(msg, rawReadOkBody)); err != nil {
			return fmt.Errorf("could not encode ReadOkBody msg: %v", err)
		}
	case "topology":
		var topologyBody TopologyBody
		if err := json.Unmarshal(msg.RawBody, &topologyBody); err != nil {
			return fmt.Errorf("could not unmarshal %v: %v", msg.RawBody, err)
		}
		topologyOkMsg, err := node.NewOkReply(msg, body.ID, "topology_ok")
		if err != nil {
			return fmt.Errorf("could not create TopologyOk msg: %v", err)
		}
		if err := encoder.Encode(topologyOkMsg); err != nil {
			return fmt.Errorf("could not encode TopologyOk msg: %v", err)
		}
	default:
		panic("received unknown message type")
	}

	return nil
}

func main() {
	node.MainLoop(&BroadcastNode{messages: []int{}})
}
