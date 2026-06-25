package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aaronbittel/pool/internal/node"
)

type GossipBody struct {
	node.MsgBody
	Messages []int `json:"messages"`
}

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

func NewReadOkBody(msgID, replyMsgID int, messages []int) ReadOkBody {
	return ReadOkBody{
		MsgBody: node.MsgBody{
			Type:      "read_ok",
			InReplyTo: replyMsgID,
			ID:        msgID,
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
			Type: "topology_ok",
		},
	}
}

type BroadcastNode struct {
	name      string
	encoder   *json.Encoder
	nextMsgID int

	mutex    sync.Mutex // protects messages
	messages map[int]struct{}

	topology map[string][]string
}

func (b *BroadcastNode) SetEncoder(encoder *json.Encoder) {
	b.encoder = encoder
}

func (b *BroadcastNode) newID() int {
	id := b.nextMsgID
	b.nextMsgID += 1
	return id
}

func (b *BroadcastNode) messageSlice() []int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	messages := make([]int, 0, len(b.messages))
	for m := range b.messages {
		messages = append(messages, m)
	}
	return messages
}

func (b *BroadcastNode) gossip() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, neighbor := range b.topology[b.name] {
				body := GossipBody{
					MsgBody:  node.MsgBody{Type: "gossip"},
					Messages: b.messageSlice(),
				}
				raw, err := json.Marshal(body)
				if err != nil {
					panic(err)
				}
				msg := node.Msg{
					Src:     b.name,
					Dst:     neighbor,
					RawBody: raw,
				}
				if err := b.encoder.Encode(msg); err != nil {
					panic(err)
				}
			}
		}
	}
}

func (b *BroadcastNode) Step(msg node.Msg, encoder *json.Encoder) error {
	var body node.MsgBody

	if err := json.Unmarshal(msg.RawBody, &body); err != nil {
		return fmt.Errorf("could not unmarshal msg in body %v: %v", msg, err)
	}

	switch body.Type {
	case "init":
		var initBody node.InitBody
		if err := json.Unmarshal(msg.RawBody, &initBody); err != nil {
			return fmt.Errorf("could not unmarshal rawbody: %v", err)
		}
		b.name = initBody.NodeID

		if err := node.ReplayToInit(msg, b.newID(), body.ID, encoder); err != nil {
			return fmt.Errorf("could not reply to init: %v", err)
		}
	case "broadcast":
		var broadcastBody BroadcastBody
		if err := json.Unmarshal(msg.RawBody, &broadcastBody); err != nil {
			return fmt.Errorf("could not unmarshal %v: %v", msg.RawBody, err)
		}

		b.mutex.Lock()
		b.messages[broadcastBody.Message] = struct{}{}
		b.mutex.Unlock()

		broadcastOkMsg, err := node.NewOkReply(msg, b.newID(), body.ID, "broadcast_ok")
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
		rawReadOkBody, err := json.Marshal(NewReadOkBody(b.newID(), body.ID, b.messageSlice()))
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
		b.topology = topologyBody.Topology
		topologyOkMsg, err := node.NewOkReply(msg, b.newID(), body.ID, "topology_ok")
		if err != nil {
			return fmt.Errorf("could not create TopologyOk msg: %v", err)
		}
		if err := encoder.Encode(topologyOkMsg); err != nil {
			return fmt.Errorf("could not encode TopologyOk msg: %v", err)
		}
		go b.gossip()
	case "gossip":
		var gossipBody GossipBody
		if err := json.Unmarshal(msg.RawBody, &gossipBody); err != nil {
			panic(err)
		}
		b.mutex.Lock()
		for _, m := range gossipBody.Messages {
			b.messages[m] = struct{}{}
		}
		b.mutex.Unlock()
	case "broadcast_ok", "read_ok", "topology_ok":
	default:
		panic(fmt.Sprintf("received unknown message type %q", body.Type))
	}

	return nil
}

func main() {
	node.MainLoop(&BroadcastNode{messages: make(map[int]struct{})})
}
