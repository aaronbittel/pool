package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aaronbittel/pool/internal/node"
)

// TODO: maybe create another event that emits every 10s where each nodes sents all
// messages to each neighbor which itself knows and it does not know that the other
// knows of it. This should reset the messages needed to be sent in gossips to zero till
// new messages arrive.
// Or if not send all, send a few extra that are not yet known.

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

type set[T comparable] map[T]struct{}

type BroadcastNode struct {
	name      string
	encoder   *json.Encoder
	nextMsgID int

	messageMutex sync.Mutex // protects messages
	messages     set[int]

	topology map[string][]string

	knownMutex sync.Mutex // protects known
	// all messages of a node that was sent to this node via a gossip message
	// these messages dont not need to be sent to them in a gossip message
	known map[string]set[int]
}

func (b *BroadcastNode) InitNode(encoder *json.Encoder, events chan node.Event) {
	b.encoder = encoder
	b.nextMsgID = 0
	b.messages = make(set[int])
	b.known = make(map[string]set[int])

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				events <- node.Event{Kind: node.Injected}
			}
		}
	}()
}

func (b *BroadcastNode) newID() int {
	id := b.nextMsgID
	b.nextMsgID += 1
	return id
}

func (b *BroadcastNode) messageSlice() []int {
	b.messageMutex.Lock()
	defer b.messageMutex.Unlock()
	messages := make([]int, 0, len(b.messages))
	for m := range b.messages {
		messages = append(messages, m)
	}
	return messages
}

func (b *BroadcastNode) Step(event node.Event, encoder *json.Encoder) error {
	switch event.Kind {
	case node.Injected:
		// received an event to do gossip
		for _, neighbor := range b.topology[b.name] {
			sendMessages := []int{}
			b.knownMutex.Lock()
			for _, message := range b.messageSlice() {
				if _, ok := b.known[neighbor][message]; !ok {
					sendMessages = append(sendMessages, message)
				}
			}
			b.knownMutex.Unlock()

			body := GossipBody{
				MsgBody:  node.MsgBody{Type: "gossip"},
				Messages: sendMessages,
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
	case node.Message:
		msg := event.Msg

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

			b.messageMutex.Lock()
			b.messages[broadcastBody.Message] = struct{}{}
			b.messageMutex.Unlock()

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
			b.knownMutex.Lock()
			for _, neigbour := range b.topology[b.name] {
				b.known[neigbour] = make(set[int])
			}
			b.knownMutex.Unlock()
			topologyOkMsg, err := node.NewOkReply(msg, b.newID(), body.ID, "topology_ok")
			if err != nil {
				return fmt.Errorf("could not create TopologyOk msg: %v", err)
			}
			if err := encoder.Encode(topologyOkMsg); err != nil {
				return fmt.Errorf("could not encode TopologyOk msg: %v", err)
			}
		case "gossip":
			var gossipBody GossipBody
			if err := json.Unmarshal(msg.RawBody, &gossipBody); err != nil {
				panic(err)
			}
			b.messageMutex.Lock()
			b.knownMutex.Lock()
			for _, m := range gossipBody.Messages {
				b.known[msg.Src][m] = struct{}{}
				b.messages[m] = struct{}{}
			}
			b.knownMutex.Unlock()
			b.messageMutex.Unlock()
		case "broadcast_ok", "read_ok", "topology_ok":
		default:
			panic(fmt.Sprintf("received unknown message type %q", body.Type))
		}
	default:
		panic(fmt.Sprintf("got unexpected event kind %d", event.Kind))
	}

	return nil
}

func main() {
	node.MainLoop(&BroadcastNode{})
}
