package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aaronbittel/pool/internal/node"
)

type GossipBody struct {
	node.MsgBody
	Messages []int `json:"messages"`
}

type BroadcastBody struct {
	Message int `json:"message"`
}

type BroadcastOkBody struct {
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
	Topology map[string][]string `json:"topology"`
}

type TopologyOkBody struct {
	node.MsgBody
}

type set[T comparable] map[T]struct{}

type BroadcastNode struct {
	name string
	id   int

	messages set[int]

	topology map[string][]string

	// all messages of a node that was sent to this node via a gossip message
	// these messages dont not need to be sent to them in a gossip message
	known map[string]set[int]
	// extraPerc specifies the percentage (0–100) by which the number of messages sent
	// to a neighbor in a gossip round may be increased above the baseline. For example,
	// if the baseline is 10 messages and extraPerc is 10, then the node may send up to
	// 1 additional message (i.e., 11 total).
	extraPerc int
}

type GossipEvent struct{}

func (GossipEvent) IsInjected() {}

func (b *BroadcastNode) InitNode(initBody node.InitBody, events chan node.Event) node.Node {
	b.name = initBody.NodeID
	// start from 1 because MainLoop write the first message (init)
	b.id = 1
	b.messages = make(set[int])
	b.known = make(map[string]set[int])
	b.extraPerc = 10

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				events <- node.Event{
					Kind:     node.KindInjected,
					Injected: GossipEvent{},
				}
			}
		}
	}()

	return b
}

func (b *BroadcastNode) messagesAsSlice() []int {
	messages := make([]int, 0, len(b.messages))
	for m := range b.messages {
		messages = append(messages, m)
	}
	return messages
}

func (b *BroadcastNode) Step(event node.Event, encoder *json.Encoder) error {
	switch event.Kind {
	case node.KindInjected:
		// received an event to do gossip
		for _, neighbor := range b.topology[b.name] {
			allMessages := b.messagesAsSlice()
			additional := ((len(allMessages) - len(b.known[neighbor])) * b.extraPerc) / 100
			sendToNeighbor := []int{}
			for _, message := range allMessages {
				if _, ok := b.known[neighbor][message]; !ok {
					sendToNeighbor = append(sendToNeighbor, message)
				} else if additional > 0 {
					sendToNeighbor = append(sendToNeighbor, message)
					additional -= 1
				}
			}

			msg := node.Msg{
				Src: b.name,
				Dst: neighbor,
			}
			gossipBody := GossipBody{
				MsgBody:  node.MsgBody{Type: "gossip"},
				Messages: sendToNeighbor,
			}
			msg.MarshalBody(gossipBody)
			if err := encoder.Encode(msg); err != nil {
				return err
			}
		}
	case node.KindMessage:
		reply := event.Msg.IntoReply(&b.id)

		switch event.Msg.Type {
		case "broadcast":
			var broadcastBody BroadcastBody
			if err := json.Unmarshal(event.Msg.RawBody, &broadcastBody); err != nil {
				return err
			}

			b.messages[broadcastBody.Message] = struct{}{}

			payload := BroadcastOkBody{MsgBody: reply.MsgBody}
			if err := reply.MarshalBody(payload); err != nil {
				return err
			}

			if err := reply.Send(encoder); err != nil {
				return err
			}
		case "read":
			payload := ReadOkBody{
				MsgBody:  reply.MsgBody,
				Messages: b.messagesAsSlice(),
			}
			if err := reply.MarshalBody(payload); err != nil {
				return err
			}

			if err := reply.Send(encoder); err != nil {
				return err
			}
		case "topology":
			var topologyBody TopologyBody
			if err := json.Unmarshal(event.Msg.RawBody, &topologyBody); err != nil {
				return err
			}

			b.topology = topologyBody.Topology
			for _, neigbour := range b.topology[b.name] {
				b.known[neigbour] = make(set[int])
			}

			payload := TopologyOkBody{MsgBody: reply.MsgBody}
			if err := reply.MarshalBody(payload); err != nil {
				return err
			}

			if err := reply.Send(encoder); err != nil {
				return err
			}
		case "gossip":
			var gossipBody GossipBody
			if err := json.Unmarshal(event.Msg.RawBody, &gossipBody); err != nil {
				panic(err)
			}
			for _, m := range gossipBody.Messages {
				b.known[reply.Dst][m] = struct{}{}
				b.messages[m] = struct{}{}
			}
		case "broadcast_ok", "read_ok", "topology_ok":
		default:
			panic(fmt.Sprintf("received unknown message type %q", event.Msg.Type))
		}
	default:
		panic(fmt.Sprintf("got unexpected event kind %d", event.Kind))
	}

	return nil
}

func main() {
	if err := node.MainLoop(&BroadcastNode{}); err != nil {
		fmt.Fprintf(os.Stderr, "BroadcastNode failed: %v", err)
		os.Exit(1)
	}
}
