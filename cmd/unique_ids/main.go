package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aaronbittel/pool/internal/node"
)

type GenerateOkBody struct {
	node.MsgBody
	GeneratedID string `json:"id"`
}

type UniqueIdNode struct {
	ID int
}

func (u UniqueIdNode) InitNode(_initBody node.InitBody, _events chan node.Event) node.Node {
	u.ID = 0
	return u
}

func (u UniqueIdNode) generateNewID() string {
	return rand.Text()
}

func (u UniqueIdNode) Step(event node.Event, encoder *json.Encoder) error {
	if event.Kind != node.KindMessage {
		panic("got injected event when there's no event injection")
	}

	reply := event.Msg.IntoReply(&u.ID)

	switch event.Msg.Type {
	case "generate":
		idOkBody := GenerateOkBody{
			MsgBody:     reply.MsgBody,
			GeneratedID: u.generateNewID(),
		}
		if err := reply.MarshalBody(idOkBody); err != nil {
			return err
		}
		if err := reply.Send(encoder); err != nil {
			return err
		}
	default:
		panic("received unknown msg type")
	}

	return nil
}
func main() {
	if err := node.MainLoop(UniqueIdNode{}); err != nil {
		fmt.Fprintf(os.Stderr, "UniqueIdNode failed: %v", err)
		os.Exit(1)
	}
}
