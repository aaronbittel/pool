package main

import (
	"crypto/rand"
	"encoding/json"

	"github.com/aaronbittel/pool/internal/node"
)

type GenerateOkBody struct {
	node.MsgBody
	GeneratedID string `json:"id"`
}

type UniqueIdNode struct {
	ID int
}

func (u UniqueIdNode) InitNode(_events chan node.Event) {
	u.ID = 0
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
	case "init":
		initOkBody := node.InitOKBody{MsgBody: reply.MsgBody}
		if err := reply.MarshalBody(initOkBody); err != nil {
			return err
		}
		if err := reply.Send(encoder); err != nil {
			return err
		}
	case "init_ok":
		panic("received init_ok")
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
	node.MainLoop(UniqueIdNode{})
}
