package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"

	"github.com/aaronbittel/pool/internal/node"
)

type GenerateOkBody struct {
	node.MsgBody
	GeneratedID string `json:"id"`
}

type UniqueIdNode struct {
	nextMsgID int
}

func (u UniqueIdNode) InitNode(_encoder *json.Encoder, _messages chan node.Msg) {
	u.nextMsgID = 0
}

func (u UniqueIdNode) generateNewID() string {
	return rand.Text()
}

func (u *UniqueIdNode) newID() int {
	id := u.nextMsgID
	u.nextMsgID += 1
	return id
}

func (u UniqueIdNode) Step(msg node.Msg, encoder *json.Encoder) error {
	var body node.MsgBody

	if err := json.Unmarshal(msg.RawBody, &body); err != nil {
		return fmt.Errorf("could not unmarshal msg in body %v: %v", msg, err)
	}

	switch body.Type {
	case "init":
		if err := node.ReplayToInit(msg, u.newID(), body.ID, encoder); err != nil {
			fmt.Errorf("could not reply to init: %v", err)
		}
	case "init_ok":
		panic("received init_ok")
	case "generate":
		idOkBody := GenerateOkBody{
			MsgBody: node.MsgBody{
				Type:      "generate_ok",
				InReplyTo: body.ID,
			},
			GeneratedID: u.generateNewID(),
		}
		rawIdOkBody, err := json.Marshal(idOkBody)
		if err != nil {
			return fmt.Errorf("could not marshal idOkBody %v: %v", idOkBody, err)
		}
		idOkMsg := node.NewReply(msg, rawIdOkBody)
		if err := encoder.Encode(idOkMsg); err != nil {
			return fmt.Errorf("could not encode %v: %v", idOkMsg, err)
		}
	default:
		panic("received unknown msg type")
	}

	return nil
}
func main() {
	node.MainLoop(UniqueIdNode{})
}
