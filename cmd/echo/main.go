package main

import (
	"encoding/json"
	"fmt"

	"github.com/aaronbittel/pool/internal/node"
)

type EchoBody struct {
	node.MsgBody
	Echo string `json:"echo"`
}

type EchoOKBody struct {
	node.MsgBody
	Echo string `json:"echo"`
}

type EchoNode struct {
	nextMsgID int
}

func (e *EchoNode) InitNode(_events chan node.Event) {
	e.nextMsgID = 0
}

func (e *EchoNode) echoOKBody(echo EchoBody) EchoOKBody {
	return EchoOKBody{
		MsgBody: node.MsgBody{
			Type:      "echo_ok",
			ID:        e.newID(),
			InReplyTo: echo.ID,
		},
		Echo: echo.Echo,
	}
}

func (e *EchoNode) newID() int {
	id := e.nextMsgID
	e.nextMsgID += 1
	return id
}

func (e *EchoNode) Step(event node.Event, encoder *json.Encoder) error {
	if event.Kind != node.Message {
		panic("got injected event when there's no event injection")
	}

	msg := event.Msg

	var body node.MsgBody

	if err := json.Unmarshal(msg.RawBody, &body); err != nil {
		return fmt.Errorf("could not unmarshal msg in body %v: %v", msg, err)
	}

	switch body.Type {
	case "init":
		if err := node.ReplayToInit(msg, e.newID(), body.ID, encoder); err != nil {
			fmt.Errorf("could not reply to init: %v", err)
		}
	case "echo":
		var echo EchoBody
		if err := json.Unmarshal(msg.RawBody, &echo); err != nil {
			fmt.Errorf("could not unmarshal raw msg body into EchoBody: %v", err)
		}
		rawEchoOK, err := json.Marshal(e.echoOKBody(echo))
		if err != nil {
			fmt.Errorf("could not marshal echoOkBody: %v", err)
		}
		if err := encoder.Encode(node.NewReply(msg, rawEchoOK)); err != nil {
			return fmt.Errorf("could not encode echo replay: %v", err)
		}
	case "echo_ok":
	case "init_ok":
		panic("received init_ok msg")
	default:
		panic("illegal msg type")
	}

	return nil
}

func main() {
	node.MainLoop(&EchoNode{})
}
