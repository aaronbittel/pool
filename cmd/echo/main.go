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
	if event.Kind != node.KindMessage {
		panic("got injected event when there's no event injection")
	}

	switch event.Msg.Type {
	case "init":
		if err := node.ReplayToInit(event.Msg, e.newID(), event.Msg.ID, encoder); err != nil {
			fmt.Errorf("could not reply to init: %v", err)
		}
	case "echo":
		var echo EchoBody
		if err := json.Unmarshal(event.Msg.RawBody, &echo); err != nil {
			fmt.Errorf("could not unmarshal raw msg body into EchoBody: %v", err)
		}
		reply, err := node.ReplyTo(*event.Msg, e.echoOKBody(echo))
		if err != nil {
			return err
		}
		if err := reply.Send(encoder); err != nil {
			return err
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
