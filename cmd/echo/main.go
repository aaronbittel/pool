package main

import (
	"encoding/json"
	"fmt"

	"github.com/aaronbittel/pool/internal/node"
)

type EchoBody struct {
	Echo string `json:"echo"`
}

type EchoOKBody struct {
	node.MsgBody
	Echo string `json:"echo"`
}

type EchoNode struct {
	id int
}

func (e *EchoNode) InitNode(_events chan node.Event) {
	e.id = 0
}

func (e *EchoNode) Step(event node.Event, encoder *json.Encoder) error {
	if event.Kind != node.KindMessage {
		panic("got injected event when there's no event injection")
	}

	reply := event.Msg.IntoReply(&e.id)

	switch event.Msg.Type {
	case "init":
		initOkBody := node.InitOKBody{MsgBody: reply.MsgBody}
		if err := reply.MarshalBody(initOkBody); err != nil {
			return err
		}
		if err := reply.Send(encoder); err != nil {
			return err
		}
	case "echo":
		var echo EchoBody
		if err := json.Unmarshal(event.Msg.RawBody, &echo); err != nil {
			return err
		}
		payload := EchoOKBody{MsgBody: reply.MsgBody, Echo: echo.Echo}
		if err := reply.MarshalBody(payload); err != nil {
			return err
		}
		if err := reply.Send(encoder); err != nil {
			return err
		}
	case "echo_ok":
	case "init_ok":
		panic("received init_ok msg")
	default:
		panic(fmt.Sprintf("illegal msg type %q", event.Msg.Type))
	}

	return nil
}

func main() {
	node.MainLoop(&EchoNode{})
}
