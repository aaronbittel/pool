package node

import (
	"encoding/json"
	"fmt"
	"os"
)

type Msg struct {
	Src     string          `json:"src"`
	Dst     string          `json:"dest"`
	RawBody json.RawMessage `json:"body"`
}

func NewReply(msg Msg, rawBody json.RawMessage) Msg {
	return Msg{
		Src:     msg.Dst,
		Dst:     msg.Src,
		RawBody: rawBody,
	}
}

type MsgBody struct {
	Type      string `json:"type"`
	ID        int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to"`
}

type InitBody struct {
	MsgBody
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type InitOKBody struct {
	MsgBody
}

type Node interface {
	Step(msg Msg, encoder *json.Encoder) error
}

func MainLoop(node Node) {
	var (
		stdinDecoder  = json.NewDecoder(os.Stdin)
		stdoutEncoder = json.NewEncoder(os.Stdout)
		msg           Msg
	)

	for {
		if err := stdinDecoder.Decode(&msg); err != nil {
			panic(err)
		}

		if err := node.Step(msg, stdoutEncoder); err != nil {
			panic(err)
		}
	}
}

func ReplayToInit(msg Msg, msgID int, encoder *json.Encoder) error {
	rawInitOK, err := json.Marshal(NewInitOKBody(msgID))
	if err != nil {
		return fmt.Errorf("could not marshal initOkBody: %v", err)
	}
	if err := encoder.Encode(NewReply(msg, rawInitOK)); err != nil {
		return fmt.Errorf("could not encode init replay: %v", err)
	}
	return nil
}

func NewInitOKBody(msgID int) InitOKBody {
	return InitOKBody{
		MsgBody: MsgBody{
			Type:      "init_ok",
			InReplyTo: msgID,
		},
	}
}
