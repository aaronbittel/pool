package node

import (
	"bufio"
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
	InReplyTo int    `json:"in_reply_to",omitzero`
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
	InitNode(encoder *json.Encoder, messages chan Msg)
	Step(msg Msg, encoder *json.Encoder) error
}

func MainLoop(node Node) {
	var (
		messages      = make(chan Msg)
		stdoutEncoder = json.NewEncoder(os.Stdout)
	)

	go readMessagesFromStdin(messages)

	node.InitNode(stdoutEncoder, messages)

	for msg := range messages {
		if err := node.Step(msg, stdoutEncoder); err != nil {
			panic(err)
		}
	}
}

func readMessagesFromStdin(messages chan Msg) {
	// each message will be on a seperate line
	scanner := bufio.NewScanner(os.Stdin)

	var msg Msg
	for scanner.Scan() {
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			fmt.Fprintf(os.Stderr, "could not unmarshal stdin into msg: %v", err)
			os.Exit(1)
		}
		messages <- msg
	}
}

func ReplayToInit(msg Msg, msgID, replyMsgID int, encoder *json.Encoder) error {
	rawInitOK, err := json.Marshal(NewInitOKBody(msgID, replyMsgID))
	if err != nil {
		return fmt.Errorf("could not marshal initOkBody: %v", err)
	}
	if err := encoder.Encode(NewReply(msg, rawInitOK)); err != nil {
		return fmt.Errorf("could not encode init replay: %v", err)
	}
	return nil
}

func NewInitOKBody(msgID, replyMsgID int) InitOKBody {
	return InitOKBody{
		MsgBody: MsgBody{
			Type:      "init_ok",
			InReplyTo: replyMsgID,
			ID:        msgID,
		},
	}
}

func NewOkReply(msg Msg, msgID, replyMsgID int, typ string) (Msg, error) {
	body := MsgBody{
		Type:      typ,
		InReplyTo: replyMsgID,
		ID:        msgID,
	}

	rawResp, err := json.Marshal(body)
	if err != nil {
		return Msg{}, err
	}

	return Msg{
		Src:     msg.Dst,
		Dst:     msg.Src,
		RawBody: rawResp,
	}, nil
}
