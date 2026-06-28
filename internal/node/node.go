package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

type MessageKind int

const (
	KindMessage MessageKind = iota
	KindInjected
)

type InjectedMessage interface {
	IsInjected()
}

type Event struct {
	Kind     MessageKind
	Msg      *Msg
	Injected InjectedMessage
}

type Msg struct {
	Src string `json:"src"`
	Dst string `json:"dest"`
	// Extracted fields type, msg_id and in_reply_to. Those are also present in the
	// RawBody
	MsgBody `json:"-"`
	// Complete Raw body of the JSOn
	RawBody json.RawMessage `json:"body"`
}

func (m *Msg) UnmarshalJSON(b []byte) error {
	var aux struct {
		Src     string          `json:"src"`
		Dst     string          `json:"dest"`
		BodyRaw json.RawMessage `json:"body"`
	}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	m.Src = aux.Src
	m.Dst = aux.Dst
	m.RawBody = aux.BodyRaw

	if err := json.Unmarshal(m.RawBody, &m.MsgBody); err != nil {
		return err
	}

	return nil
}

func (m *Msg) MarshalBody(payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	m.RawBody = raw
	return nil
}

func (m Msg) IntoReply(id *int) *Msg {
	msgID := *id
	*id += 1

	return &Msg{
		Src: m.Dst,
		Dst: m.Src,
		MsgBody: MsgBody{
			ID:        msgID,
			InReplyTo: m.ID,
			Type:      fmt.Sprintf("%s_ok", m.Type),
		},
	}
}

type MsgBody struct {
	Type      string `json:"type"`
	ID        int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to",omitzero`
}

type InitBody struct {
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type InitOKBody struct {
	MsgBody
}

type Node interface {
	InitNode(events chan Event)
	Step(event Event, encoder *json.Encoder) error
}

func (m *Msg) Send(encoder *json.Encoder) error {
	if err := encoder.Encode(m); err != nil {
		return fmt.Errorf("could not encode echo replay: %v", err)
	}
	return nil
}

func MainLoop(node Node) {
	var (
		events        = make(chan Event)
		stdoutEncoder = json.NewEncoder(os.Stdout)
	)

	go readMessagesFromStdin(events)

	node.InitNode(events)

	for event := range events {
		if err := node.Step(event, stdoutEncoder); err != nil {
			panic(err)
		}
	}
}

func readMessagesFromStdin(events chan Event) {
	// each message will be on a seperate line
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		var msg Msg
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			fmt.Fprintf(os.Stderr, "could not unmarshal stdin into msg: %v", err)
			os.Exit(1)
		}
		events <- Event{Kind: KindMessage, Msg: &msg}
	}
}

func ReplayToInit(msg *Msg, msgID, replyMsgID int, encoder *json.Encoder) error {
	rawInitOK, err := json.Marshal(NewInitOKBody(msgID, replyMsgID))
	if err != nil {
		return fmt.Errorf("could not marshal initOkBody: %v", err)
	}
	reply := Msg{
		Src:     msg.Dst,
		Dst:     msg.Src,
		RawBody: rawInitOK,
	}
	if err := encoder.Encode(reply); err != nil {
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

func NewOkReply(msg *Msg, msgID, replyMsgID int, typ string) (Msg, error) {
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
