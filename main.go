package main

import (
	"encoding/json"
	"os"
)

type Header struct {
	Src string `json:"src"`
	Dst string `json:"dest"`
}

type InitPayload struct {
	Type    string   `json:"type"`
	MsgID   int      `json:"msg_id"`
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type Init struct {
	Header
	Body InitPayload `json:"body"`
}

type InitOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

type InitOk struct {
	Header
	Body InitOKBody `json:"body"`
}

type Echo struct {
	Header
	Body EchoBody `json:"body"`
}

type EchoBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Echo  string `json:"echo"`
}

type EchoOK struct {
	Header
	Body EchoOKBody `json:"body"`
}

type EchoOKBody struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to"`
	Echo      string `json:"echo"`
}

func main() {
	var (
		stdinDecoder  = json.NewDecoder(os.Stdin)
		stdoutEncoder = json.NewEncoder(os.Stdout)

		init Init
		echo Echo
	)

	if err := stdinDecoder.Decode(&init); err != nil {
		panic(err)
	}

	initOK := InitOk{
		Header: Header{
			Src: init.Dst,
			Dst: init.Src,
		},
		Body: InitOKBody{
			Type:      "init_ok",
			InReplyTo: init.Body.MsgID,
		},
	}

	if err := stdoutEncoder.Encode(&initOK); err != nil {
		panic(err)
	}

	for {
		if err := stdinDecoder.Decode(&echo); err != nil {
			panic(err)
		}

		echoOK := EchoOK{
			Header: Header{
				Src: echo.Dst,
				Dst: echo.Src,
			},
			Body: EchoOKBody{
				Type:      "echo_ok",
				MsgID:     echo.Body.MsgID,
				InReplyTo: echo.Body.MsgID,
				Echo:      echo.Body.Echo,
			},
		}

		if err := stdoutEncoder.Encode(&echoOK); err != nil {
			panic(err)
		}
	}
}
