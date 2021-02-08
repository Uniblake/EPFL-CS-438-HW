package client

import "go.dedis.ch/cs438/hw3/gossip/types"

const DefaultUIPort = "8080" // Port number for exchanging messages with the user interface

type ClientMessage struct {
	Contents    string `json:"contents"`
	Destination string `json:"destination"`
	Share       string `json:"share"`
	FileName    string `json:"filename"`
	Request     string `json:"request"`
	Keywords    string `json:"keywords"`
	Budget      string `json:"budget"`
}

// ChainResp is used to transmit GetBlocks() responses
type ChainResp struct {
	Last  string
	Chain map[string]types.Block
}
