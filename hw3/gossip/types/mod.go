// Package types contains the type messages for Paxos and TLC. We use a separate
// package to avoid import cycles.
package types

import (
	"crypto/sha256"
	"encoding/hex"
)

// Paxos messages. Feel free to move that in a separate file and/or package.

// ExtraMessage is carried by a rumor message.
type ExtraMessage struct {
	PaxosPrepare *PaxosPrepare
	PaxosPromise *PaxosPromise
	PaxosPropose *PaxosPropose
	PaxosAccept  *PaxosAccept
	TLC          *TLC
}

// Copy performs a deep copy of extra message
func (e *ExtraMessage) Copy() *ExtraMessage {
	var paxosPrepare *PaxosPrepare
	var paxosPromise *PaxosPromise
	var paxosPropose *PaxosPropose
	var paxosAccept *PaxosAccept
	var tlc *TLC

	if e.PaxosPrepare != nil {
		paxosPrepare = new(PaxosPrepare)
		paxosPrepare.PaxosSeqID = e.PaxosPrepare.PaxosSeqID
		paxosPrepare.ID = e.PaxosPrepare.ID
	}

	if e.PaxosPromise != nil {
		paxosPromise = new(PaxosPromise)
		paxosPromise.PaxosSeqID = e.PaxosPromise.PaxosSeqID
		paxosPromise.IDp = e.PaxosPromise.IDp
		paxosPromise.IDa = e.PaxosPromise.IDa
		paxosPromise.Value = *(e.PaxosPromise.Value.Copy())
	}

	if e.PaxosPropose != nil {
		paxosPropose = new(PaxosPropose)
		paxosPropose.PaxosSeqID = e.PaxosPropose.PaxosSeqID
		paxosPropose.ID = e.PaxosPropose.ID
		paxosPropose.Value = *(e.PaxosPropose.Value.Copy())
	}

	if e.PaxosAccept != nil {
		paxosAccept = new(PaxosAccept)
		paxosAccept.PaxosSeqID = e.PaxosAccept.PaxosSeqID
		paxosAccept.ID = e.PaxosAccept.ID
		paxosAccept.Value = *(e.PaxosAccept.Value.Copy())
	}

	if e.TLC != nil {
		tlc = new(TLC)
		tlc.Block = *e.TLC.Block.Copy()
	}

	return &ExtraMessage{
		PaxosPrepare: paxosPrepare,
		PaxosPromise: paxosPromise,
		PaxosPropose: paxosPropose,
		PaxosAccept:  paxosAccept,
		TLC:          tlc,
	}
}

// PaxosPrepare describes a PREPARE request to an acceptor.
type PaxosPrepare struct {
	PaxosSeqID int
	ID         int
}

// PaxosPromise describes a PROMISE request made by an acceptor to a proposer.
// IDp is the ID the proposer sent. IDa is the highest ID the acceptor saw and
// Value is the value it commits to, if any.
// Value/ID
type PaxosPromise struct {
	PaxosSeqID int
	IDp        int

	IDa   int
	Value Block
}

// PaxosPropose describes a PROPOSE request made by a proposer to an ACCEPTOR.
type PaxosPropose struct {
	PaxosSeqID int
	ID         int

	Value Block
}

// PaxosAccept describes an ACCEPT request that is sent by an acceptor to its
// proposer and all the learners.
type PaxosAccept struct {
	PaxosSeqID int
	ID         int

	Value Block
}

// TLC is the message sent by a node when it knows concensus has been reached
// for that block.
type TLC struct {
	Block Block
}

// Blockchain data structures. Feel free to move that in a separate file and/or
// package.

// Block describes the content of a block in the blockchain.
type Block struct {
	BlockNumber  int // not included in the hash
	PreviousHash []byte

	Metahash []byte
	Filename string
}

// Hash returns the hash of a block. It doesn't take the index.
func (b Block) Hash() []byte {
	h := sha256.New()

	h.Write(b.PreviousHash)

	h.Write(b.Metahash)
	h.Write([]byte(b.Filename))

	return h.Sum(nil)
}

// Copy performs a deep copy of a block
func (b Block) Copy() *Block {
	return &Block{
		BlockNumber:  b.BlockNumber,
		PreviousHash: append([]byte{}, b.PreviousHash...),

		Metahash: append([]byte{}, b.Metahash...),
		Filename: b.Filename,
	}
}

// Equal return if two blocks are equal
func (b *Block) Equal(ib *Block) bool {
	en := b.BlockNumber == ib.BlockNumber
	eh := hex.EncodeToString(b.Hash()) == hex.EncodeToString(ib.Hash())
	return en && eh
}
