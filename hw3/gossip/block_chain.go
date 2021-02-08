package gossip

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/types"
)

// Blockchain data structures. Feel free to move that in a separate file and/or
// package.

// BlockChain describes a chain of blocks.
type BlockChain struct {
	sync.RWMutex
	Chain []*types.Block
}

// CheckMappingConflict .
func (b *BlockChain) CheckMappingConflict(metaHash []byte, fileName string) bool {
	b.RLock()
	defer b.RUnlock()
	for _, each := range b.Chain {
		if each.Filename == fileName {
			return true
		} else if hex.EncodeToString(each.Metahash) == hex.EncodeToString(metaHash) {
			return true
		}
	}
	return false
}

// GetLen return the length of a block chain
func (b *BlockChain) GetLen() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.Chain)
}

// Append a block to the tail of the chain
func (b *BlockChain) Append(t *types.Block) {
	b.Lock()
	defer b.Unlock()
	b.Chain = append(b.Chain, t)
}

// GetPreviousHash return the previous hash of the block chain
func (b *BlockChain) GetPreviousHash() []byte {
	b.RLock()
	defer b.RUnlock()
	if len(b.Chain) == 0 {
		return make([]byte, 32)
	}
	return b.Chain[len(b.Chain)-1].Hash()
}

// GetChain return the current block chain
func (b *BlockChain) GetChain() (string, map[string]types.Block) {
	b.RLock()
	defer b.RUnlock()
	var tailHash string
	m := make(map[string]types.Block)
	length := len(b.Chain)
	for id, each := range b.Chain {
		hashStr := hex.EncodeToString(each.Hash())
		m[hashStr] = *(each.Copy())
		if id == length-1 {
			tailHash = hashStr
		}
	}
	return tailHash, m
}

// ================= utils for gossiper =====================

// InitPaxosHelper return a new Paxos helper
func (g *Gossiper) InitPaxosHelper(N, prt, nIdx int, chain *BlockChain) {
	g.Paxos = NewPaxosHelper(N, prt, nIdx, chain)
}

// ConstructExtraRumor return a rumor in hw3
func (g *Gossiper) ConstructExtraRumor(ext *types.ExtraMessage) *RumorMessage {
	g.IDMutex.Lock()
	defer g.IDMutex.Unlock()
	g.NextSendID++
	return &RumorMessage{
		Origin: g.Name,
		ID:     g.NextSendID - 1,
		Extra:  ext,
	}
}

// ProposeUntilConsensus try to set a global name on a file
func (g *Gossiper) ProposeUntilConsensus(metaHash []byte, fileName string) bool {
	// var success bool = false
	var proposal *types.Block = nil
	// for !success {
	for {
		if g.Paxos.hasStop {
			return false
		}
		myBlock := &types.Block{
			BlockNumber:  g.Paxos.RoundID,
			PreviousHash: g.Paxos.Blocks.GetPreviousHash(),
			Metahash:     metaHash,
			Filename:     fileName,
		}
		if g.Paxos.Blocks.CheckMappingConflict(metaHash, fileName) {
			return false
		}
		proposal = g.AddPaxosPrepare(myBlock)
		if proposal != nil {
			if proposal.BlockNumber == -1 {
				return false
			} else if proposal.Equal(myBlock) {
				// start send my proposal
				success := g.AddPaxosPropose(proposal)
				if success == 1 {
					// reach consensus on myself
					return true
				} else if success == -1 {
					// gossiper stop triggered
					return false
				}
			} else {
				// start send other's proposal
				g.AddPaxosPropose(proposal)
			}
		}
	}
}

// AskForLocalPromise ask local acceptor to promise the local prepare
func (g *Gossiper) AskForLocalPromise(prep *types.PaxosPrepare) {
	g.Paxos.AccHelper.Lock()
	g.Paxos.ProHelper.Lock()
	defer g.Paxos.AccHelper.Unlock()
	defer g.Paxos.ProHelper.Unlock()
	if g.Paxos.RoundID == prep.PaxosSeqID {
		if g.Paxos.AccHelper.MaxIDp < prep.ID {
			// local acceptor promise
			g.Paxos.AccHelper.MaxIDp = prep.ID
			// local proposer ack
			g.Paxos.ProHelper.ConsentMap[g.Name] = true
		}
	}
	return
}

// AskForLocalAccept ask local acceptor to accept the local proposal
func (g *Gossiper) AskForLocalAccept(prop *types.PaxosPropose) {
	g.Paxos.AccHelper.Lock()
	defer g.Paxos.AccHelper.Unlock()
	if g.Paxos.AccHelper.Value == nil {
		if g.Paxos.RoundID == prop.PaxosSeqID {
			if g.Paxos.AccHelper.MaxIDp <= prop.ID {
				// local acceptor accept
				g.Paxos.AccHelper.MaxIDp = prop.ID
				g.Paxos.AccHelper.Value = prop.Value.Copy()
				g.Paxos.AccHelper.AcceptStatus[prop.ID] = 1
				g.Paxos.AccHelper.ObsvBlock[prop.ID] = prop.Value.Copy()
				// mongering local accept to other node
				accept := &types.PaxosAccept{
					PaxosSeqID: prop.PaxosSeqID,
					ID:         prop.ID,
					Value:      *(prop.Value.Copy()),
				}
				ext := &types.ExtraMessage{PaxosAccept: accept}
				rumor := g.ConstructExtraRumor(ext)
				// g.BroadcastRumorForPaxos(rumor)
				g.BroadcastRumorForPaxos(rumor)
			}
		}
	}
}

// AddPaxosPrepare start phase 1 in a Paxos box
// return the value to propose in phase 2
func (g *Gossiper) AddPaxosPrepare(v *types.Block) *types.Block {
	timer := time.NewTicker(time.Duration(g.Paxos.PReTry) * time.Second)
	for {
		// clear consent map
		g.Paxos.ClearConsentMap()
		prep := &types.PaxosPrepare{
			PaxosSeqID: g.Paxos.RoundID,
			ID:         g.Paxos.ProHelper.IDHelper.GetCurrent(),
		}
		ext := &types.ExtraMessage{PaxosPrepare: prep}
		// fmt.Println("AP 1")
		// add this propose to current propose
		g.Paxos.RegisterLocalPropose(prep.PaxosSeqID, prep.ID, *(v.Copy()))
		// try to send and get promise from local acceptor
		g.AskForLocalPromise(prep)
		// fmt.Println("AP 2")
		// go g.StartRumorMongering(g.ConstructExtraRumor(ext), nil)
		rumor := g.ConstructExtraRumor(ext)
		g.BroadcastRumorForPaxos(rumor)
		// g.BroadcastRumorForPaxos(rumor)
		fmt.Printf("=> %s prepare broadcast on seqID: %v, ID: %v.\n", g.Name, prep.PaxosSeqID, prep.ID)
		select {
		case <-g.Paxos.stopSync:
			return &types.Block{BlockNumber: -1}
		case <-timer.C:
			// fmt.Println("AP 3")
			g.Paxos.ProHelper.IDHelper.GetNext()
			fmt.Printf("=> %s prepare waiting time up, try seqID: %v, ID: %v.\n", g.Name, prep.PaxosSeqID, prep.ID)
		case promise := <-g.Paxos.ProHelper.ch:
			// fmt.Println("AP 4")
			// start phase 2 when over half nodes promise my proposal
			// if promise.IDp == g.Paxos.ProHelper.IDHelper.GetCurrent() {
			if promise.Value.Equal(&types.Block{}) {
				fmt.Printf("=> %s prepare success, finish prepare on seqID: %v, ID: %v.\n", g.Name, prep.PaxosSeqID, prep.ID)
				return v
			}
			fmt.Printf("=> %s prepare fail, help prepare on seqID: %v, ID: %v.\n", g.Name, prep.PaxosSeqID, prep.ID)
			// start phase 2 when noticed with an accepted one
			return &(promise.Value)
		case <-g.Paxos.ProHelper.consAck:
			// consensus is reached among everyone, should directly stop and try the next Paxos box
			// reinit Paxos box
			g.Paxos.ForwardToNextBlock()
			fmt.Printf("%s prepare fail, notice consensus, block len: %v\n", g.Name, g.Paxos.Blocks.GetLen())
			return nil
		}
	}
}

// AddPaxosPropose start phase 2 in a Paxos box
// return true if phase 2 consensus on itself, else false
func (g *Gossiper) AddPaxosPropose(v *types.Block) int {
	timer := time.NewTimer(time.Duration(g.Paxos.PReTry) * time.Second)
	prop := &types.PaxosPropose{
		PaxosSeqID: g.Paxos.RoundID,
		ID:         g.Paxos.ProHelper.IDHelper.GetCurrent(),
		Value:      *v,
	}
	ext := &types.ExtraMessage{PaxosPropose: prop}
	g.AskForLocalAccept(prop)
	rumor := g.ConstructExtraRumor(ext)
	g.BroadcastRumorForPaxos(rumor)
	// go g.StartRumorMongering(g.ConstructExtraRumor(ext), nil)
	fmt.Printf("=> %s propose broadcast on seqID: %v, ID: %v.\n", g.Name, prop.PaxosSeqID, prop.ID)
	select {
	case <-g.Paxos.stopSync:
		return -1
	case <-timer.C:
		g.Paxos.ProHelper.IDHelper.GetNext()
		fmt.Printf("=> %s propose waiting time up, try seqID: %v, ID: %v.\n", g.Name, g.Paxos.RoundID, g.Paxos.ProHelper.IDHelper.GetCurrent())
		return 0
	case consBlock := <-g.Paxos.ProHelper.consAck:
		if consBlock.Equal(v) {
			g.Paxos.ForwardToNextBlock()
			fmt.Printf("%s propose success, notice consensus on %s, block len: %v\n", g.Name, consBlock.Filename, g.Paxos.Blocks.GetLen())
			return 1
		}
		fmt.Printf("%s propose fail, notice consensus on %s, block len: %v\n", g.Name, consBlock.Filename, g.Paxos.Blocks.GetLen())
		g.Paxos.ForwardToNextBlock()
		return 0
	}
}

// ================= utils for paxos =====================

// PaxosHelper describes paxos roles.
type PaxosHelper struct {
	N         int
	PReTry    int
	AccHelper *Acceptor
	ProHelper *Proposer
	Blocks    *BlockChain
	RoundID   int
	stopSync  chan int
	hasStop   bool
}

// Acceptor describes Acceptor
type Acceptor struct {
	sync.RWMutex
	MaxIDp       int
	Value        *types.Block
	AcceptStatus map[int]int // ID ---> Num of Accept nodes
	ObsvBlock    map[int]*types.Block
	ObsvTLC      map[string]int // block.hash ---> num of tlc
}

// Proposer describes Proposer
type Proposer struct {
	sync.RWMutex
	ConsentMap     map[string]bool // origin ---> false(deny)/true(promise)
	CurrentPropose *types.PaxosPropose
	IDHelper       *uniqIDGen
	ch             chan *types.PaxosPromise
	consAck        chan *types.Block
}

// NewPaxosHelper return a PaxosHelper
func NewPaxosHelper(N, prt, nIdx int, exc *BlockChain) *PaxosHelper {
	var chain *BlockChain
	var rid int
	acc := &Acceptor{
		MaxIDp:       -1,
		AcceptStatus: make(map[int]int),
		ObsvBlock:    make(map[int]*types.Block),
		ObsvTLC:      make(map[string]int),
	}
	pro := &Proposer{
		ConsentMap: make(map[string]bool),
		IDHelper:   newSeqGen(nIdx, N),
		ch:         make(chan *types.PaxosPromise),
		consAck:    make(chan *types.Block),
	}
	if exc == nil {
		chain = &BlockChain{Chain: make([]*types.Block, 0)}
		rid = 0
	} else {
		chain = exc
		rid = len(chain.Chain)
	}
	return &PaxosHelper{
		N:         N,
		PReTry:    prt,
		AccHelper: acc,
		ProHelper: pro,
		Blocks:    chain,
		RoundID:   rid,
		stopSync:  make(chan int),
		hasStop:   false,
	}
}

// ForwardToNextBlock the paxos helper, reset stored flags in last round
func (p *PaxosHelper) StopPaxos() {
	close(p.stopSync)
	p.hasStop = true
}

// ForwardToNextBlock the paxos helper, reset stored flags in last round
func (p *PaxosHelper) ForwardToNextBlock() {
	p.AccHelper.Lock()
	p.ProHelper.Lock()
	defer p.AccHelper.Unlock()
	defer p.ProHelper.Unlock()
	p.AccHelper.MaxIDp = -1
	p.AccHelper.Value = nil
	p.AccHelper.AcceptStatus = make(map[int]int)
	p.AccHelper.ObsvBlock = make(map[int]*types.Block)
	p.AccHelper.ObsvTLC = make(map[string]int)

	p.ProHelper.ConsentMap = make(map[string]bool)
	p.ProHelper.CurrentPropose = nil
	p.ProHelper.IDHelper = newSeqGen(p.ProHelper.IDHelper.currentID%p.ProHelper.IDHelper.total, p.ProHelper.IDHelper.total)
	p.ProHelper.ch = make(chan *types.PaxosPromise)
	p.ProHelper.consAck = make(chan *types.Block)

	p.RoundID = p.Blocks.GetLen()
	p.stopSync = make(chan int)
	p.hasStop = false
}

// ClearConsentMap .
func (p *PaxosHelper) ClearConsentMap() {
	p.ProHelper.Lock()
	defer p.ProHelper.Unlock()
	p.ProHelper.ConsentMap = make(map[string]bool)
}

// RegisterLocalPropose add a prepare / propose message to p.ProHelper.CurrentPropose
func (p *PaxosHelper) RegisterLocalPropose(seqID int, ID int, value types.Block) {
	p.ProHelper.Lock()
	defer p.ProHelper.Unlock()
	p.ProHelper.CurrentPropose = &types.PaxosPropose{
		PaxosSeqID: seqID,
		ID:         ID,
		Value:      value,
	}
	return
}

// TryAdmitPromise check if this is for me, and update according to it.
// (for me, above theshold)
// return true if for me, else return false
// return false if below N/2+1, else return true
func (p *PaxosHelper) TryAdmitPromise(msg *types.PaxosPromise, origin string, name string) (bool, bool) {
	// defer func() {
	// 	fmt.Println("safe return")
	// }()
	p.ProHelper.Lock()
	defer p.ProHelper.Unlock()
	// check if this is for me
	if p.ProHelper.CurrentPropose != nil {
		if p.ProHelper.CurrentPropose.ID == msg.IDp && msg.PaxosSeqID == p.RoundID {
			if msg.Value.Equal(&types.Block{}) { // empty accepted block
				p.ProHelper.ConsentMap[origin] = true
				if len(p.ProHelper.ConsentMap) >= int(p.N/2+1) {
					fmt.Printf("%s collect promise number enough: %v\n", name, len(p.ProHelper.ConsentMap))
					select {
					case p.ProHelper.ch <- msg:
						return true, true
					default:
						fmt.Println("warning, trigger old prepare finish")
						return true, true
					}
				}
				fmt.Printf("%s collect promise number: %v\n", name, len(p.ProHelper.ConsentMap))
			} else {
				select {
				case p.ProHelper.ch <- msg:
					return true, false
				default:
					return true, false
				}
			}
			return true, len(p.ProHelper.ConsentMap) >= int(p.N/2+1)
		}
	}
	return false, false
}

// TryAcceptPrepare compare the id and try to promise
func (p *PaxosHelper) TryAcceptPrepare(msg *types.PaxosPrepare, name string) (bool, *types.ExtraMessage) {
	p.AccHelper.Lock()
	defer p.AccHelper.Unlock()
	if p.RoundID != msg.PaxosSeqID {
		return false, nil
	}
	if p.AccHelper.Value == nil {
		if p.AccHelper.MaxIDp < msg.ID {
			p.AccHelper.MaxIDp = msg.ID
			promise := &types.PaxosPromise{
				PaxosSeqID: p.RoundID,
				IDp:        msg.ID,
				IDa:        -1,
				Value:      types.Block{},
			}
			fmt.Printf("%s give promise to SeqID: %v, ID: %v\n", name, p.RoundID, msg.ID)
			return true, &types.ExtraMessage{PaxosPromise: promise}
		}
	} else {
		promise := &types.PaxosPromise{
			PaxosSeqID: p.RoundID,
			IDp:        msg.ID,
			IDa:        p.AccHelper.MaxIDp,
			Value:      *(p.AccHelper.Value.Copy()),
		}
		fmt.Printf("%s Reject promise in SeqID: %v, <ID: %v> with <ID: %v>\n", name, p.RoundID, msg.ID, p.AccHelper.MaxIDp)
		return false, &types.ExtraMessage{PaxosPromise: promise}
	}
	return false, nil
}

// TryAcceptPropose try to accpet a proposal
func (p *PaxosHelper) TryAcceptPropose(msg *types.PaxosPropose, name string) (bool, *types.ExtraMessage) {
	p.AccHelper.Lock()
	defer p.AccHelper.Unlock()
	if p.AccHelper.Value == nil {
		if p.AccHelper.MaxIDp <= msg.ID && msg.PaxosSeqID == p.RoundID {
			fmt.Printf("%s give accept to SeqID: %v, ID: %v, file: %s\n", name, p.RoundID, msg.ID, msg.Value.Filename)
			p.AccHelper.MaxIDp = msg.ID
			p.AccHelper.Value = &msg.Value
			accept := &types.PaxosAccept{
				PaxosSeqID: p.RoundID,
				ID:         msg.ID,
				Value:      msg.Value,
			}
			return true, &types.ExtraMessage{PaxosAccept: accept}
		}
	}
	return false, nil
}

// TryAdmitAccept try to admit an accept
// return if one reach N/2+1 and the ext if not nil
func (p *PaxosHelper) TryAdmitAccept(msg *types.PaxosAccept, name string) (bool, *types.ExtraMessage) {
	p.AccHelper.Lock()
	defer p.AccHelper.Unlock()
	if msg.PaxosSeqID == p.RoundID {
		// if this is correct for this round, record this accept
		abvThr, ext := p.RecordAcceptAnounce(msg.ID, &(msg.Value), name)
		// check if this is for me, if is, trigger something
		return abvThr, ext
	}
	return false, nil
}

// RecordAcceptAnounce increase the num of a given ID, return if one reach N/2+1
func (p *PaxosHelper) RecordAcceptAnounce(ID int, block *types.Block, name string) (bool, *types.ExtraMessage) {
	if _, ok := p.AccHelper.AcceptStatus[ID]; ok {
		p.AccHelper.AcceptStatus[ID]++
	} else {
		p.AccHelper.AcceptStatus[ID] = 1
		p.AccHelper.ObsvBlock[ID] = block.Copy()
	}
	readyForTLC := p.AccHelper.AcceptStatus[ID] >= int(p.N/2+1)
	alreadySendTLC := p.AccHelper.AcceptStatus[ID] > int(p.N/2+1)
	if readyForTLC && !alreadySendTLC {
		fmt.Printf("%s observe accept <num: %v> to SeqID: %v, file: %s, send TLC\n", name, p.AccHelper.AcceptStatus[ID], p.RoundID, block.Filename)
		tlc := &types.TLC{Block: *block}
		return true, &types.ExtraMessage{TLC: tlc}
	}
	fmt.Printf("%s observe accept <num: %v> to SeqID: %v, file: %s\n", name, p.AccHelper.AcceptStatus[ID], p.RoundID, block.Filename)
	return false, nil
}

// TryAcceptTLC register a new TLC, return the num of this TLC
func (p *PaxosHelper) TryAcceptTLC(msg *types.TLC, name string) (bool, int) {
	p.AccHelper.Lock()
	defer p.AccHelper.Unlock()
	if p.RoundID != msg.Block.BlockNumber {
		return false, 0
	}
	for hash := range p.AccHelper.ObsvTLC {
		if hash == hex.EncodeToString(msg.Block.Hash()) {
			p.AccHelper.ObsvTLC[hash]++
			fmt.Printf("%s observe TLC <num: %v> to SeqID: %v, file: %s\n", name, p.AccHelper.ObsvTLC[hash], p.RoundID, msg.Block.Filename)
			return false, p.AccHelper.ObsvTLC[hash]
		}
	}
	// if !exist
	p.AccHelper.ObsvTLC[hex.EncodeToString(msg.Block.Hash())] = 1
	fmt.Printf("%s observe TLC <num: %v> to SeqID: %v, file: %s\n", name, p.AccHelper.ObsvTLC[hex.EncodeToString(msg.Block.Hash())], p.RoundID, msg.Block.Filename)
	return true, 1
}

// CheckConsensusStatus check if a consensus is reached on current block
func (g *Gossiper) CheckConsensusStatus(msg *types.TLC) {
	g.Paxos.Blocks.Append(&(msg.Block))
	newFile := &File{
		Name:     msg.Block.Filename,
		MetaHash: hex.EncodeToString(msg.Block.Metahash),
	}
	g.IndexedFiles.Update(newFile)
	// send the consensus to the probable local propose goroutine
	if g.Paxos.ProHelper.CurrentPropose != nil {
		fmt.Printf("%s consensus notice when participate paxos on Block Number: %v, file: %s\n", g.Name, msg.Block.BlockNumber, msg.Block.Filename)
		g.Paxos.ProHelper.consAck <- &msg.Block
	} else {
		fmt.Printf("%s consensus notice wait paxos on Block Number: %v, file: %s\n", g.Name, msg.Block.BlockNumber, msg.Block.Filename)
		close(g.Paxos.ProHelper.consAck)
		g.Paxos.ForwardToNextBlock()
	}
}
