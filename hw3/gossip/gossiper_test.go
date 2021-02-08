package gossip

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/cs438/hw3/gossip/types"
)

var factory = GetFactory()

// The subfolders of the temporary folder that stores files
const (
	sharedDataFolder = "shared"
	downloadFolder   = "download"
	chunkSize        = 8192
)

// Test 1
// Node A proposes a file, we check that the other nodes got the PREPARE message
// Node A got the PROMISE messages back.
func TestGossiper_No_Contention_Single_Propose(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	inA := nA.getIns(ctx)
	inB := nB.getIns(ctx)
	inC := nC.getIns(ctx)
	inD := nD.getIns(ctx)
	inE := nE.getIns(ctx)

	nA.addFile(t, "test1.txt", []byte{0xAA})
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 1)

	getPrepare := func(h *history) int {
		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosPrepare != nil {

				return 1
			}
		}

		return 0
	}

	getPromise := func(h *history) []*types.PaxosPromise {
		res := []*types.PaxosPromise{}

		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosPromise != nil {

				res = append(res, msg.message.Rumor.Extra.PaxosPromise)
			}
		}

		return res
	}

	// A should receive at least 3 Promise messages
	promises := getPromise(inA)
	require.True(t, len(promises) >= 3)

	// At least 3 nodes should receive the Prepare message
	bReceived := getPrepare(inB)
	cReceived := getPrepare(inC)
	dReceived := getPrepare(inD)
	eReceived := getPrepare(inE)

	require.True(t, bReceived+cReceived+dReceived+eReceived >= 3)
}

// Test 2
// Node A proposes a file, we check that the other nodes got the PROPOSE message
// Node receive the ACCEPT messages back.
func TestGossiper_No_Contention_Single_Accept(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	inA := nA.getIns(ctx)
	inB := nB.getIns(ctx)
	inC := nC.getIns(ctx)
	inD := nD.getIns(ctx)
	inE := nE.getIns(ctx)

	nA.addFile(t, "test1.txt", []byte{0xAA})
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 1)

	getAccept := func(h *history) []*types.PaxosAccept {
		res := []*types.PaxosAccept{}

		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosAccept != nil {

				res = append(res, msg.message.Rumor.Extra.PaxosAccept)
			}
		}

		return res
	}

	getPropose := func(h *history) int {
		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosPropose != nil {

				return 1
			}
		}

		return 0
	}

	// A should receive at least 3 Accept messages
	accepts := getAccept(inA)
	require.True(t, len(accepts) >= 3)

	// At least 3 nodes should receive the Propose message
	bReceived := getPropose(inB)
	cReceived := getPropose(inC)
	dReceived := getPropose(inD)
	eReceived := getPropose(inE)

	require.True(t, bReceived+cReceived+dReceived+eReceived >= 3)
}

// Test 3
// Node A proposes a file, we check that at least 3 nodes should receive at
// least 3 ACCEPT messages.
func TestGossiper_No_Contention_Single_Consensus_Completion(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	inA := nA.getIns(ctx)
	inB := nB.getIns(ctx)
	inC := nC.getIns(ctx)
	inD := nD.getIns(ctx)
	inE := nE.getIns(ctx)

	nA.addFile(t, "test1.txt", []byte{0xAA})
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 2)

	getAccept := func(h *history) int {
		res := []*types.PaxosAccept{}

		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosAccept != nil {

				res = append(res, msg.message.Rumor.Extra.PaxosAccept)
			}
		}

		if len(res) >= 3 {
			return 1
		}

		return 0
	}

	aReceived := getAccept(inA)
	bReceived := getAccept(inB)
	cReceived := getAccept(inC)
	dReceived := getAccept(inD)
	eReceived := getAccept(inE)

	require.True(t, aReceived+bReceived+cReceived+dReceived+eReceived >= 3)
}

// Test 3 INTEGRATION
// Node A proposes a file, we check that at least 3 nodes should receive at
// least 3 ACCEPT messages.
func TestBinGossiper_No_Contention_Single_Consensus_Completion(t *testing.T) {
	nA := createAndStartBinNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	inA := nA.getIns(ctx)
	inB := nB.getIns(ctx)
	inC := nC.getIns(ctx)
	inD := nD.getIns(ctx)
	inE := nE.getIns(ctx)

	nA.addFile(t, "test1.txt", []byte{0xAA})
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 2)

	getAccept := func(h *history) int {
		res := []*types.PaxosAccept{}

		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosAccept != nil {

				res = append(res, msg.message.Rumor.Extra.PaxosAccept)
			}
		}

		if len(res) >= 3 {
			return 1
		}

		return 0
	}

	aReceived := getAccept(inA)
	bReceived := getAccept(inB)
	cReceived := getAccept(inC)
	dReceived := getAccept(inD)
	eReceived := getAccept(inE)

	require.True(t, aReceived+bReceived+cReceived+dReceived+eReceived >= 3)
}

// Test 4
// Node A proposes a file, node B, C, and D are down. We check that Node A
// retries after the "paxos retry" timeout.
func TestGossiper_No_Contention_Single_Retry(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithPaxosRetry(2), WithAntiEntropy(1))
	defer nA.stop()

	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	nB.stop()
	nC.stop()
	nD.stop()
	nE.stop()

	outA := nA.getOuts(ctx)

	nA.addFile(t, "test1.txt", []byte{0xAA})
	go nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 1)

	getPrepare := func(h *history) []*types.PaxosPrepare {
		res := []*types.PaxosPrepare{}

		h.Lock()
		defer h.Unlock()

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.PaxosPrepare != nil {

				res = append(res, msg.message.Rumor.Extra.PaxosPrepare)
			}
		}

		return res
	}

	aSent := getPrepare(outA)

	// Node A, as a proposer, should've gossiped at least one propose message
	require.True(t, len(aSent) >= 1)
	// all the PROPOSE message should have index 0
	for _, p := range aSent {
		require.True(t, p.ID == 0)
	}

	time.Sleep(time.Second * 3)

	// we expect that we find a PROPOSE message with ID = 0 + numNodes
	aSent = getPrepare(outA)
	ok := false
	for _, p := range aSent {
		if p.ID == 5 {
			ok = true
		}
	}

	require.True(t, ok)
}

// Test 8
// Node A proposes a name-metahash, we check that at least 3 nodes have the
// name-metahash as the first block or their chain.
func TestGossiper_No_Contention_Block_Consensus(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	content := []byte{0xAA}

	// Compute the metahash
	h := sha256.New()
	h.Write(content)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash := h.Sum(nil)

	nA.addFile(t, "test1.txt", content)
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 2)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	checkBlock := func(last string, chain map[string]types.Block) int {
		if len(chain) == 1 {
			block := chain[aLast]
			expected := types.Block{
				BlockNumber:  0,
				Filename:     "test1.txt",
				Metahash:     metaHash,
				PreviousHash: make([]byte, 32),
			}
			require.Equal(t, expected, block)

			return 1
		}

		return 0
	}

	aCheck := checkBlock(aLast, aChain)
	bCheck := checkBlock(bLast, bChain)
	cCheck := checkBlock(cLast, cChain)
	dCheck := checkBlock(dLast, dChain)
	eCheck := checkBlock(eLast, eChain)

	require.True(t, aCheck+bCheck+cCheck+dCheck+eCheck >= 3)
}

// Test 8 INTEGRATION
// Node A proposes a name-metahash, we check that at least 3 nodes have the
// name-metahash as the first block or their chain.
func TestBinGossiper_No_Contention_Block_Consensus(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartBinNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartBinNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartBinNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	content := []byte{0xAA}

	// Compute the metahash
	h := sha256.New()
	h.Write(content)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash := h.Sum(nil)

	nA.addFile(t, "test1.txt", content)
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 2)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	checkBlock := func(last string, chain map[string]types.Block) int {
		if len(chain) == 1 {
			block := chain[aLast]
			expected := types.Block{
				BlockNumber:  0,
				Filename:     "test1.txt",
				Metahash:     metaHash,
				PreviousHash: make([]byte, 32),
			}
			require.Equal(t, expected, block)

			return 1
		}

		return 0
	}

	aCheck := checkBlock(aLast, aChain)
	bCheck := checkBlock(bLast, bChain)
	cCheck := checkBlock(cLast, cChain)
	dCheck := checkBlock(dLast, dChain)
	eCheck := checkBlock(eLast, eChain)

	require.True(t, aCheck+bCheck+cCheck+dCheck+eCheck >= 3)
}

// Test 9
// Node A proposes a name-metahash, we check that at least 3 nodes received at
// least 3 TLC messages.
func TestGossiper_No_Contention_Block_TLC_Consensus(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inA := nA.getIns(ctx)
	inB := nB.getIns(ctx)
	inC := nC.getIns(ctx)
	inD := nD.getIns(ctx)
	inE := nE.getIns(ctx)

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	content := []byte{0xAA}

	// Compute the metahash
	h := sha256.New()
	h.Write(content)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash := h.Sum(nil)

	nA.addFile(t, "test1.txt", content)
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 2)

	expectedBlock := types.Block{
		BlockNumber:  0,
		Filename:     "test1.txt",
		Metahash:     metaHash,
		PreviousHash: make([]byte, 32),
	}

	getTLC := func(h *history) int {
		h.Lock()
		defer h.Unlock()

		count := 0

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.TLC != nil {

				require.Equal(t, expectedBlock, msg.message.Rumor.Extra.TLC.Block)
				count++
			}
		}

		if count >= 3 {
			return 1
		}

		return 0
	}

	aTLC := getTLC(inA)
	bTLC := getTLC(inB)
	cTLC := getTLC(inC)
	dTLC := getTLC(inD)
	eTLC := getTLC(inE)

	require.True(t, aTLC+bTLC+cTLC+dTLC+eTLC >= 3)
}

// Test 9 INTEGRATION
// Node A proposes a name-metahash, we check that at least 3 nodes received at
// least 3 TLC messages.
func TestBinGossiper_No_Contention_Block_TLC_Consensus(t *testing.T) {
	nA := createAndStartBinNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartBinNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartBinNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inA := nA.getIns(ctx)
	inB := nB.getIns(ctx)
	inC := nC.getIns(ctx)
	inD := nD.getIns(ctx)
	inE := nE.getIns(ctx)

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	content := []byte{0xAA}

	// Compute the metahash
	h := sha256.New()
	h.Write(content)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash := h.Sum(nil)

	nA.addFile(t, "test1.txt", content)
	nA.gossiper.IndexShares("test1.txt")

	time.Sleep(time.Second * 2)

	expectedBlock := types.Block{
		BlockNumber:  0,
		Filename:     "test1.txt",
		Metahash:     metaHash,
		PreviousHash: make([]byte, 32),
	}

	getTLC := func(h *history) int {
		h.Lock()
		defer h.Unlock()

		count := 0

		for _, msg := range h.p {
			if msg.message.Rumor != nil &&
				msg.message.Rumor.Extra != nil &&
				msg.message.Rumor.Extra.TLC != nil {

				require.Equal(t, expectedBlock, msg.message.Rumor.Extra.TLC.Block)
				count++
			}
		}

		if count >= 3 {
			return 1
		}

		return 0
	}

	aTLC := getTLC(inA)
	bTLC := getTLC(inB)
	cTLC := getTLC(inC)
	dTLC := getTLC(inD)
	eTLC := getTLC(inE)

	require.True(t, aTLC+bTLC+cTLC+dTLC+eTLC >= 3)
}

// Test 10
// Node A and B sends a proposal at the same time. We check that at least 3
// nodes agree on the same proposal.
func TestGossiper_Contention_Single_Block_Consensus(t *testing.T) {
	name1 := "test1.txt"
	name2 := "test2.txt"

	content1 := []byte{0xAA}
	content2 := []byte{0xBB}

	var metaHash1 []byte
	var metaHash2 []byte

	// Compute the metahash
	h := sha256.New()
	h.Write(content1)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash1 = h.Sum(nil)

	h = sha256.New()
	h.Write(content2)
	chunk = h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash2 = h.Sum(nil)

	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	nA.addFile(t, name1, content1)
	nB.addFile(t, name2, content2)

	go nA.gossiper.IndexShares(name1)
	go nB.gossiper.IndexShares(name2)

	time.Sleep(time.Second * 2)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	getFirst := func(last string, chain map[string]types.Block) (types.Block, bool) {
		if last == "" {
			return types.Block{}, false
		}

		for {
			block := chain[last]
			if block.BlockNumber == 0 {
				return block, true
			}

			last = hex.EncodeToString(block.PreviousHash)
		}
	}

	mergeChain := func(block types.Block, chainCount map[string]int, chainMerged map[string]types.Block) {
		key := hex.EncodeToString(block.Hash())

		_, ok := chainCount[key]
		if !ok {
			chainCount[key] = 0
		}

		chainCount[key]++
		chainMerged[key] = block
	}

	countBlocks := map[string]int{}
	mergedBlocks := map[string]types.Block{}

	aFirst, ok := getFirst(aLast, aChain)
	if ok {
		mergeChain(aFirst, countBlocks, mergedBlocks)
	}

	bFirst, ok := getFirst(bLast, bChain)
	if ok {
		mergeChain(bFirst, countBlocks, mergedBlocks)
	}

	cFirst, ok := getFirst(cLast, cChain)
	if ok {
		mergeChain(cFirst, countBlocks, mergedBlocks)
	}

	dFirst, ok := getFirst(dLast, dChain)
	if ok {
		mergeChain(dFirst, countBlocks, mergedBlocks)
	}

	eFirst, ok := getFirst(eLast, eChain)
	if ok {
		mergeChain(eFirst, countBlocks, mergedBlocks)
	}

	// There should be at least one block with a count of 3

	threeFound := false
	keyFound := ""
	for k, v := range countBlocks {
		if v >= 3 {
			threeFound = true
			keyFound = k
		}
	}

	require.True(t, threeFound)

	// We check that the block with at least three occurrences is the expected
	// one.

	block1 := types.Block{
		BlockNumber:  0,
		Filename:     name1,
		Metahash:     metaHash1,
		PreviousHash: make([]byte, 32),
	}

	block2 := types.Block{
		BlockNumber:  0,
		Filename:     name2,
		Metahash:     metaHash2,
		PreviousHash: make([]byte, 32),
	}

	allPotentialBlocks := []types.Block{
		block1,
		block2,
	}

	require.True(t, threeFound)
	require.Contains(t, allPotentialBlocks, mergedBlocks[keyFound])
}

// Test 10 INTEGRATION
// Node A and B sends a proposal at the same time. We check that at least 3
// nodes agree on the same proposal.
func TestBinGossiper_Contention_Single_Block_Consensus(t *testing.T) {
	name1 := "test1.txt"
	name2 := "test2.txt"

	content1 := []byte{0xAA}
	content2 := []byte{0xBB}

	var metaHash1 []byte
	var metaHash2 []byte

	// Compute the metahash
	h := sha256.New()
	h.Write(content1)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash1 = h.Sum(nil)

	h = sha256.New()
	h.Write(content2)
	chunk = h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash2 = h.Sum(nil)

	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartBinNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartBinNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartBinNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartBinNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	nA.addFile(t, name1, content1)
	nB.addFile(t, name2, content2)

	go nA.gossiper.IndexShares(name1)
	go nB.gossiper.IndexShares(name2)

	time.Sleep(time.Second * 2)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	getFirst := func(last string, chain map[string]types.Block) (types.Block, bool) {
		if last == "" {
			return types.Block{}, false
		}

		for {
			block := chain[last]
			if block.BlockNumber == 0 {
				return block, true
			}

			last = hex.EncodeToString(block.PreviousHash)
		}
	}

	mergeChain := func(block types.Block, chainCount map[string]int, chainMerged map[string]types.Block) {
		key := hex.EncodeToString(block.Hash())

		_, ok := chainCount[key]
		if !ok {
			chainCount[key] = 0
		}

		chainCount[key]++
		chainMerged[key] = block
	}

	countBlocks := map[string]int{}
	mergedBlocks := map[string]types.Block{}

	aFirst, ok := getFirst(aLast, aChain)
	if ok {
		mergeChain(aFirst, countBlocks, mergedBlocks)
	}

	bFirst, ok := getFirst(bLast, bChain)
	if ok {
		mergeChain(bFirst, countBlocks, mergedBlocks)
	}

	cFirst, ok := getFirst(cLast, cChain)
	if ok {
		mergeChain(cFirst, countBlocks, mergedBlocks)
	}

	dFirst, ok := getFirst(dLast, dChain)
	if ok {
		mergeChain(dFirst, countBlocks, mergedBlocks)
	}

	eFirst, ok := getFirst(eLast, eChain)
	if ok {
		mergeChain(eFirst, countBlocks, mergedBlocks)
	}

	// There should be at least one block with a count of 3

	threeFound := false
	keyFound := ""
	for k, v := range countBlocks {
		if v >= 3 {
			threeFound = true
			keyFound = k
		}
	}

	require.True(t, threeFound)

	// We check that the block with at least three occurrences is the expected
	// one.

	block1 := types.Block{
		BlockNumber:  0,
		Filename:     name1,
		Metahash:     metaHash1,
		PreviousHash: make([]byte, 32),
	}

	block2 := types.Block{
		BlockNumber:  0,
		Filename:     name2,
		Metahash:     metaHash2,
		PreviousHash: make([]byte, 32),
	}

	allPotentialBlocks := []types.Block{
		block1,
		block2,
	}

	require.True(t, threeFound)
	require.Contains(t, allPotentialBlocks, mergedBlocks[keyFound])
}

// Test 11
// Node A and B make a proposal. The proposal that is rejected for block 1
// should be accepted in block 2, with at least 3 nodes having that proposal.
func TestGossiper_Contention_TLC_Retry(t *testing.T) {
	name1 := "test1.txt"
	name2 := "test2.txt"

	content1 := []byte{0xAA}
	content2 := []byte{0xBB}

	var metaHash1 []byte
	var metaHash2 []byte

	// Compute the metahash
	h := sha256.New()
	h.Write(content1)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash1 = h.Sum(nil)

	h = sha256.New()
	h.Write(content2)
	chunk = h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash2 = h.Sum(nil)

	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	nA.addFile(t, name1, content1)
	nB.addFile(t, name2, content2)

	go nA.gossiper.IndexShares(name1)
	go nB.gossiper.IndexShares(name2)

	time.Sleep(time.Second * 10)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	getBlockByIndex := func(index int, last string, chain map[string]types.Block) (types.Block, bool) {
		if last == "" {
			return types.Block{}, false
		}

		for {
			block := chain[last]
			if block.BlockNumber == index {
				return block, true
			}

			// no block found
			if block.BlockNumber == 0 {
				return types.Block{}, false
			}

			last = hex.EncodeToString(block.PreviousHash)
		}
	}

	mergeChain := func(block types.Block, chainCount map[string]int, chainMerged map[string]types.Block) {
		key := hex.EncodeToString(block.Hash())

		_, ok := chainCount[key]
		if !ok {
			chainCount[key] = 0
		}

		chainCount[key]++
		chainMerged[key] = block
	}

	countBlocks := map[string]int{}
	mergedBlocks := map[string]types.Block{}

	aFirst, ok := getBlockByIndex(0, aLast, aChain)
	if ok {
		mergeChain(aFirst, countBlocks, mergedBlocks)
	}

	bFirst, ok := getBlockByIndex(0, bLast, bChain)
	if ok {
		mergeChain(bFirst, countBlocks, mergedBlocks)
	}

	cFirst, ok := getBlockByIndex(0, cLast, cChain)
	if ok {
		mergeChain(cFirst, countBlocks, mergedBlocks)
	}

	dFirst, ok := getBlockByIndex(0, dLast, dChain)
	if ok {
		mergeChain(dFirst, countBlocks, mergedBlocks)
	}

	eFirst, ok := getBlockByIndex(0, eLast, eChain)
	if ok {
		mergeChain(eFirst, countBlocks, mergedBlocks)
	}

	// There should be at least one block with a count of 3 for block 0

	threeFound := false
	keyFound := ""
	for k, v := range countBlocks {
		if v >= 3 {
			threeFound = true
			keyFound = k
		}
	}

	require.True(t, threeFound)

	// We check that the block with at least three occurrences is the expected
	// one.

	block1 := types.Block{
		BlockNumber:  0,
		Filename:     name1,
		Metahash:     metaHash1,
		PreviousHash: make([]byte, 32),
	}

	block2 := types.Block{
		BlockNumber:  0,
		Filename:     name2,
		Metahash:     metaHash2,
		PreviousHash: make([]byte, 32),
	}

	allPotentialBlocks := []types.Block{
		block1,
		block2,
	}

	firstBlock := mergedBlocks[keyFound]

	require.True(t, threeFound)
	require.Contains(t, allPotentialBlocks, firstBlock)

	// We do the same for the second block

	countBlocks = map[string]int{}
	mergedBlocks = map[string]types.Block{}

	aFirst, ok = getBlockByIndex(1, aLast, aChain)
	if ok {
		mergeChain(aFirst, countBlocks, mergedBlocks)
	}

	bFirst, ok = getBlockByIndex(1, bLast, bChain)
	if ok {
		mergeChain(bFirst, countBlocks, mergedBlocks)
	}

	cFirst, ok = getBlockByIndex(1, cLast, cChain)
	if ok {
		mergeChain(cFirst, countBlocks, mergedBlocks)
	}

	dFirst, ok = getBlockByIndex(1, dLast, dChain)
	if ok {
		mergeChain(dFirst, countBlocks, mergedBlocks)
	}

	eFirst, ok = getBlockByIndex(1, eLast, eChain)
	if ok {
		mergeChain(eFirst, countBlocks, mergedBlocks)
	}

	// There should be at least one block with a count of 3 for block 0

	threeFound = false
	keyFound = ""
	for k, v := range countBlocks {
		if v >= 3 {
			threeFound = true
			keyFound = k
		}
	}

	require.True(t, threeFound)

	// We check that the block with at least three occurrences is not the same
	// one as the first block.

	require.NotEqual(t, mergedBlocks[keyFound].Metahash, firstBlock.Metahash)
	require.NotEqual(t, mergedBlocks[keyFound].Filename, firstBlock.Filename)

	require.True(t, threeFound)

	block1 = types.Block{
		BlockNumber:  1,
		Filename:     name1,
		Metahash:     metaHash1,
		PreviousHash: firstBlock.Hash(),
	}

	block2 = types.Block{
		BlockNumber:  1,
		Filename:     name2,
		Metahash:     metaHash2,
		PreviousHash: firstBlock.Hash(),
	}

	allPotentialBlocks = []types.Block{
		block1,
		block2,
	}

	require.Contains(t, allPotentialBlocks, mergedBlocks[keyFound])
}

// Test 12
// We make Node A send multiple proposals, which should all be included in
// succeeding blocks.
func TestGossiper_No_Contention_Long_Blockchain(t *testing.T) {
	numProposals := 6
	numNodes := 5
	proposalIndex := 0

	maxRetry := 10
	waitRetry := time.Second * 5

	// time.Sleep(time.Second * 10)

	rand.Seed(time.Now().UnixNano())

	proposeBlock := func(node *nodeInfo) (string, []byte) {

		fileNameBuf := make([]byte, 8)
		_, err := rand.Read(fileNameBuf)
		require.NoError(t, err)

		fileName := hex.EncodeToString(fileNameBuf)

		content := make([]byte, 4)
		_, err = rand.Read(content)
		require.NoError(t, err)

		// Compute the metahash
		h := sha256.New()
		h.Write(content)
		chunk := h.Sum(nil)

		h = sha256.New()
		h.Write(chunk)
		metaHash := h.Sum(nil)

		_, blocks := node.gossiper.GetBlocks()
		blockN := len(blocks)

		node.addFile(t, fileName, content)
		node.gossiper.IndexShares(fileName)

		lastBlock, blocks := node.gossiper.GetBlocks()

		for len(blocks) == blockN {
			time.Sleep(time.Millisecond * 500)
			lastBlock, blocks = node.gossiper.GetBlocks()
		}

		block := blocks[lastBlock]

		require.Equal(t, metaHash, block.Metahash)
		require.Equal(t, fileName, block.Filename)
		require.Equal(t, proposalIndex, block.BlockNumber)

		proposalIndex++

		return fileName, metaHash
	}

	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		n := createAndStartNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()

	var filename string
	var metaHash []byte

	for i := 0; i < numProposals; i++ {
		filename, metaHash = proposeBlock(nodes[0])
	}

	// We check that at least N/2 + 1 other nodes have 6 blocks

	ok := false

	for i := 0; i < maxRetry; i++ {

		hasBlocks := 0

		checkBlocks := func(node *nodeInfo) {
			last, chain := node.gossiper.GetBlocks()

			time.Sleep(time.Millisecond * 100)

			if len(chain) == numProposals {
				hasBlocks++

				lastBlock := chain[last]
				require.Equal(t, filename, lastBlock.Filename)
				require.Equal(t, metaHash, lastBlock.Metahash)
				require.Equal(t, numProposals-1, lastBlock.BlockNumber)
			}
		}

		time.Sleep(time.Second)

		for _, n := range nodes {
			checkBlocks(n)
		}

		if hasBlocks >= numNodes/2+1 {
			ok = true
			break
		}

		time.Sleep(waitRetry)
	}

	require.True(t, ok, "after %d retries, didn't find enough nodes "+
		"with the expected number of blocks", maxRetry)
}

// Test 12 INTEGRATION
// We make Node A send multiple proposals, which should all be included in
// succeeding blocks.
func TestBinGossiper_No_Contention_Long_Blockchain(t *testing.T) {
	numProposals := 6
	numNodes := 5
	proposalIndex := 0

	maxRetry := 10
	waitRetry := time.Second * 5

	// time.Sleep(time.Second * 10)

	referenceNodes := map[int]struct{}{
		1: struct{}{}, 2: struct{}{},
		3: struct{}{}, 4: struct{}{},
	}

	rand.Seed(time.Now().UnixNano())

	proposeBlock := func(node *nodeInfo) (string, []byte) {

		fileNameBuf := make([]byte, 8)
		_, err := rand.Read(fileNameBuf)
		require.NoError(t, err)

		fileName := hex.EncodeToString(fileNameBuf)

		content := make([]byte, 4)
		_, err = rand.Read(content)
		require.NoError(t, err)

		// Compute the metahash
		h := sha256.New()
		h.Write(content)
		chunk := h.Sum(nil)

		h = sha256.New()
		h.Write(chunk)
		metaHash := h.Sum(nil)

		_, blocks := node.gossiper.GetBlocks()
		blockN := len(blocks)

		node.addFile(t, fileName, content)
		node.gossiper.IndexShares(fileName)

		lastBlock, blocks := node.gossiper.GetBlocks()

		for len(blocks) == blockN {
			time.Sleep(time.Millisecond * 500)
			lastBlock, blocks = node.gossiper.GetBlocks()
		}

		block := blocks[lastBlock]

		require.Equal(t, metaHash, block.Metahash)
		require.Equal(t, fileName, block.Filename)
		require.Equal(t, proposalIndex, block.BlockNumber)

		proposalIndex++

		return fileName, metaHash
	}

	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		var n nodeInfo

		_, found := referenceNodes[i]
		if found {
			n = createAndStartBinNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		} else {
			n = createAndStartNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		}

		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()

	var filename string
	var metaHash []byte

	for i := 0; i < numProposals; i++ {
		filename, metaHash = proposeBlock(nodes[0])
	}

	// We check that at least N/2 + 1 other nodes have 6 blocks

	ok := false

	for i := 0; i < maxRetry; i++ {

		hasBlocks := 0

		checkBlocks := func(node *nodeInfo) {
			last, chain := node.gossiper.GetBlocks()

			time.Sleep(time.Millisecond * 100)

			if len(chain) == numProposals {
				hasBlocks++

				lastBlock := chain[last]
				require.Equal(t, filename, lastBlock.Filename)
				require.Equal(t, metaHash, lastBlock.Metahash)
				require.Equal(t, numProposals-1, lastBlock.BlockNumber)
			}
		}

		time.Sleep(time.Second)

		for _, n := range nodes {
			checkBlocks(n)
		}

		if hasBlocks >= numNodes/2+1 {
			ok = true
			break
		}

		time.Sleep(waitRetry)
	}

	require.True(t, ok, "after %d retries, didn't find enough nodes "+
		"with the expected number of blocks", maxRetry)
}

// Test 13
// Same as test 12 but with more nodes
func TestGossiper_No_Contention_Higher_Nodes_Long_Blockchain(t *testing.T) {
	numProposals := 4
	numNodes := 11

	maxRetry := 10
	waitRetry := time.Second * 5

	proposalIndex := 0

	time.Sleep(time.Second * 10)

	rand.Seed(time.Now().UnixNano())

	proposeBlock := func(node *nodeInfo) (string, []byte) {

		fileNameBuf := make([]byte, 8)
		_, err := rand.Read(fileNameBuf)
		require.NoError(t, err)

		fileName := hex.EncodeToString(fileNameBuf)

		content := make([]byte, 4)
		_, err = rand.Read(content)
		require.NoError(t, err)

		// Compute the metahash
		h := sha256.New()
		h.Write(content)
		chunk := h.Sum(nil)

		h = sha256.New()
		h.Write(chunk)
		metaHash := h.Sum(nil)

		_, blocks := node.gossiper.GetBlocks()
		blockN := len(blocks)

		node.addFile(t, fileName, content)
		node.gossiper.IndexShares(fileName)

		lastBlock, blocks := node.gossiper.GetBlocks()

		for len(blocks) == blockN {
			time.Sleep(time.Millisecond * 500)
			lastBlock, blocks = node.gossiper.GetBlocks()
		}

		block := blocks[lastBlock]

		require.Equal(t, metaHash, block.Metahash)
		require.Equal(t, fileName, block.Filename)
		require.Equal(t, proposalIndex, block.BlockNumber)

		proposalIndex++

		return fileName, metaHash
	}

	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		n := createAndStartNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()

	var filename string
	var metaHash []byte

	for i := 0; i < numProposals; i++ {
		filename, metaHash = proposeBlock(nodes[0])
	}

	// We check that at least N/2 - 1 other nodes have 6 blocks

	ok := false

	for i := 0; i < maxRetry; i++ {
		hasBlocks := 0

		checkBlocks := func(node *nodeInfo) {
			last, chain := node.gossiper.GetBlocks()

			if len(chain) == numProposals {
				hasBlocks++

				lastBlock := chain[last]
				require.Equal(t, filename, lastBlock.Filename)
				require.Equal(t, metaHash, lastBlock.Metahash)
				require.Equal(t, numProposals-1, lastBlock.BlockNumber)
			}
		}

		for _, n := range nodes {
			checkBlocks(n)
		}

		if hasBlocks >= numNodes/2+1 {
			ok = true
			break
		}
		time.Sleep(waitRetry)
	}

	require.True(t, ok, "after %d retries, didn't find enough nodes "+
		"with the expected number of blocks", maxRetry)
}

// Test 14
// Multiple nodes try to send a proposal. We check that at least 3 nodes have
// the same proposal in their last and only block
func TestGossiper_Contention_Long_Blockchain(t *testing.T) {
	numNodes := 5
	numProposals := 4

	maxRetry := 30
	waitRetry := time.Second * 10

	time.Sleep(time.Second * 10)

	rand.Seed(time.Now().UnixNano())

	proposeBlock := func(node *nodeInfo) (string, []byte) {

		fileNameBuf := make([]byte, 8)
		_, err := rand.Read(fileNameBuf)
		require.NoError(t, err)

		fileName := hex.EncodeToString(fileNameBuf)

		content := make([]byte, 4)
		_, err = rand.Read(content)
		require.NoError(t, err)

		// Compute the metahash
		h := sha256.New()
		h.Write(content)
		chunk := h.Sum(nil)

		h = sha256.New()
		h.Write(chunk)
		metaHash := h.Sum(nil)

		node.addFile(t, fileName, content)
		node.gossiper.IndexShares(fileName)

		return fileName, metaHash
	}

	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		n := createAndStartNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()

	fileNames := make([]string, numProposals)
	metaHashes := make([][]byte, numProposals)

	for i := 0; i < numProposals; i++ {
		go func(i int) {
			filename, metahash := proposeBlock(nodes[i])
			fileNames[i] = filename
			metaHashes[i] = metahash
		}(i)
	}

	ok := false

	time.Sleep(waitRetry)

	for i := 0; i < maxRetry; i++ {

		numFound := 0

		// We check that at least N/2 - 1 other nodes have numProposals blocks

		for _, n := range nodes {
			_, chain := n.gossiper.GetBlocks()

			time.Sleep(time.Millisecond * 100)

			if len(chain) == numProposals {
				numFound++
			}
		}

		if numFound >= numNodes/2+1 {
			ok = true
			break
		}

		time.Sleep(waitRetry)
	}

	require.True(t, ok, "consensus not reached after %d retries", maxRetry)
}

// Test 14 INTEGRATION
// Multiple nodes try to send a proposal. We check that at least 3 nodes have
// the same proposal in their last and only block
func TestBinGossiper_Contention_Long_Blockchain(t *testing.T) {
	numNodes := 5
	numProposals := 4

	maxRetry := 30
	waitRetry := time.Second * 10

	time.Sleep(time.Second * 10)

	referenceNodes := map[int]struct{}{
		1: struct{}{}, 2: struct{}{},
		3: struct{}{}, 4: struct{}{},
	}

	rand.Seed(time.Now().UnixNano())

	proposeBlock := func(node *nodeInfo) (string, []byte) {

		fileNameBuf := make([]byte, 8)
		_, err := rand.Read(fileNameBuf)
		require.NoError(t, err)

		fileName := hex.EncodeToString(fileNameBuf)

		content := make([]byte, 4)
		_, err = rand.Read(content)
		require.NoError(t, err)

		// Compute the metahash
		h := sha256.New()
		h.Write(content)
		chunk := h.Sum(nil)

		h = sha256.New()
		h.Write(chunk)
		metaHash := h.Sum(nil)

		node.addFile(t, fileName, content)
		node.gossiper.IndexShares(fileName)

		return fileName, metaHash
	}

	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		var n nodeInfo

		_, found := referenceNodes[i]
		if found {
			n = createAndStartBinNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		} else {
			n = createAndStartNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		}

		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()

	fileNames := make([]string, numProposals)
	metaHashes := make([][]byte, numProposals)

	for i := 0; i < numProposals; i++ {
		go func(i int) {
			filename, metahash := proposeBlock(nodes[i])
			fileNames[i] = filename
			metaHashes[i] = metahash
		}(i)
	}

	ok := false

	time.Sleep(waitRetry)

	for i := 0; i < maxRetry; i++ {

		numFound := 0

		// We check that at least N/2 - 1 other nodes have numProposals blocks

		for _, n := range nodes {
			_, chain := n.gossiper.GetBlocks()

			time.Sleep(time.Millisecond * 100)

			if len(chain) == numProposals {
				numFound++
			}
		}

		if numFound >= numNodes/2+1 {
			ok = true
			break
		}

		time.Sleep(waitRetry)
	}

	require.True(t, ok, "consensus not reached after %d retries", maxRetry)
}

// Test 15
// Same as test 14 but with more nodes
func TestGossiper_Contention_Higher_Nodes_Long_Blockchain(t *testing.T) {
	numNodes := 11
	numProposals := 4

	maxRetry := 30
	waitRetry := time.Second * 10

	time.Sleep(time.Second * 10)

	rand.Seed(time.Now().UnixNano())

	proposeBlock := func(node *nodeInfo) (string, []byte) {

		fileNameBuf := make([]byte, 8)
		_, err := rand.Read(fileNameBuf)
		require.NoError(t, err)

		fileName := hex.EncodeToString(fileNameBuf)

		content := make([]byte, 4)
		_, err = rand.Read(content)
		require.NoError(t, err)

		// Compute the metahash
		h := sha256.New()
		h.Write(content)
		chunk := h.Sum(nil)

		h = sha256.New()
		h.Write(chunk)
		metaHash := h.Sum(nil)

		node.addFile(t, fileName, content)
		node.gossiper.IndexShares(fileName)

		return fileName, metaHash
	}

	nodes := make([]*nodeInfo, numNodes)
	bagNodes := make(map[string]nodeInfo)

	for i := 0; i < numNodes; i++ {
		n := createAndStartNode(t, string(rune('A'+i)), WithNodeIndex(i), WithNumParticipants(numNodes), WithAntiEntropy(1))
		nodes[i] = &n
		bagNodes[string(rune('A'+i))] = n
	}

	defer stopNodes(nodes...)

	lb := newLinkBuilder(bagNodes)
	lb.connectAll()

	fileNames := make([]string, numProposals)
	metaHashes := make([][]byte, numProposals)

	for i := 0; i < numProposals; i++ {
		go func(i int) {
			filename, metahash := proposeBlock(nodes[i])
			fileNames[i] = filename
			metaHashes[i] = metahash
		}(i)
	}

	ok := false

	time.Sleep(waitRetry)

	for i := 0; i < maxRetry; i++ {

		numFound := 0

		// We check that at least N/2 - 1 other nodes have numProposals blocks

		for _, n := range nodes {
			_, chain := n.gossiper.GetBlocks()

			time.Sleep(time.Millisecond * 100)

			if len(chain) == numProposals {
				numFound++
			}
		}

		if numFound >= numNodes/2+1 {
			ok = true
			break
		}

		time.Sleep(waitRetry)
	}

	require.True(t, ok, "consensus not reached after %d retries", maxRetry)
}

// Test 16
// We check the uniqueness of a filename on the chain. If a filename has already
// been recorded on the chain then it should not be able to store the same
// filename again.
func TestGossiper_Unique_Filename(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	// D register a filename

	filename := "test1.txt"
	content := []byte{0xAA}

	// Compute the metahash
	h := sha256.New()
	h.Write(content)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash := h.Sum(nil)

	time.Sleep(time.Second)

	nD.addFile(t, filename, content)
	nD.gossiper.IndexShares(filename)

	time.Sleep(time.Second * 10)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	checkBlock := func(last string, chain map[string]types.Block) int {
		if len(chain) == 1 {
			block := chain[aLast]
			expected := types.Block{
				BlockNumber:  0,
				Filename:     filename,
				Metahash:     metaHash,
				PreviousHash: make([]byte, 32),
			}
			require.Equal(t, expected, block)

			return 1
		}

		return 0
	}

	aCheck := checkBlock(aLast, aChain)
	bCheck := checkBlock(bLast, bChain)
	cCheck := checkBlock(cLast, cChain)
	dCheck := checkBlock(dLast, dChain)
	eCheck := checkBlock(eLast, eChain)

	require.True(t, aCheck+bCheck+cCheck+dCheck+eCheck >= 3)

	// C tries to register the same filename

	content = []byte{0xBB}

	nC.addFile(t, filename, content)
	nC.gossiper.IndexShares(filename)

	time.Sleep(time.Second * 2)

	_, aChain = nA.gossiper.GetBlocks()
	_, bChain = nB.gossiper.GetBlocks()
	_, cChain = nC.gossiper.GetBlocks()
	_, dChain = nD.gossiper.GetBlocks()
	_, eChain = nE.gossiper.GetBlocks()

	require.Len(t, aChain, 1)
	require.Len(t, bChain, 1)
	require.Len(t, cChain, 1)
	require.Len(t, dChain, 1)
	require.Len(t, eChain, 1)
}

// Test 17
// We check the uniqueness of a metahash on the chain. If a metahash has already
// been recorded on the chain then it should not be able to store the same
// filename again.
func TestGossiper_Unique_Metahash(t *testing.T) {
	nA := createAndStartNode(t, "NA", WithNodeIndex(0), WithNumParticipants(5), WithAntiEntropy(1))
	nB := createAndStartNode(t, "NB", WithNodeIndex(1), WithNumParticipants(5), WithAntiEntropy(1))
	nC := createAndStartNode(t, "NC", WithNodeIndex(2), WithNumParticipants(5), WithAntiEntropy(1))
	nD := createAndStartNode(t, "ND", WithNodeIndex(3), WithNumParticipants(5), WithAntiEntropy(1))
	nE := createAndStartNode(t, "NE", WithNodeIndex(4), WithNumParticipants(5), WithAntiEntropy(1))

	defer stopNodes(&nA, &nB, &nC, &nD, &nE)

	nodes := map[string]nodeInfo{
		"A": nA, "B": nB, "C": nC, "D": nD, "E": nE,
	}

	lb := newLinkBuilder(nodes)
	lb.connectAll()

	filename := "test1.txt"
	content := []byte{0xAA}

	// Compute the metahash
	h := sha256.New()
	h.Write(content)
	chunk := h.Sum(nil)

	h = sha256.New()
	h.Write(chunk)
	metaHash := h.Sum(nil)

	nC.addFile(t, filename, content)
	nC.gossiper.IndexShares(filename)

	time.Sleep(time.Second * 2)

	aLast, aChain := nA.gossiper.GetBlocks()
	bLast, bChain := nB.gossiper.GetBlocks()
	cLast, cChain := nC.gossiper.GetBlocks()
	dLast, dChain := nD.gossiper.GetBlocks()
	eLast, eChain := nE.gossiper.GetBlocks()

	checkBlock := func(last string, chain map[string]types.Block) int {
		if len(chain) == 1 {
			block := chain[aLast]
			expected := types.Block{
				BlockNumber:  0,
				Filename:     filename,
				Metahash:     metaHash,
				PreviousHash: make([]byte, 32),
			}
			require.Equal(t, expected, block)

			return 1
		}

		return 0
	}

	aCheck := checkBlock(aLast, aChain)
	bCheck := checkBlock(bLast, bChain)
	cCheck := checkBlock(cLast, cChain)
	dCheck := checkBlock(dLast, dChain)
	eCheck := checkBlock(eLast, eChain)

	require.True(t, aCheck+bCheck+cCheck+dCheck+eCheck >= 3)

	// Try to register the same content with a different filename

	filename = "test2.txt"

	nC.addFile(t, filename, content)
	nC.gossiper.IndexShares(filename)

	time.Sleep(time.Second * 2)

	_, aChain = nA.gossiper.GetBlocks()
	_, bChain = nB.gossiper.GetBlocks()
	_, cChain = nC.gossiper.GetBlocks()
	_, dChain = nD.gossiper.GetBlocks()
	_, eChain = nE.gossiper.GetBlocks()

	require.Len(t, aChain, 1)
	require.Len(t, bChain, 1)
	require.Len(t, cChain, 1)
	require.Len(t, dChain, 1)
	require.Len(t, eChain, 1)
}

// -----------------------------------------------------------------------------
// Utility functions

type nodeInfo struct {
	addr     string
	id       string
	gossiper BaseGossiper
	folder   string
	stream   chan GossipPacket // optional

	// file created in the working directory that must be deleted
	localFiles []string

	template nodeTemplate
}

func newNodeInfo(g BaseGossiper, addr string, t nodeTemplate) *nodeInfo {
	return &nodeInfo{
		id:       g.GetIdentifier(),
		addr:     addr,
		gossiper: g,
		template: t,
	}
}

func (n nodeInfo) shareDir() string {
	return n.template.sharedDataFolder
}

func (n nodeInfo) downloadDir() string {
	return n.template.downloadFolder
}

func (n nodeInfo) getIns(ctx context.Context) *history {
	watchIn := &history{p: make([]packet, 0)}
	chanIn := n.gossiper.Watch(ctx, true)

	go func() {
		for p := range chanIn {
			watchIn.Lock()
			watchIn.p = append(watchIn.p, packet{p.Addr, p.Msg})
			// fmt.Println(p)
			watchIn.Unlock()
		}
	}()

	return watchIn
}

func (n nodeInfo) getOuts(ctx context.Context) *history {
	watchOut := &history{p: make([]packet, 0)}
	chanOut := n.gossiper.Watch(ctx, false)

	go func() {
		for p := range chanOut {
			watchOut.Lock()
			watchOut.p = append(watchOut.p, packet{p.Addr, p.Msg})
			// fmt.Println(p)
			watchOut.Unlock()
		}
	}()

	return watchOut
}

func (n nodeInfo) getCallbacks(ctx context.Context) *history {
	msgs := &history{p: make([]packet, 0)}

	n.gossiper.RegisterCallback(func(origin string, message GossipPacket) {
		msgs.Lock()
		msgs.p = append(msgs.p, packet{origin, message})
		msgs.Unlock()
	})

	return msgs
}

// addFile adds a file to the current working directory. This is needed because
// the indexFile method looks for the file in that place.
func (n *nodeInfo) addFile(t *testing.T, name string, content []byte) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	err = ioutil.WriteFile(wd+"/"+name, content, os.ModePerm)
	require.NoError(t, err)

	n.localFiles = append(n.localFiles, wd+"/"+name)
}

func (n *nodeInfo) stop() {
	n.gossiper.Stop()

	if n.folder != "" {
		os.RemoveAll(n.folder)
	}

	for _, localFile := range n.localFiles {
		os.Remove(localFile)
	}
}

func createAndStartNode(t *testing.T, name string, opts ...nodeOption) nodeInfo {

	n := createNode(t, factory, "127.0.0.1:0", name, opts...)

	startNodesBlocking(t, n.gossiper)

	n.addr = n.gossiper.GetLocalAddr()

	return n
}

func createAndStartBinNode(t *testing.T, name string, opts ...nodeOption) nodeInfo {

	n := createNode(t, NewBinGossipFactory("./hw3.linux.amd64"), "127.0.0.1:0", name, opts...)

	startNodesBlocking(t, n.gossiper)

	n.addr = n.gossiper.GetLocalAddr()

	return n
}

// createNode creates a node but don't start it. The address of the node is
// unknown until it has been started.
func createNode(t *testing.T, fac GossipFactory, addr, name string, opts ...nodeOption) nodeInfo {

	template := newNodeTemplate().apply(opts...)

	fullName := fmt.Sprintf("%v---%v", name, t.Name())

	var folder string
	var err error

	if template.sharedDataFolder == "" || template.downloadFolder == "" {
		folder, err = ioutil.TempDir("", "gossiper-"+name+"-")
		require.NoError(t, err)

		if template.sharedDataFolder == "" {
			err = os.Mkdir(folder+"/"+sharedDataFolder, os.ModePerm)
			require.NoError(t, err)

			_, err = os.Create(folder + "/" + sharedDataFolder + "/indexedState.json")
			require.NoError(t, err)

			template.sharedDataFolder = folder + "/" + sharedDataFolder
		}

		if template.downloadFolder == "" {
			err = os.Mkdir(folder+"/"+downloadFolder, os.ModePerm)
			require.NoError(t, err)

			template.downloadFolder = folder + "/" + downloadFolder
		}
	}

	node, err := fac.New(addr, fullName, template.antiEntropy, template.routeTimer,
		template.sharedDataFolder, template.downloadFolder, template.numParticipants,
		template.nodeIndex, template.paxosRetry)
	require.NoError(t, err)

	require.Len(t, node.GetNodes(), 0)
	require.Equal(t, fullName, node.GetIdentifier())

	return nodeInfo{
		id:       node.GetIdentifier(),
		gossiper: node,
		folder:   folder,
		template: *template,
	}
}

type nodeTemplate struct {
	antiEntropy int
	routeTimer  int

	sharedDataFolder string
	downloadFolder   string

	numParticipants int
	nodeIndex       int
	paxosRetry      int
}

func newNodeTemplate() *nodeTemplate {
	return &nodeTemplate{
		antiEntropy: 1000,
		routeTimer:  0,

		nodeIndex:       0,
		numParticipants: 1,
		paxosRetry:      1000,
	}
}

func (t *nodeTemplate) apply(opts ...nodeOption) *nodeTemplate {
	for _, opt := range opts {
		opt(t)
	}

	return t
}

type nodeOption func(*nodeTemplate)

func WithAntiEntropy(value int) nodeOption {
	return func(n *nodeTemplate) {
		n.antiEntropy = value
	}
}

func WithRouteTimer(value int) nodeOption {
	return func(n *nodeTemplate) {
		n.routeTimer = value
	}
}

func WithSharedDataFolder(folder string) nodeOption {
	return func(n *nodeTemplate) {
		n.sharedDataFolder = folder
	}
}

func WithDownloadFolder(folder string) nodeOption {
	return func(n *nodeTemplate) {
		n.downloadFolder = folder
	}
}

func WithNumParticipants(num int) nodeOption {
	return func(n *nodeTemplate) {
		n.numParticipants = num
	}
}

func WithNodeIndex(index int) nodeOption {
	return func(n *nodeTemplate) {
		n.nodeIndex = index
	}
}

func WithPaxosRetry(d int) nodeOption {
	return func(n *nodeTemplate) {
		n.paxosRetry = d
	}
}

// startNodesBlocking waits until the node is started
func startNodesBlocking(t *testing.T, nodes ...BaseGossiper) {
	wg := new(sync.WaitGroup)
	wg.Add(len(nodes))
	for idx := range nodes {
		go func(i int) {
			defer wg.Done()
			ready := make(chan struct{})
			go nodes[i].Run(ready)
			<-ready
		}(idx)
	}
	wg.Wait()
}

type packet struct {
	origin  string
	message GossipPacket
}

type history struct {
	sync.Mutex
	p []packet
}

func (h *history) getPs() []packet {
	h.Lock()
	defer h.Unlock()

	return append([]packet{}, h.p...)
}

type linkBuilder struct {
	nodes map[string]nodeInfo
}

func newLinkBuilder(nodes map[string]nodeInfo) linkBuilder {
	return linkBuilder{nodes}
}

func (b linkBuilder) add(link string) {
	// we accept the following links:
	// "A --> B"
	// "A <-- B"
	// "A <-> B"

	parts := strings.Split(link, " ")
	nA := b.nodes[parts[0]]
	nB := b.nodes[parts[2]]

	switch parts[1] {
	case "-->":
		nA.gossiper.AddRoute(nB.id, nB.addr)
		nA.gossiper.AddAddresses(nB.addr)
	case "<--":
		nB.gossiper.AddRoute(nA.id, nA.addr)
		nB.gossiper.AddAddresses(nA.addr)
	case "<->":
		nA.gossiper.AddRoute(nB.id, nB.addr)
		nB.gossiper.AddRoute(nA.id, nA.addr)

		nA.gossiper.AddAddresses(nB.addr)
		nB.gossiper.AddAddresses(nA.addr)
	default:
		fmt.Println("[WARNING] unknown link", parts[1])
	}
}

func (b linkBuilder) connectAll() {
	for idFrom := range b.nodes {
		for idTo := range b.nodes {
			if idTo == idFrom {
				continue
			}

			b.add(idFrom + " --> " + idTo)
		}
	}
}

// stopNodes stops a set of nodes concurrently
func stopNodes(nodes ...*nodeInfo) {
	wg := sync.WaitGroup{}
	wg.Add(len(nodes))

	for _, node := range nodes {
		go func(n *nodeInfo) {
			n.stop()
			wg.Done()
		}(node)
	}

	wg.Wait()
}
