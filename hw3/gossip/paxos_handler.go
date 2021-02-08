// ========== CS-438 HW3 Skeleton ===========
// *** Implement here the handler for simple message processing ***

package gossip

import (
	"fmt"
	"net"

	"go.dedis.ch/cs438/hw3/gossip/types"
)

// ExecExtraMsg .
func (g *Gossiper) ExecExtraMsg(msg *types.ExtraMessage, addr *net.UDPAddr, origin string) {
	var err error
	// solve the paxos messages
	// fmt.Printf("%s got a new extra message\n", g.Name)
	switch {
	case msg.PaxosPrepare != nil:
		err = g.ExecPaxosPrepare(msg.PaxosPrepare, addr)
	case msg.PaxosPromise != nil:
		err = g.ExecPaxosPromise(msg.PaxosPromise, addr, origin)
	case msg.PaxosPropose != nil:
		err = g.ExecPaxosPropose(msg.PaxosPropose, addr)
	case msg.PaxosAccept != nil:
		// fmt.Printf("%s got a paxos accept from %s\n", g.Name, origin)
		err = g.ExecPaxosAccept(msg.PaxosAccept, addr)
	case msg.TLC != nil:
		// fmt.Printf("%s got a TLC from %s\n", g.Name, origin)
		err = g.ExecTLC(msg.TLC, addr)
		// fmt.Printf("%s handle a TLC\n", g.Name)
	}
	// Display error if any
	if err != nil {
		panic(fmt.Sprintln("Fail to process ExtraMessage: ", err))
	}
}

// ExecPaxosPrepare .
func (g *Gossiper) ExecPaxosPrepare(msg *types.PaxosPrepare, addr *net.UDPAddr) error {
	_, ext := g.Paxos.TryAcceptPrepare(msg, g.Name)
	// if givePromise {
	// 	fmt.Printf("%s give promise to PaxosSeqID: %v, ID: %v\n", g.Name, msg.PaxosSeqID, msg.ID)
	// }
	if ext != nil {
		// broadcast my reply for a prepare: promise or reply with another accept id
		rumor := g.ConstructExtraRumor(ext)
		g.BroadcastRumorForPaxos(rumor)
		g.UpdatePeerStatusList(rumor)
		// go g.StartRumorMongering(rumor, nil)
	}
	return nil
}

// ExecPaxosPromise .
func (g *Gossiper) ExecPaxosPromise(msg *types.PaxosPromise, addr *net.UDPAddr, origin string) error {
	// fmt.Println(g.Name, "start exec promise")
	g.Paxos.TryAdmitPromise(msg, origin, g.Name)
	// forMe, AbvThr := g.Paxos.TryAdmitPromise(msg, origin)
	return nil
}

// ExecPaxosPropose .
func (g *Gossiper) ExecPaxosPropose(msg *types.PaxosPropose, addr *net.UDPAddr) error {
	_, ext := g.Paxos.TryAcceptPropose(msg, g.Name)
	// if giveAccept {
	// 	fmt.Printf("%s give accept to PaxosSeqID: %v, ID: %v\n", g.Name, msg.PaxosSeqID, msg.ID)
	// }
	if ext != nil {
		// broadcast my accept of a proposal
		rumor := g.ConstructExtraRumor(ext)
		// fmt.Printf("%s send an accept\n", g.Name)
		// go g.StartRumorMongering(rumor, nil)
		g.BroadcastRumorForPaxos(rumor)
	}
	return nil
}

// ExecPaxosAccept .
func (g *Gossiper) ExecPaxosAccept(msg *types.PaxosAccept, addr *net.UDPAddr) error {
	AbvThr, ext := g.Paxos.TryAdmitAccept(msg, g.Name)
	// fmt.Printf("%s receive an accept message.", g.Name)
	if AbvThr {
		// it's time to send a tlc
		// g.BroadcastMessage(GossipPacket{Rumor: g.ConstructExtraRumor(ext)})
		// fmt.Printf("%s start send a TLC on SeqID: %v, ID: %v\n", g.Name, msg.PaxosSeqID, msg.ID)
		// fmt.Printf("send TLC: %+v", ext.TLC.Block)
		// g.StartRumorMongering(, nil)
		// check previous hash is ok:
		if len(ext.TLC.Block.PreviousHash) == 0 {
			ext.TLC.Block.PreviousHash = make([]byte, 32)
		}
		rumor := g.ConstructExtraRumor(ext)
		g.BroadcastRumorForPaxos(rumor)
		// g.BroadcastRumorForPaxos(rumor)
		// add this TLC into observed tlc record
		g.ExecTLC(&types.TLC{Block: msg.Value}, nil)
		// g.Paxos.AccHelper.Lock()
		// defer g.Paxos.AccHelper.Unlock()
		// for hash := range g.Paxos.AccHelper.ObsvTLC {
		// 	if hash == hex.EncodeToString(ext.TLC.Block.Hash()) {
		// 		g.Paxos.AccHelper.ObsvTLC[hash]++
		// 	}
		// 	return nil
		// }
		// g.Paxos.AccHelper.ObsvTLC[hex.EncodeToString(ext.TLC.Block.Hash())] = 1
	}
	return nil
}

// ExecTLC .
func (g *Gossiper) ExecTLC(msg *types.TLC, addr *net.UDPAddr) error {
	// get the num of this TLC block
	_, num := g.Paxos.TryAcceptTLC(msg, g.Name)
	if num == int(g.Paxos.N/2+1) {
		g.CheckConsensusStatus(msg)
		// fmt.Printf("%s TLC num: %v, ifNew: %v, close chan: true \n", g.Name, num, ifNew)
		return nil
	}
	// fmt.Printf("%s TLC on SeqID: %v\n", g.Name, msg.Block.BlockNumber)
	return nil
}
