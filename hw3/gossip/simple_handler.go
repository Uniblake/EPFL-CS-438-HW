// ========== CS-438 HW2 Skeleton ===========
// *** Implement here the handler for simple message processing ***

package gossip

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"go.dedis.ch/onet/v3/log"
)

// Exec is the function that the gossiper uses to execute the handler for a SimpleMessage
// processSimple processes a SimpleMessage as such:
// - add message's relay address to the known peers
// - update the relay field
func (msg *SimpleMessage) Exec(g *Gossiper, addr *net.UDPAddr) error {
	fmt.Printf("SIMPLE MESSAGE origin %s from %s contents %s\n",
		msg.OriginPeerName, msg.RelayPeerAddr, msg.Contents)
	defer g.StopWait.Done()
	currentPeers := g.GetNodes()
	// change relay address
	msg.RelayPeerAddr = g.GossipAddr.String()
	newPacket := &GossipPacket{Simple: msg}
	jsonPacket, err := json.Marshal(newPacket)
	if err != nil {
		log.Fatal("SimpleMessage Exec wrong serialization: ", err)
	}
	// callback
	if g.callBack != nil {
		g.callBack(msg.OriginPeerName, GossipPacket{Simple: msg})
	}
	for _, each := range currentPeers {
		if each != addr.String() {
			UDPAddress, err := net.ResolveUDPAddr(udpVer, each)
			if err != nil {
				panic(fmt.Sprintln("SimpleMessage Exec Fail to resolve udp adress:", err))
			}
			g.GossipConn.WriteToUDP(jsonPacket, UDPAddress)
		}
	}
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a RumorMessage
func (msg *RumorMessage) Exec(g *Gossiper, addr *net.UDPAddr) error {
	// if msg.Text != "" {
	// 	fmt.Printf("RUMOR origin %s from %s ID %d contents %s\n",
	// 		msg.Origin, addr.String(), msg.ID, msg.Text)
	// }
	defer g.StopWait.Done()
	// update route map
	g.UpdateRoutes(msg, addr)
	// fmt.Println("recv a new message")
	// lock and update the peers status list.
	ifNew := g.UpdatePeerStatusList(msg)
	currentStatus := g.GetPeerStatusList()
	if ifNew {
		if g.callBack != nil && msg.Text != "" {
			g.callBack(msg.Origin, GossipPacket{Rumor: msg}.Copy())
		}
	}
	// fmt.Printf("Node: %s, Rumor ID: %v, ExtraPrepare: %+v\n", g.Name, msg.ID, msg.Extra.PaxosPrepare)
	// fmt.Printf("Rumor: %+v, %v, %v\n", msg.Extra, msg.Extra != nil, ifNew)
	// conduct block chain operation
	if msg.Extra != nil && ifNew {
		go g.ExecExtraMsg(msg.Extra, addr, msg.Origin)
	}
	// copy the message
	nmsg := GossipPacket{Rumor: msg}.Copy().Rumor
	IfAlive := g.MongeringTask.CheckIfIsMongering(addr, nmsg, nil)
	if IfAlive == 0 {
		if (nmsg.Origin == g.Name) && ifNew {
			g.StartRumorMongering(nmsg, nil)
		} else {
			g.SendStatusMessage(addr, currentStatus)
			// fmt.Printf("%s respond: %s with a status packet: %+v, ifNew: %v\n", g.Name, addr.String(), currentStatus, ifNew)
			if ifNew {
				g.StartRumorMongering(nmsg, addr)
			}
		}
	}
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a StatusMessage
func (msg *StatusPacket) Exec(g *Gossiper, addr *net.UDPAddr) error {
	// msgContents := ""
	// for _, eachOne := range msg.Want {
	// 	msgContents = msgContents + fmt.Sprintf(" peer %s nextID %d", eachOne.Identifier, eachOne.NextID)
	// }
	// fmt.Printf("STATUS from %s %s\n", addr.String(), msgContents)
	defer g.StopWait.Done()
	// check if mongering, if is, then send a signal for mongering rountine to solve
	nstatus := new(StatusPacket)
	nstatus.Want = append([]PeerStatus{}, msg.Want...)
	IfAlive := g.MongeringTask.CheckIfIsMongering(addr, nil, nstatus)
	if IfAlive == 0 {
		// does not belong to any live mongering task, this is an anti-entropy one
		// get current status
		currentStatus := g.GetPeerStatusList()
		IfMyNew, ToSend := g.FindMyAdvancedRumor(msg, currentStatus)
		if IfMyNew {
			// I have something new to say
			g.SendRumorMessage(addr, &ToSend)
		} else {
			// fmt.Printf("IN SYNC WITH %s\n", addr.String())
			IfInNew := g.FindInAdvancedRumor(msg, currentStatus)
			if IfInNew {
				// there is somehting I want, get my current status
				// call a new routine to send the status message
				g.SendStatusMessage(addr, currentStatus)
			}
		}
	}
	// fmt.Printf("Status %s point3 \n", g.Name)
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a PrivateMessage
func (msg *PrivateMessage) Exec(g *Gossiper, addr *net.UDPAddr) error {
	defer g.StopWait.Done()
	copyRoutes := g.GetRoutingTable()
	negLimit := msg.HopLimit < 0
	forMe := msg.Destination == g.Name
	if negLimit {
		panic("discoverd private message with HopLimit < 0.")
	} else {
		// check and add addr to known peers
		g.AddAddresses(addr.String())
		if forMe {
			// successfully receive the msg for me
			if g.callBack != nil {
				g.callBack(msg.Origin, GossipPacket{Private: msg})
			}
		} else {
			if msg.HopLimit == 0 {
				// run out of chance to deliver it
				return nil
			}
			// else: try to deliver it
			for key := range copyRoutes {
				if key == msg.Destination {
					nextAddr, err := net.ResolveUDPAddr(udpVer, copyRoutes[key].NextHop)
					if err != nil {
						panic(fmt.Sprintln("PrivateMessage Exec: Fail to resolve udp adress:", err))
					}
					msg.HopLimit--
					newPacket := GossipPacket{Private: msg}
					jsonPacket, err := json.Marshal(&newPacket)
					if err != nil {
						log.Fatal("Exec Private Message: wrong serialization: ", err)
					}
					g.GossipConn.WriteToUDP(jsonPacket, nextAddr)
					break
				}
			}

		}
	}
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a DataRequest
func (msg *DataRequest) Exec(g *Gossiper, addr *net.UDPAddr) error {
	defer g.StopWait.Done()
	if msg.Destination == g.Name {
		// find the data and give a reply
		metaHash, dataID, isMeta, ok := g.MetaDataStorage.Find(msg.HashStr())
		if ok {
			dataBytes, ok := g.FetchLocalFile(metaHash, dataID, isMeta)
			if ok {
				err := g.SendDataReply(msg.HashStr(), dataBytes, msg.Origin)
				return err
			}
		}
		return errors.New("Asking data that i do not have")
	}
	// send to  next hop
	if msg.HopLimit > 0 {
		msg.HopLimit--
		if err := g.SendToNextHop(msg.Destination, &GossipPacket{DataRequest: msg}); err != nil {
			return errors.New("No route to the destination")
		}
	}
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a DataReply
func (msg *DataReply) Exec(g *Gossiper, addr *net.UDPAddr) error {
	defer g.StopWait.Done()
	if msg.Destination == g.Name {
		// if not expect this packet
		if ok := g.DownloadTrace.Contains(msg.Origin, msg.HashStr()); !ok {
			return errors.New("unexpected data packet for me")
		}
		// verify the data
		if VerifyHash(msg.Data, msg.HashValue) {
			// send the packet to the download routine
			g.DownloadTrace.Ctrls[msg.Origin][msg.HashStr()].Trigger <- &GossipPacket{DataReply: msg}
		}
	} else {
		// send to  next hop
		if msg.HopLimit > 0 {
			msg.HopLimit--
			if err := g.SendToNextHop(msg.Destination, &GossipPacket{DataReply: msg}); err != nil {
				return errors.New("No route to the destination")
			}
		}
	}
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a SearchRequest
func (msg *SearchRequest) Exec(g *Gossiper, addr *net.UDPAddr) error {
	defer g.StopWait.Done()
	// check if is a duplicate one
	if g.DuplicateSurv.CheckExist(msg.Origin, msg.Keywords) {
		fmt.Println("message duplicated")
		return nil
	}
	// register the new request
	g.DuplicateSurv.RegisterNewSearch(msg.Origin, msg.Keywords)
	// process the request locally and search for local files
	matchFiles, ok := g.IndexedFiles.SearchByKeyWords(msg.Keywords)
	if ok {
		g.SendSearchReply(matchFiles, msg.Origin)
	}
	// ring flood to peers
	msg.Budget--
	if msg.Budget > 0 {
		g.RingFlood(msg, addr.String())
	}
	return nil
}

// Exec is the function that the gossiper uses to execute the handler for a SearchReply
func (msg *SearchReply) Exec(g *Gossiper, addr *net.UDPAddr) error {
	defer g.StopWait.Done()
	if msg.Destination == g.Name {
		for _, item := range msg.Results {
			g.RemotePos.Update(msg.Origin, item.HashStr(), item.ChunkMap)
		}
		// check if it belongs to a infinite search routine
		g.SearchTrace.ContainsAndTrigger(&GossipPacket{SearchReply: msg})
	} else {
		// send to  next hop
		if msg.HopLimit > 0 {
			msg.HopLimit--
			if err := g.SendToNextHop(msg.Destination, &GossipPacket{SearchReply: msg}); err != nil {
				return errors.New("No route to the destination")
			}
		}
	}
	return nil
}
