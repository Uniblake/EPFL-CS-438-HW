// ========== CS-438 HW2 Skeleton ===========
// *** Implement here the gossiper ***
package gossip

import (
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

// UpdateRoutes implements Gossiper. It updates the current routing map
func (g *Gossiper) UpdateRoutes(msg *RumorMessage, addr *net.UDPAddr) {
	g.RouteMutex.Lock()
	defer g.RouteMutex.Unlock()
	ifNewOrigin := true
	ifUpdate := false
	if _, ok := g.routes[msg.Origin]; ok {
		ifNewOrigin = false
		if g.routes[msg.Origin].LastID < msg.ID {
			g.routes[msg.Origin].LastID = msg.ID
			g.routes[msg.Origin].NextHop = addr.String()
			ifUpdate = true
		}
	}
	if ifNewOrigin && msg.Origin != g.Name {
		ifUpdate = true
		g.routes[msg.Origin] = &RouteStruct{NextHop: addr.String(), LastID: msg.ID}
	}
	if ifUpdate {
		fmt.Printf("DSDV %s %s\n", msg.Origin, addr.String())
	}
}

// UpdatePeerStatusList implements Gossiper. It updates PeerStatusList and RumorMap
func (g *Gossiper) UpdatePeerStatusList(msg *RumorMessage) bool {
	g.StatusMutex.Lock()
	defer g.StatusMutex.Unlock()
	IfForeign := true
	NewIncome := false
	for idx, eachStatus := range g.PeerStatusList {
		if (eachStatus.Identifier == msg.Origin) && (msg.ID == eachStatus.NextID) {
			g.PeerStatusList[idx].NextID++
			g.RumorMap[msg.Origin].MessageList = append(g.RumorMap[msg.Origin].MessageList, *msg)
			NewIncome = true
		}
		if eachStatus.Identifier == msg.Origin {
			IfForeign = false
		}
	}
	if IfForeign && (msg.ID == 1) {
		newStatus := PeerStatus{
			Identifier: msg.Origin,
			NextID:     2,
		}
		g.PeerStatusList = append(g.PeerStatusList, newStatus)
		g.RumorMap[msg.Origin] = &MessageRepo{MessageList: make([]RumorMessage, 0)}
		g.RumorMap[msg.Origin].MessageList = append(g.RumorMap[msg.Origin].MessageList, *msg)
		NewIncome = true
	}
	return NewIncome
}

// GetPeerStatusList implements Gossiper. It return a new copy of PeerStatusList
func (g *Gossiper) GetPeerStatusList() []PeerStatus {
	newList := make([]PeerStatus, len(g.PeerStatusList))
	copy(newList, g.PeerStatusList)
	return newList
}

// FindMyAdvancedRumor implements Gossiper. It find the first of my advanced rumor
func (g *Gossiper) FindMyAdvancedRumor(msg *StatusPacket, myStatusCopy []PeerStatus) (bool, RumorMessage) {
	g.StatusMutex.RLock()
	defer g.StatusMutex.RUnlock()
	myNew := false // I have something new to tell the sender
	toSend := RumorMessage{}
	for _, myEachStatus := range myStatusCopy {
		myNewID := true
		// check if i have <C:n+1/2/3/...> and sender have <C:n>
		for _, inEachStatus := range msg.Want {
			if myEachStatus.Identifier == inEachStatus.Identifier {
				myNewID = false
				if myEachStatus.NextID >= inEachStatus.NextID+1 {
					myNew = true
					// toSend.Origin = myEachStatus.Identifier
					// toSend.ID = inEachStatus.NextID
					toSend = g.RumorMap[myEachStatus.Identifier].MessageList[inEachStatus.NextID-1]
					break
				}
			}
		}
		if myNewID == true {
			myNew = true
			// toSend.Origin = myEachStatus.Identifier
			// toSend.ID = 1
			toSend = g.RumorMap[myEachStatus.Identifier].MessageList[0]
			break
		}
		if myNew == true {
			break
		}
	}
	return myNew, toSend
}

// GetInAdvancedRumorList implements Gossiper. It return the list of next wanted rumor status
func (g *Gossiper) GetInAdvancedRumorList(msg *StatusPacket, myStatusCopy []PeerStatus) (bool, []PeerStatus) {
	inNew := false // I have something want from the sender
	result := make([]PeerStatus, 0)
	var wantStatus PeerStatus
	for _, inEachStatus := range msg.Want {
		inNewID := true
		// check if i have <C:n+1/2/3/...> and sender have <C:n>
		for _, myEachStatus := range myStatusCopy {
			if inEachStatus.Identifier == myEachStatus.Identifier {
				inNewID = false
				if inEachStatus.NextID >= myEachStatus.NextID+1 {
					inNew = true
					result = append(result, myEachStatus)
				}
			}
		}
		if inNewID {
			inNew = true
			wantStatus = PeerStatus{
				Identifier: inEachStatus.Identifier,
				NextID:     1,
			}
			result = append(result, wantStatus)
		}
	}
	return inNew, result
}

// FindInAdvancedRumor implements Gossiper. It find the first of income advanced rumor
func (g *Gossiper) FindInAdvancedRumor(msg *StatusPacket, myStatusCopy []PeerStatus) bool {
	inNew := false // send has something new to tell me
	for _, inEachStatus := range msg.Want {
		inNewID := true
		// check if i have <C:n+1/2/3/...> and sender have <C:n>
		for _, myEachStatus := range myStatusCopy {
			if inEachStatus.Identifier == myEachStatus.Identifier {
				inNewID = false
				if inEachStatus.NextID >= myEachStatus.NextID+1 {
					inNew = true
					break
				}
			}
		}
		if inNewID == true {
			inNew = true
			break
		}
		if inNew == true {
			break
		}
	}
	return inNew
}

// CheckGlobalAddress implements Gossiper. It check if a global gossiper already exist
func (g *Gossiper) CheckGlobalAddress(identifier string) bool {
	ifExist := false
	for _, peerStatus := range g.PeerStatusList {
		if peerStatus.Identifier == identifier {
			ifExist = true
			break
		}
	}
	return ifExist
}

// CheckNeighborAddress implements Gossiper. It check if a neighbor already exist
func (g *Gossiper) CheckNeighborAddress(address string) bool {
	ifExist := false
	for _, peerAddr := range g.Peers {
		if peerAddr.String() == address {
			ifExist = true
			break
		}
	}
	return ifExist
}

// =========================================================================
// ================Tools for rumor mongering controller=====================
// =========================================================================

// MongeringController is a controller for each living mongering routine
type MongeringController struct {
	ExpSrc       *net.UDPAddr
	ExpStatus    *PeerStatus
	ExpRumor     []PeerStatus
	IncomeRumor  *RumorMessage
	IncomeStatus *StatusPacket
	TriedPeers   []*net.UDPAddr
	Timer        *time.Ticker
	Trigger      chan int
}

// CheckIfWantRumor find if the income is wanted
func (mc *MongeringController) CheckIfWantRumor(src *net.UDPAddr, rumor *RumorMessage) bool {
	if mc.ExpSrc.String() != src.String() || len(mc.ExpRumor) == 0 {
		return false
	}
	for _, eachStatus := range mc.ExpRumor {
		if eachStatus.Identifier == rumor.Origin && eachStatus.NextID == rumor.ID {
			// this message is in wishing list
			return true
		}
	}
	return false
}

// CheckIfWantStatus find if the income is wanted
func (mc *MongeringController) CheckIfWantStatus(src *net.UDPAddr, status *StatusPacket) bool {
	if mc.ExpSrc.String() != src.String() || mc.ExpStatus == nil {
		return false
	}
	for _, eachStatus := range status.Want {
		if eachStatus.Identifier == mc.ExpStatus.Identifier && eachStatus.NextID >= mc.ExpStatus.NextID {
			return true
		}
	}
	return false
}

// UpdateExpRumor update the ExpRumor list according to current status
func (mc *MongeringController) UpdateExpRumor(myStatusCopy []PeerStatus) int {
	// make a list to store the index to delete
	rumor := mc.IncomeRumor
	DelList := make([]int, 0)
	var recvIdx int
	// delete the rumor from mc.ExpRumor
	for idx, eachWant := range mc.ExpRumor {
		if rumor.Origin == eachWant.Identifier && rumor.ID == eachWant.NextID {
			recvIdx = idx
			break
		}
	}
	DelList = append(DelList, recvIdx)
	// delete the rumor from myStatusCopy
	for i, eachWant := range mc.ExpRumor {
		IfHasRecv := false
		for _, eachStatus := range myStatusCopy {
			if eachWant.Equal(&eachStatus) {
				IfHasRecv = true
				break
			}
		}
		if IfHasRecv {
			DelList = append(DelList, i)
		}
	}
	// refresh the ExpRumor List
	newExpRumor := make([]PeerStatus, 0)
	for k, Status := range mc.ExpRumor {
		KeepAlive := true
		for _, index := range DelList {
			if k == index {
				KeepAlive = false
			}
		}
		if KeepAlive {
			newExpRumor = append(newExpRumor, Status)
		}
	}
	mc.ExpRumor = newExpRumor
	return len(mc.ExpRumor)
}

// MongeringControllerList is a list of mongering controller
type MongeringControllerList struct {
	ControllerList map[int]*MongeringController
	AccessMutex    sync.RWMutex
}

// AppendNewCtrl append the ctrl
func (mcl *MongeringControllerList) AppendNewCtrl(nc *MongeringController) int {
	mcl.AccessMutex.Lock()
	defer mcl.AccessMutex.Unlock()
	// generate a unique ssID
	var ssID int
	for {
		ssID = GenRanInt(sessionIDBase)
		if _, ok := mcl.ControllerList[ssID]; !ok {
			break
		}
	}
	mcl.ControllerList[ssID] = nc
	return ssID
}

// DeleteNilCtrl delete the nil ctrl
func (mcl *MongeringControllerList) DeleteNilCtrl(ssID int) bool {
	mcl.AccessMutex.Lock()
	defer mcl.AccessMutex.Unlock()
	if _, ok := mcl.ControllerList[ssID]; !ok {
		panic("delete ctrl not exist")
	}
	delete(mcl.ControllerList, ssID)
	return true
}

// CheckIfIsMongering check if this income packet belong to any living mongering rountine
func (mcl *MongeringControllerList) CheckIfIsMongering(
	src *net.UDPAddr, rumor *RumorMessage, status *StatusPacket) int {
	mcl.AccessMutex.Lock()
	defer mcl.AccessMutex.Unlock()
	action := 0
	var index int
	if (rumor != nil) && (status == nil) {
		// if a rumor is received
		for ssID := range mcl.ControllerList {
			ctr := mcl.ControllerList[ssID]
			if ctr.CheckIfWantRumor(src, rumor) {
				action = 1
				ctr.IncomeRumor = rumor
				ctr.IncomeStatus = nil
				index = ssID
				break
			}
		}
	} else if (rumor == nil) && (status != nil) {
		// if a status message is received
		for ssID := range mcl.ControllerList {
			ctr := mcl.ControllerList[ssID]
			if ctr.CheckIfWantStatus(src, status) {
				action = 2
				ctr.IncomeRumor = nil
				ctr.IncomeStatus = status
				index = ssID
				break
			}
		}
	}
	if action != 0 {
		// if it is expected by a living mongering routine: send a signal back
		select {
		case mcl.ControllerList[index].Trigger <- action:
			return action
		default:
			return 0
		}
	}
	return action
}

// MessageRepo store the message
type MessageRepo struct {
	MessageList []RumorMessage
}

// FindAccordingToID find a message in known message pool and return the find one.
func (mr *MessageRepo) FindAccordingToID(idx uint32) (bool, RumorMessage) {
	IfExist := false
	result := RumorMessage{
		Origin: "anonymous",
		ID:     0,
	}
	if uint32(len(mr.MessageList)) >= idx {
		IfExist = true
		result.Origin = mr.MessageList[idx-1].Origin
		result.ID = mr.MessageList[idx-1].ID
		result.Text = mr.MessageList[idx-1].Text
	}
	return IfExist, result
}
