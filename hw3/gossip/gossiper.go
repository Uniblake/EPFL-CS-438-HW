// ========== CS-438 HW3 Skeleton ===========
// *** Implement here the gossiper ***
package gossip

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/cs438/hw3/gossip/types"
	"go.dedis.ch/cs438/hw3/gossip/watcher"
	"go.dedis.ch/onet/v3/log"
)

// BaseGossipFactory provides a factory to instantiate a Gossiper
//
// - implements gossip.GossipFactory
type BaseGossipFactory struct{}

// stopMsg is used to notifier the listener when we want to close the
// connection, so that the listener knows it can stop listening.
const stopMsg = "stop"
const udpVer = "udp4"

const myChunkSize = 8192
const downloadHopLimit = 9
const searchHopLimit = 9
const duplicateLimit = 500
const typeData = "data"
const typeSearch = "search"
const metaDataSeq = 0
const hashLength = 32
const backOffBase = 2
const searchMaxExp = 5
const searchInterval = 1
const searchThreshold = 2
const maxReadBufSize = 1024 * 1024
const sessionIDBase = 100000

// New implements gossip.GossipFactory. It creates a new gossiper.
func (f BaseGossipFactory) New(address, identifier string, antiEntropy int,
	routeTimer int, rootSharedData string, rootDownloadedFiles string,
	numParticipant, nodeIndex int, paxosRetry int) (BaseGossiper, error) {

	return NewGossiper(address, identifier, antiEntropy, routeTimer, rootSharedData, rootDownloadedFiles, numParticipant, nodeIndex, paxosRetry)
}

// Gossiper provides the functionalities to handle a distributes gossip
// protocol.
//
// - implements gossip.BaseGossiper
type Gossiper struct {
	Name           string
	GossipAddr     *net.UDPAddr
	GossipConn     *net.UDPConn
	Peers          []*net.UDPAddr          // peers the gossiper can contact directly
	PeerStatusList []PeerStatus            // all the peers rumor status the gossiper knows
	RumorMap       map[string]*MessageRepo //to find ID from Origin C: RumorMap[C]
	NextSendID     uint32                  // next message ID to send from this gossiper
	MongeringTask  MongeringControllerList
	StopWait       sync.WaitGroup // use to wait all goroutine to finish using conn and close
	KnownMutex     sync.RWMutex   // use to make goroutine concurrently update and read known Peers
	StatusMutex    sync.RWMutex   // use to make goroutine concurrently update and read PeerStatusList
	IDMutex        sync.RWMutex   // use to control read and write NextSendID
	RouteMutex     sync.RWMutex   // use to control read and write routes
	AntTicker      *time.Ticker
	AntiInterval   int  // time intervel to trigger anti-entropy
	RouteInterval  int  // time intervel to send route rumor (0:disable)
	IsClosed       bool // to ask the anti-entropy to stop
	HopLimit       int

	// attr of the watch&controller
	inWatcher  watcher.Watcher
	outWatcher watcher.Watcher
	callBack   NewMessageCallback // update the new message to UI / watcher

	// routes holds the routes to different destinations. The key is the Origin,
	// or destination.
	routes   map[string]*RouteStruct
	Handlers map[reflect.Type]interface{}

	// Folder to store data about indexed files (comes from -sharedir)
	rootSharedData string
	// Folder to store downloaded files (comes from -downdir)
	rootDownloadedFiles string
	IndexedFiles        *IndexedHelper
	DownloadTrace       *DownloadInfo
	SearchTrace         *SearchInfo
	RemotePos           *RemotePossession
	MetaDataStorage     *MetaDataHelper
	DuplicateSurv       *DuplicateHelper
	stopSignal          chan uint8
	stopFinish          chan uint8
	Paxos               *PaxosHelper
}

// NewGossiper returns a Gossiper that is able to listen to the given address
// and which has the given identifier. The address must be a valid IPv4 UDP
// address. This method can panic if it is not possible to create a
// listener on that address. To run the gossip protocol, call `Run` on the
// gossiper.
func NewGossiper(address, identifier string, antiEntropy int, routeTimer int,
	rootSharedData string, rootDownloadedFiles string, numParticipant int,
	nodeIndex int, paxosRetry int) (BaseGossiper, error) {
	UDPAddress, err := net.ResolveUDPAddr(udpVer, address)
	fmt.Printf("%s to %s\n", address, UDPAddress.String())
	if err != nil {
		panic(fmt.Sprintln("Fail to resolve udp adress:", err))
	}
	// start the udp connection
	udpConn, err := net.ListenUDP(udpVer, UDPAddress)
	if err != nil {
		panic(fmt.Sprintln("Fail to establish udp connection on given address:", err))
	}
	err = udpConn.SetReadBuffer(maxReadBufSize)
	if err != nil {
		fmt.Println("fail to expand udp read buffer")
	}
	err = udpConn.SetWriteBuffer(maxReadBufSize)
	if err != nil {
		fmt.Println("fail to expand udp write buffer")
	}
	g := Gossiper{
		Name:                identifier,
		GossipAddr:          UDPAddress,
		GossipConn:          udpConn,
		NextSendID:          1,
		PeerStatusList:      make([]PeerStatus, 0),
		Peers:               make([]*net.UDPAddr, 0),
		RumorMap:            make(map[string]*MessageRepo),
		Handlers:            make(map[reflect.Type]interface{}),
		routes:              make(map[string]*RouteStruct),
		AntiInterval:        antiEntropy,
		RouteInterval:       routeTimer,
		inWatcher:           watcher.NewSimpleWatcher(),
		outWatcher:          watcher.NewSimpleWatcher(),
		StopWait:            sync.WaitGroup{},
		KnownMutex:          sync.RWMutex{},
		StatusMutex:         sync.RWMutex{},
		IDMutex:             sync.RWMutex{},
		RouteMutex:          sync.RWMutex{},
		IsClosed:            false,
		HopLimit:            10,
		rootSharedData:      rootSharedData,
		rootDownloadedFiles: rootDownloadedFiles,
		MongeringTask: MongeringControllerList{
			ControllerList: make(map[int]*MongeringController),
			AccessMutex:    sync.RWMutex{},
		},
		IndexedFiles:    &IndexedHelper{Files: make([]*File, 0)},
		DownloadTrace:   &DownloadInfo{Ctrls: make(map[string]map[string]*DownloadController)},
		SearchTrace:     &SearchInfo{Ctrls: make([]*SearchController, 0)},
		RemotePos:       &RemotePossession{PossessMap: make(map[string]map[string][]uint32)},
		MetaDataStorage: &MetaDataHelper{DataPool: make(map[string]map[string]uint32)},
		DuplicateSurv:   &DuplicateHelper{Records: make(map[string]map[uint32][]string)},
		stopSignal:      make(chan uint8),
		stopFinish:      make(chan uint8, 1),
	}
	g.InitPaxosHelper(numParticipant, paxosRetry, nodeIndex, nil)
	// try to recover from crash
	g.LoadCrashConfig()
	// check if dirs exist, if not, create them
	if _, err = CheckOrCreatePath(rootSharedData); err != nil {
		panic(fmt.Sprintln("Error in creating Dir: rootSharedData.", err))
	}
	if _, err = CheckOrCreatePath(rootDownloadedFiles); err != nil {
		panic(fmt.Sprintln("Error in creating Dir: rootDownloadedFiles.", err))
	}
	fmt.Println(g.Name, " download: ", g.rootDownloadedFiles, " shared: ", g.rootSharedData)
	// register all the handlers
	if err = g.RegisterHandler(&SimpleMessage{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&RumorMessage{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&StatusPacket{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&PrivateMessage{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&DataRequest{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&DataReply{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&SearchRequest{}); err != nil {
		log.Fatal("failed to register", err)
	}
	if err = g.RegisterHandler(&SearchReply{}); err != nil {
		log.Fatal("failed to register", err)
	}
	return &g, nil
}

// Run implements gossip.BaseGossiper. It starts the listening of UDP datagrams
// on the given address and starts the antientropy. This is a blocking function.
func (g *Gossiper) Run(ready chan struct{}) {
	buf := make([]byte, 16384)
	// close the channel and start rumor mongering and anti-entropy
	close(ready)
	// start a goroutine for periodic route rumor spread
	go g.StartRouting()
	// start a goroutine for periodic anti-entropy
	go g.StartAntiEntropy()
	// start listening to conn and solve the recerived packets
	for {
		bufSize, recvSrc, err := g.GossipConn.ReadFromUDP(buf)
		if err != nil {
			panic(fmt.Sprintln("Fail to start listen:", err))
		}
		var receivedPacket GossipPacket
		err = json.Unmarshal(buf[:bufSize], &receivedPacket)
		if err != nil {
			log.Fatal("Run when unmarshal a packet: wrong serialization: ", err)
		}
		// notify the watcher
		g.inWatcher.Notify(CallbackPacket{Addr: recvSrc.String(), Msg: receivedPacket.Copy()})
		// check and add addr to known peers
		g.AddAddresses(recvSrc.String())
		// output := "PEERS "
		// for _, eachPeer := range g.GetNodes() {
		// 	output = output + eachPeer + ","
		// }
		// fmt.Println(output)
		switch {
		case receivedPacket.Simple != nil:
			if receivedPacket.Simple.Contents == stopMsg {
				g.Paxos.StopPaxos()
				g.StopWait.Wait()
				g.GossipConn.Close()
				close(g.stopFinish)
				return
			}
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.Simple, recvSrc)
		case receivedPacket.Rumor != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.Rumor, recvSrc)
		case receivedPacket.Status != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.Status, recvSrc)
		case receivedPacket.Private != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.Private, recvSrc)
		case receivedPacket.DataRequest != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.DataRequest, recvSrc)
		case receivedPacket.DataReply != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.DataReply, recvSrc)
		case receivedPacket.SearchRequest != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.SearchRequest, recvSrc)
		case receivedPacket.SearchReply != nil:
			g.StopWait.Add(1)
			go g.ExecuteHandler(receivedPacket.SearchReply, recvSrc)
		default:
			panic("Run: Empty gossip packet found in network.")
		}
	}
}

// GenRanInt return a random int mod base
func GenRanInt(base int) int {
	rand.Seed(time.Now().UnixNano())
	result := rand.Intn(base)
	return result
}

// SendStatusMessage implements Gossiper. It sends a status message to dst
func (g *Gossiper) SendStatusMessage(dst *net.UDPAddr, statusList []PeerStatus) {
	newStatusPacket := &StatusPacket{
		Want: statusList,
	}
	newPacket := &GossipPacket{Status: newStatusPacket}
	jsonPacket, err := json.Marshal(newPacket)
	if err != nil {
		log.Fatal("SendStatusMessage: wrong serialization: ", err)
	}
	g.GossipConn.WriteToUDP(jsonPacket, dst)
	g.outWatcher.Notify(CallbackPacket{Addr: dst.String(), Msg: *newPacket})
}

// SendRumorMessage implements Gossiper. It sends a rumor message to dst
func (g *Gossiper) SendRumorMessage(dst *net.UDPAddr, rumor *RumorMessage) {
	newPacket := &GossipPacket{Rumor: rumor}
	jsonPacket, err := json.Marshal(newPacket)
	if err != nil {
		log.Fatal("SendRumorMessage: wrong serialization: ", err)
	}
	g.GossipConn.WriteToUDP(jsonPacket, dst)
	g.outWatcher.Notify(CallbackPacket{Addr: dst.String(), Msg: *newPacket})
	// fmt.Printf("MONGERING with %s\n", dst.String())
}

// BroadcastRumorForPaxos implements gossiper.
func (g *Gossiper) BroadcastRumorForPaxos(msg *RumorMessage) {
	g.UpdatePeerStatusList(msg)
	packet := GossipPacket{Rumor: msg}.Copy()
	jsonPacket, err := json.Marshal(&packet)
	if err != nil {
		log.Fatal("AddPrivateMessage: wrong serialization: ", err)
	}
	for len(g.Peers) == 0 {
		time.Sleep(time.Millisecond * time.Duration(20))
	}
	g.KnownMutex.RLock()
	defer g.KnownMutex.RUnlock()
	for idx, peerAddr := range g.Peers {
		g.GossipConn.WriteToUDP(jsonPacket, peerAddr)
		if idx == 0 {
			g.outWatcher.Notify(CallbackPacket{Addr: g.Peers[0].String(), Msg: packet.Copy()})
		}

	}
}

// StartRouting implements Gossiper. It start route rumor spreading.
func (g *Gossiper) StartRouting() {
	// if it is disabled
	if g.RouteInterval == 0 {
		return
	}
	// set the ticker
	ticker := time.NewTicker(time.Duration(g.RouteInterval) * time.Second)
	// send start-up route rumor
	g.SendRouteMessage()
	for {
		select {
		case <-g.stopSignal:
			return
		case <-ticker.C:
			g.SendRouteMessage()
		}
	}
}

// SendRouteMessage send a route message
func (g *Gossiper) SendRouteMessage() bool {
	// send the start-up route rumor
	success := true
	var routeRumor RumorMessage
	var routePacket GossipPacket
	g.KnownMutex.RLock()
	length := len(g.Peers)
	if length != 0 {
		// choose one dst
		choose := GenRanInt(length)
		newSrc := g.Peers[choose]
		g.KnownMutex.RUnlock()
		// construct the route rumor packet
		g.IDMutex.Lock()
		routeRumor = RumorMessage{Origin: g.Name, ID: g.NextSendID}
		g.NextSendID++
		g.IDMutex.Unlock()
		routePacket = GossipPacket{Rumor: &routeRumor}
		jsonPacket, err := json.Marshal(&routePacket)
		if err != nil {
			log.Fatal("fail to send route message, wrong serialization: ", err)
		}
		g.GossipConn.WriteToUDP(jsonPacket, newSrc)
	} else {
		g.KnownMutex.RUnlock()
		success = false
	}
	return success
}

// StartAntiEntropy implements Gossiper. It start anti entropy.
func (g *Gossiper) StartAntiEntropy() {
	// set a ticker for anti-entropy
	ticker := time.NewTicker(time.Duration(g.AntiInterval) * time.Second)
	for {
		select {
		case <-g.stopSignal:
			// fmt.Printf("%s stop anti entropy\n", g.Name)
			return
		case <-ticker.C:
			// construct the status packet
			g.StatusMutex.RLock()
			currentStatus := g.GetPeerStatusList()
			g.StatusMutex.RUnlock()
			statusPacket := &StatusPacket{Want: currentStatus}
			packet := &GossipPacket{Status: statusPacket}
			jsonPacket, err := json.Marshal(packet)
			if err != nil {
				log.Fatal("fail to run anti-entropy, wrong serialization: ", err)
			}
			// randomly choose a gossiper and send the status packet
			g.KnownMutex.RLock()
			length := len(g.Peers)
			if length != 0 {
				choose := GenRanInt(length)
				newSrc := g.Peers[choose]
				g.KnownMutex.RUnlock()
				g.GossipConn.WriteToUDP(jsonPacket, newSrc)
			} else {
				g.KnownMutex.RUnlock()
			}
		}
	}
}

// StartRumorMongering implements Gossiper. It start a new rumor mongering task.
func (g *Gossiper) StartRumorMongering(msg *RumorMessage, msgSrc *net.UDPAddr) {
	var stop int
	var success bool
	// create a new mongering routine
	newTc := time.NewTicker(time.Duration(10) * time.Second)
	newCtr := &MongeringController{
		ExpStatus:  nil,
		ExpRumor:   make([]PeerStatus, 0),
		Timer:      newTc,
		Trigger:    make(chan int),
		TriedPeers: make([]*net.UDPAddr, 0),
	}
	// append the new task to task list
	nssID := g.MongeringTask.AppendNewCtrl(newCtr)
	// defer func() {
	// 	fmt.Printf("<<%s>>Length of living mongering list 2: %v\n", g.Name, len(g.MongeringTask.ControllerList))
	// }()
	defer g.MongeringTask.DeleteNilCtrl(nssID)
	// defer func() {
	// 	g.MongeringTask.AccessMutex.Lock()
	// 	defer g.MongeringTask.AccessMutex.Unlock()
	// 	newCtr = nil
	// }()
	// append the src of the sender to tried (avoid loop)
	if msgSrc != nil {
		newCtr.TriedPeers = append(newCtr.TriedPeers, msgSrc)
	}
	// choose a random Peer and send
	success, _ = g.RandomlySelectAndSend(newCtr, msg)
	if !success {
		return
	}
	for {
		select {
		case <-g.stopSignal:
			// fmt.Printf("%s stop rumor mongering\n", g.Name)
			return
		case <-newCtr.Timer.C:
			// time is end, choose a random Peer and send
			if success, _ = g.RandomlySelectAndSend(newCtr, msg); !success {
				return
			}
		case action := <-newCtr.Trigger:
			// if a new message found belong to this rountine
			currentStatus := g.GetPeerStatusList()
			if action == 1 {
				// expected rumor received
				// update the ExpRumor list according to this rumor and g.PeerStatusList
				WantNum := newCtr.UpdateExpRumor(currentStatus)
				g.SendStatusMessage(newCtr.ExpSrc, currentStatus)
				newCtr.ExpStatus = nil
				if WantNum == 0 {
					// finished, choose to stop or another
					stop = GenRanInt(2)
					if stop == 1 {
						// choose to stop
						return
					}
					// choose a new gossiper to mongering
					success, _ := g.RandomlySelectAndSend(newCtr, msg)
					if !success {
						// fmt.Println("all have tried and mongering stop: ", goid())
						return
					}
					// fmt.Printf("FLIPPED COIN sending rumor to %s\n", RandSrc.String())
				}
			} else if action == 2 {
				// expected status received
				IfHaveAdvanced, rumorToSend := g.FindMyAdvancedRumor(newCtr.IncomeStatus, currentStatus)
				if IfHaveAdvanced {
					// I have something new to send
					g.SendRumorMessage(newCtr.ExpSrc, &rumorToSend)
					// update the expected message
					newCtr.ExpStatus = &PeerStatus{
						Identifier: rumorToSend.Origin,
						NextID:     rumorToSend.ID + 1,
					}
					newCtr.ExpRumor = make([]PeerStatus, 0)
				} else {
					IfWantAdvanced, AdvanceList := g.GetInAdvancedRumorList(newCtr.IncomeStatus, currentStatus)
					if IfWantAdvanced {
						// I want something from the sender
						g.SendStatusMessage(newCtr.ExpSrc, currentStatus)
						newCtr.ExpRumor = AdvanceList
						newCtr.ExpStatus = nil
					} else {
						// neither I have something new or want something
						// flips a coin to stop or proceed
						stop = GenRanInt(2)
						if stop == 1 {
							// choose to stop
							return
						}
						// choose a new gossiper to mongering
						success, _ := g.RandomlySelectAndSend(newCtr, msg)
						if !success {
							// fmt.Println("all have tried and mongering stop: ", goid())
							return
						}
						// fmt.Printf("FLIPPED COIN sending rumor to %s\n", RandSrc.String())
					}
				}
			}
		}
	}
}

// RandomlySelectAndSend implements Gossiper. It choose a random src and send and update metadata
func (g *Gossiper) RandomlySelectAndSend(newCtr *MongeringController, msg *RumorMessage) (bool, *net.UDPAddr) {
	var IfContacted, success bool
	var newSrc *net.UDPAddr
	contacted := newCtr.TriedPeers
	remained := make([]*net.UDPAddr, 0)
	g.KnownMutex.RLock()
	for _, known := range g.Peers {
		IfContacted = false
		for _, tried := range contacted {
			if known.String() == tried.String() {
				IfContacted = true
			}
		}
		if !IfContacted {
			remained = append(remained, known)
		}
	}
	g.KnownMutex.RUnlock()
	length := len(remained)
	if length > 0 {
		success = true
		selected := GenRanInt(len(remained))
		newSrc = remained[selected]
		newCtr.TriedPeers = append(newCtr.TriedPeers, newSrc)
	} else {
		success = false
		newSrc = nil
	}
	if success {
		g.SendRumorMessage(newSrc, msg)
		// update the expected message
		newCtr.ExpStatus = &PeerStatus{
			Identifier: msg.Origin,
			NextID:     msg.ID + 1,
		}
		newCtr.ExpRumor = make([]PeerStatus, 0)
		newCtr.ExpSrc = newSrc
	}
	return success, newSrc
}

// Stop implements gossip.BaseGossiper. It closes the UDP connection
func (g *Gossiper) Stop() {
	newMessage := SimpleMessage{
		OriginPeerName: g.Name,
		RelayPeerAddr:  g.GossipAddr.String(),
		Contents:       stopMsg,
	}
	newPacket := &GossipPacket{Simple: &newMessage}
	jsonPacket, err := json.Marshal(newPacket)
	if err != nil {
		log.Fatal("Stop: wrong serialization: ", err)
	}
	close(g.stopSignal)
	addr, _ := net.ResolveUDPAddr(udpVer, g.GossipConn.LocalAddr().String())
	ticker := time.NewTicker(2 * time.Second)
	for {
		if _, err = g.GossipConn.WriteToUDP(jsonPacket, addr); err != nil {
			log.Error("failed to send stop packet to local listener", "-", err)
		}
		select {
		case <-ticker.C:
			fmt.Println("stopMsg lost, resend stopMsg after 2 seconds")
		case <-g.stopFinish:
			g.GenerateCrashConfig()
			fmt.Printf("%s successfully stopped\n", g.Name)
			return
		}
	}
}

// AddSimpleMessage implements gossip.BaseGossiper. It takes a text that will be
// spread through the gossip network with the identifier of g.
func (g *Gossiper) AddSimpleMessage(text string) {
	// standard output the message from the client
	fmt.Println("CLIENT MESSAGE ", text)
	// send the message to other gossipers
	newMessage := SimpleMessage{
		OriginPeerName: g.Name,
		RelayPeerAddr:  g.GossipAddr.String(),
		Contents:       text,
	}
	newPacket := &GossipPacket{Simple: &newMessage}
	jsonPacket, err := json.Marshal(newPacket)
	if err != nil {
		log.Fatal("AddSimpleMessage wrong serialization: ", err)
	}
	currentPeers := g.GetNodes()
	for _, peerAddr := range currentPeers {
		UDPAddress, err := net.ResolveUDPAddr(udpVer, peerAddr)
		if err != nil {
			panic(fmt.Sprintln("AddSimpleMessage Fail to resolve udp adress:", err))
		}
		g.GossipConn.WriteToUDP(jsonPacket, UDPAddress)
	}
}

// AddMessage takes a text that will be spread through the gossip network
// with the identifier of g. It returns the ID of the message
func (g *Gossiper) AddMessage(text string) uint32 {
	g.IDMutex.Lock()
	newRumor := &RumorMessage{
		Origin: g.Name,
		ID:     g.NextSendID,
		Text:   text,
	}
	g.NextSendID++
	g.IDMutex.Unlock()
	// need to add this to PeerStatusList
	IfNew := g.UpdatePeerStatusList(newRumor)
	if IfNew {
		// fmt.Println("A new client message received start mongering: ", goid())
	} else {
		panic("wrong in client message update.")
	}
	g.StartRumorMongering(newRumor, nil)
	return newRumor.ID
}

// AddPrivateMessage sends the message to the next hop.
func (g *Gossiper) AddPrivateMessage(text, dest, origin string, hoplimit int) {
	copyRoutes := g.GetRoutingTable()
	nextStruct, ok := copyRoutes[dest]
	if ok {
		nextAddr, err := net.ResolveUDPAddr(udpVer, nextStruct.NextHop)
		if err != nil {
			panic(fmt.Sprintln("AddPrivateMessage Exec: Fail to resolve udp adress:", err))
		}
		msg := &PrivateMessage{
			Origin:      origin,
			ID:          0,
			Text:        text,
			Destination: dest,
			HopLimit:    hoplimit,
		}
		packet := &GossipPacket{Private: msg}
		jsonPacket, err := json.Marshal(packet)
		if err != nil {
			log.Fatal("AddPrivateMessage: wrong serialization: ", err)
		}
		g.GossipConn.WriteToUDP(jsonPacket, nextAddr)
	}
}

// BroadcastMessage implements gossip.BaseGossiper.
func (g *Gossiper) BroadcastMessage(msg GossipPacket) {
	g.KnownMutex.RLock()
	defer g.KnownMutex.RUnlock()
	for _, peerAddr := range g.Peers {
		jsonPacket, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal("AddPrivateMessage: wrong serialization: ", err)
		}
		g.GossipConn.WriteToUDP(jsonPacket, peerAddr)
		g.outWatcher.Notify(CallbackPacket{Addr: peerAddr.String(), Msg: msg})
	}
}

// AddAddresses implements gossip.BaseGossiper. It takes any number of node
// addresses that the gossiper can contact in the gossiping network.
func (g *Gossiper) AddAddresses(addresses ...string) error {
	g.KnownMutex.Lock()
	defer g.KnownMutex.Unlock()
	for _, addString := range addresses {
		// if the address not exist, add to Peers
		if !g.CheckNeighborAddress(addString) {
			udpAddress, err := net.ResolveUDPAddr(udpVer, addString)
			if err != nil {
				panic(fmt.Sprintln("AddAddresses: Fail to resolve udp adress:", err))
			}
			g.Peers = append(g.Peers, udpAddress)
		}
	}
	return nil
}

// GetNodes implements gossip.BaseGossiper. It returns the list of nodes this
// gossiper knows currently in the network.
func (g *Gossiper) GetNodes() []string {
	g.KnownMutex.RLock()
	defer g.KnownMutex.RUnlock()
	nodeList := make([]string, 0)
	for _, peerAddr := range g.Peers {
		nodeList = append(nodeList, peerAddr.String())
	}
	return nodeList
}

// GetDirectNodes implements gossip.BaseGossiper. It returns the list of nodes whose routes are known to this node
func (g *Gossiper) GetDirectNodes() []string {
	g.RouteMutex.RLock()
	defer g.RouteMutex.RUnlock()
	knownNodes := make([]string, 0)
	for key := range g.routes {
		knownNodes = append(knownNodes, key)
	}
	return knownNodes
}

// SetIdentifier implements gossip.BaseGossiper. It changes the identifier sent
// with messages originating from this gossiper.
func (g *Gossiper) SetIdentifier(id string) {
	g.Name = id
}

// GetIdentifier implements gossip.BaseGossiper. It returns the currently used
// identifier for outgoing messages from this gossiper.
func (g *Gossiper) GetIdentifier() string {
	return g.Name
}

// GetRoutingTable implements gossip.BaseGossiper. It returns the known routes.
func (g *Gossiper) GetRoutingTable() map[string]*RouteStruct {
	g.RouteMutex.RLock()
	defer g.RouteMutex.RUnlock()
	copyRoutes := make(map[string]*RouteStruct)
	for key := range g.routes {
		copyRoutes[key] = &RouteStruct{LastID: g.routes[key].LastID, NextHop: g.routes[key].NextHop}
	}
	return copyRoutes
}

// GetLocalAddr implements gossip.BaseGossiper. It returns the address
// (ip:port as a string) currently used to send to and receive messages
// from other peers.
func (g *Gossiper) GetLocalAddr() string {
	// Here is an implementation example that assumes storing the connection in the gossiper.
	// Adjust it to your implementation.
	return g.GossipConn.LocalAddr().String()
}

// AddRoute updates the gossiper's routing table by adding a next hop for the given
// peer node
func (g *Gossiper) AddRoute(peerName, nextHop string) {
	g.RouteMutex.Lock()
	defer g.RouteMutex.Unlock()
	g.routes[peerName] = &RouteStruct{
		NextHop: nextHop,
		LastID:  0,
	}
}

// RegisterCallback implements gossip.BaseGossiper. It sets the callback that
// must be called each time a new message arrives.
func (g *Gossiper) RegisterCallback(m NewMessageCallback) {
	g.callBack = m
}

// AddSearchMessage broadcasts search requests and wait for responses.
func (g *Gossiper) AddSearchMessage(origin string, budget uint64, keywords []string) {
	g.SearchAndWaitForReply(origin, keywords, budget)
}

// GetRemoteChunksPossession returns locally stored information (obtained via search) about which chunks each node has for
// the given file metahash. The key of the return map is a peer's ID and the value is a list of chunk IDs.
func (g *Gossiper) GetRemoteChunksPossession(metaHash string) map[string][]uint32 {
	return g.RemotePos.GetPossessByHash(metaHash)
}

// IndexShares indexes the files specified in filenames
func (g *Gossiper) IndexShares(shares string) {
	fileList := strings.Split(shares, ",")
	for _, share := range fileList {
		fmt.Println("======> Start indexing: ", share)
		ok, metaHash, idList, metaContent := g.IndexOneFile(string(share))
		if !ok {
			fmt.Println("======> !!!Conflict Detected!!!: ", share)
			return
		}
		fmt.Println("======> Successfully indexing: ", share)
		fmt.Println("======> Start Proposing consensus on: ", share)
		if success := g.ProposeUntilConsensus(metaHash, share); !success {
			fmt.Println("======> !!!Conflict Detected!!!: ", share)
			return
		}
		fmt.Println("======> Successfully Obtaining consensus on : ", share)
		// store the meta info in the struct
		metaHashStr := hex.EncodeToString(metaHash)
		newFile := &File{MetaHash: metaHashStr, Name: share}
		g.IndexedFiles.Update(newFile)
		g.RemotePos.Update(g.Name, metaHashStr, idList)
		g.MetaDataStorage.Update(metaHashStr, metaContent, idList)
	}
}

// GetIndexedFiles returns a list of Files currently indexed by the gossiper
func (g *Gossiper) GetIndexedFiles() []*File {
	return g.IndexedFiles.GetFiles()
}

// RemoveChunkFromLocal removes the chunk with the given hash from the local index of available chunks.
func (g *Gossiper) RemoveChunkFromLocal(chunkHash string) {
	metaHash, delIdx := g.MetaDataStorage.Remove(chunkHash)
	g.RemotePos.Remove(g.Name, metaHash, delIdx)
	fmt.Printf("%s successully remove %s, %+v\n", g.Name, metaHash, delIdx)
}

// RetrieveMetaFile Retrieves the metafile with the given hash from the dest node
func (g *Gossiper) RetrieveMetaFile(dest, hash string) ([]byte, error) {
	// if get random dest according to last search
	var dataPacket *DataReply
	var err error = errors.New("empty")
	if dest == "" {
		for err != nil {
			dest, err = g.RemotePos.GetDestByPastSearch(hash, metaDataSeq)
		}
	}
	// get the MetaData
	dataPacket, err = g.SendAndWaitForReply(dest, hash, typeData, metaDataSeq)
	return dataPacket.Data, err
}

// DownloadFile firsts requests a metafile of a given file and then retrieves it
// chunk by chunk
func (g *Gossiper) DownloadFile(dest, filename, metahash string) {
	var err error
	var i uint32
	var inDest string = dest
	MetaData, err := g.RetrieveMetaFile(inDest, metahash)
	if err == nil {
		// create new folder under download
		thisDir := fmt.Sprintf("%s/%s/", g.rootDownloadedFiles, metahash)
		if _, err := CheckOrCreatePath(thisDir); err == nil {
			thisFile := fmt.Sprintf("%sMetaFile", thisDir)
			// write the MetaFile
			if err = WriteOneChunk(MetaData, thisFile); err == nil {
				// update the metaData storage to indicate i have metaData
				g.MetaDataStorage.RegisterMetaData(metahash)
				// analyze the MetaData
				dataBlockNum := uint32(len(MetaData) / hashLength)
				fileContent := make([]byte, 0)
				for i = 0; i < dataBlockNum; i++ {
					// request the following chunks
					thisHasStr := hex.EncodeToString(MetaData[i*hashLength : (i+1)*hashLength])
					if inDest == "" {
						dest, err = g.RemotePos.GetDestByPastSearch(metahash, i)
						for err != nil {
							dest, err = g.RemotePos.GetDestByPastSearch(metahash, i)
						}
					}
					fmt.Printf("download: ask <%s> for ID <%v>\n", dest, i)
					dataPacket, err := g.SendAndWaitForReply(dest, thisHasStr, typeData, i)
					if err == nil {
						chunkName := fmt.Sprintf("%schunk%d", thisDir, i)
						if err = WriteOneChunk(dataPacket.Data, chunkName); err != nil {
							panic(fmt.Sprintln("DownloadFile: error in write chunk.", err))
						}
						fileContent = append(fileContent, dataPacket.Data...)
						// update my current possession
						g.RemotePos.Add(g.Name, metahash, uint32(i))
						g.MetaDataStorage.Add(metahash, thisHasStr, i)
						// start sharing this file
						if i == 0 {
							g.IndexedFiles.Update(&File{Name: filename, MetaHash: metahash})
						}
					} else {
						panic(fmt.Sprintln("DownloadFile: fail to retrieve DataChunk.", err))
					}
					fmt.Println("Download: ===========> success for chunk: ", i)
				}
				// reconstruct the file
				WriteOneChunk(fileContent, fmt.Sprintf("%s/%s", g.rootDownloadedFiles, filename))
			} else {
				panic(fmt.Sprintln("DownloadFile: fail to write MetaFile.", err))
			}
		} else {
			panic(fmt.Sprintln("DownloadFile: fail to create a new folder.", err))
		}
	} else {
		panic(fmt.Sprintln("DownloadFile: fail to retrieve MetaFile.", err))
	}
}

// GetBlocks returns all the blocks added so far. Key should be hexadecimal
func (g *Gossiper) GetBlocks() (string, map[string]types.Block) {
	return g.Paxos.Blocks.GetChain()
}

// Watch implements gossip.BaseGossiper. It returns a chan populated with new
// incoming packets
func (g *Gossiper) Watch(ctx context.Context, fromIncoming bool) <-chan CallbackPacket {
	w := g.inWatcher

	if !fromIncoming {
		w = g.outWatcher
	}

	o := &observer{
		ch: make(chan CallbackPacket),
	}

	w.Add(o)

	go func() {
		<-ctx.Done()
		// empty the channel
		o.terminate()
		w.Remove(o)
	}()

	return o.ch
}

// - implements watcher.observable
type observer struct {
	sync.Mutex
	ch      chan CallbackPacket
	buffer  []CallbackPacket
	closed  bool
	running bool
}

func (o *observer) Notify(i interface{}) {
	o.Lock()
	defer o.Unlock()

	if o.closed {
		return
	}

	if o.running {
		o.buffer = append(o.buffer, i.(CallbackPacket))
		return
	}

	select {
	case o.ch <- i.(CallbackPacket):

	default:
		// The buffer size is not controlled as we assume the event will be read
		// shortly by the caller.
		o.buffer = append(o.buffer, i.(CallbackPacket))

		o.checkSize()

		o.running = true

		go o.run()
	}
}

func (o *observer) run() {
	for {
		o.Lock()

		if len(o.buffer) == 0 {
			o.running = false
			o.Unlock()
			return
		}

		msg := o.buffer[0]
		o.buffer = o.buffer[1:]

		o.Unlock()

		// Wait for the channel to be available to writings.
		o.ch <- msg
	}
}

func (o *observer) checkSize() {
	const warnLimit = 1000
	if len(o.buffer) >= warnLimit {
		log.Warn("Observer queue is growing insanely")
	}
}

func (o *observer) terminate() {
	o.Lock()
	defer o.Unlock()

	o.closed = true

	if o.running {
		o.running = false
		o.buffer = nil

		// Drain the message in transit to close the channel properly.
		select {
		case <-o.ch:
		default:
		}
	}

	close(o.ch)
}

// An example of how to send an incoming packet to the Watcher
// g.inWatcher.Notify(CallbackPacket{Addr: addrStr, Msg: gossipPacket.Copy()})
