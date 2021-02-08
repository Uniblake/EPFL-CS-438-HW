// ========== CS-438 HW3 Skeleton ===========
// Define the packet structs here.
package gossip

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"go.dedis.ch/cs438/hw3/gossip/types"
)

// GetFactory returns the Gossip factory
func GetFactory() GossipFactory {
	return BaseGossipFactory{}
}

// GossipPacket defines the packet that gets encoded or deserialized from the
// network.
type GossipPacket struct {
	Simple        *SimpleMessage  `json:"simple"`
	Rumor         *RumorMessage   `json:"rumor"`
	Status        *StatusPacket   `json:"status"`
	Private       *PrivateMessage `json:"private"`
	DataRequest   *DataRequest    `json:"datarequest"`
	DataReply     *DataReply      `json:"datareply"`
	SearchRequest *SearchRequest  `json:"searchrequest"`
	SearchReply   *SearchReply    `json:"searchreply"`
}

// Copy performs a deep copy of the GossipPacket. When we use the watcher, it is
// best not to give a pointer to the original packet, as it could create some
// race.
func (g GossipPacket) Copy() GossipPacket {
	var simple *SimpleMessage
	var rumor *RumorMessage
	var status *StatusPacket
	var private *PrivateMessage
	var dataRequest *DataRequest
	var dataReply *DataReply
	var searchRequest *SearchRequest
	var searchReply *SearchReply

	if g.Simple != nil {
		simple = new(SimpleMessage)
		simple.OriginPeerName = g.Simple.OriginPeerName
		simple.RelayPeerAddr = g.Simple.RelayPeerAddr
		simple.Contents = g.Simple.Contents
	}

	if g.Rumor != nil {
		rumor = new(RumorMessage)
		rumor.Origin = g.Rumor.Origin
		rumor.ID = g.Rumor.ID
		rumor.Text = g.Rumor.Text

		if g.Rumor.Extra != nil {
			rumor.Extra = g.Rumor.Extra.Copy()
		}
	}

	if g.Status != nil {
		status = new(StatusPacket)
		status.Want = append([]PeerStatus{}, g.Status.Want...)
	}

	if g.Private != nil {
		private = new(PrivateMessage)
		private.Destination = g.Private.Destination
		private.HopLimit = g.Private.HopLimit
		private.ID = g.Private.ID
		private.Origin = g.Private.Origin
		private.Text = g.Private.Text
	}

	if g.DataRequest != nil {
		dataRequest = new(DataRequest)
		dataRequest.Origin = g.DataRequest.Origin
		dataRequest.Destination = g.DataRequest.Destination
		dataRequest.HopLimit = g.DataRequest.HopLimit
		dataRequest.HashValue = g.DataRequest.HashValue
	}

	if g.DataReply != nil {
		dataReply = new(DataReply)
		dataReply.Origin = g.DataReply.Origin
		dataReply.Destination = g.DataReply.Destination
		dataReply.HopLimit = g.DataReply.HopLimit
		dataReply.HashValue = g.DataReply.HashValue
		dataReply.Data = g.DataReply.Data
	}

	if g.SearchRequest != nil {
		searchRequest = new(SearchRequest)
		searchRequest.Origin = g.SearchRequest.Origin
		searchRequest.Budget = g.SearchRequest.Budget
		searchRequest.Keywords = g.SearchRequest.Keywords
	}

	if g.SearchReply != nil {
		searchReply = new(SearchReply)
		searchReply.Origin = g.SearchReply.Origin
		searchReply.Destination = g.SearchReply.Destination
		searchReply.HopLimit = g.SearchReply.HopLimit
		searchReply.Results = g.SearchReply.Results
	}

	return GossipPacket{
		Simple:        simple,
		Rumor:         rumor,
		Status:        status,
		Private:       private,
		DataRequest:   dataRequest,
		DataReply:     dataReply,
		SearchRequest: searchRequest,
		SearchReply:   searchReply,
	}
}

// SimpleMessage is a structure for the simple message
type SimpleMessage struct {
	OriginPeerName string `json:"originPeerName"`
	RelayPeerAddr  string `json:"relayPeerAddr"`
	Contents       string `json:"contents"`
}

// RumorMessage denotes of an actual message originating from a given Peer in the network.
type RumorMessage struct {
	Origin string `json:"origin"`
	ID     uint32 `json:"id"`
	Text   string `json:"text"`

	Extra *types.ExtraMessage `json:"extra"`
}

// Equal check if equal
func (rl *RumorMessage) Equal(rr *RumorMessage) bool {
	return rl.Origin == rr.Origin && rl.ID == rr.ID && rl.Text == rr.Text
}

// Copy a new rumor
func (rl *RumorMessage) Copy() RumorMessage {
	return RumorMessage{
		Origin: rl.Origin,
		ID:     rl.ID,
		Text:   rl.Text,
		Extra:  rl.Extra.Copy(),
	}
}

// StatusPacket is sent as a status of the current local state of messages seen
// so far. It can start a rumormongering process in the network.
type StatusPacket struct {
	Want []PeerStatus `json:"want"`
}

// PeerStatus shows how far have a node see messages coming from a peer in
// the network.
type PeerStatus struct {
	Identifier string `json:"identifier"`
	NextID     uint32 `json:"nextid"`
}

// Equal check if equal
func (rl *PeerStatus) Equal(rr *PeerStatus) bool {
	return rl.Identifier == rr.Identifier && rl.NextID == rr.NextID
}

// RouteStruct to hold the routes of other nodes. The Origin (Destination)
// is the key of the routes-map.
type RouteStruct struct {
	// NextHop is the address of the forwarding peer
	NextHop string
	// LastID is the sequence number
	LastID uint32
}

// PrivateMessage is sent privately to one peer
type PrivateMessage struct {
	Origin      string `json:"origin"`
	ID          uint32 `json:"id"`
	Text        string `json:"text"`
	Destination string `json:"destination"`
	HopLimit    int    `json:"hoplimit"`
}

// CallbackPacket describes the content of a callback
type CallbackPacket struct {
	Addr string
	Msg  GossipPacket
}

// File struct represent a local file indexed to be searchable by other peers
type File struct {
	Name     string
	MetaHash string
}

// DataRequest is a message struct for requesting a specific data block with a given hash
type DataRequest struct {
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
	HopLimit    uint32 `json:"hoplimit"`
	HashValue   []byte `json:"hashvalue"`
}

// HashStr return the HashValue in str form
func (d *DataRequest) HashStr() string {
	return hex.EncodeToString(d.HashValue)
}

// DataReply is a reply to data request with mirrored hash value and the actual data
type DataReply struct {
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
	HopLimit    uint32 `json:"hoplimit"`
	HashValue   []byte `json:"hashvalue"`
	Data        []byte `json:"data"`
}

// HashStr return the HashValue in str form
func (d *DataReply) HashStr() string {
	return hex.EncodeToString(d.HashValue)
}

// SearchRequest is a serach for files by keywords
type SearchRequest struct {
	Origin   string   `json:"origin"`
	Budget   uint64   `json:"budget"`
	Keywords []string `json:"keywords"`
}

// SearchReply is a reply for file search by keyword
type SearchReply struct {
	Origin      string          `json:"origin"`
	Destination string          `json:"destination"`
	HopLimit    uint32          `json:"hoplimit"`
	Results     []*SearchResult `json:"results"`
}

// SearchResult is a reply to a keyword search, containing for a given filename result the
// hash of the metafile and the chunks for that file that the peer who sent that reply stores.
type SearchResult struct {
	FileName string   `json:"filename"`
	MetaHash []byte   `json:"metahash"`
	ChunkMap []uint32 `json:"chunkmap"`
}

// HashStr return the MetaHash in str form
func (r *SearchResult) HashStr() string {
	return hex.EncodeToString(r.MetaHash)
}

func (c CallbackPacket) String() string {
	res := new(strings.Builder)
	res.WriteString("CallbackPacket: ")

	fmt.Fprintf(res, "{ addr: %s ", c.Addr)
	if c.Msg.Private != nil {
		fmt.Fprintf(res, "Private: %v", *c.Msg.Private)
	}
	if c.Msg.Rumor != nil {
		fmt.Fprintf(res, "Rumor: %v", *c.Msg.Rumor)
	}
	if c.Msg.Simple != nil {
		fmt.Fprintf(res, "Simple: %v", *c.Msg.Simple)
	}
	if c.Msg.Status != nil {
		fmt.Fprintf(res, "Status: %v", *c.Msg.Status)
	}
	if c.Msg.DataRequest != nil {
		fmt.Fprintf(res, "Data Request: %v", *c.Msg.DataRequest)
	}
	if c.Msg.DataReply != nil {
		fmt.Fprintf(res, "Data Reply: %v", *c.Msg.DataReply)
	}
	if c.Msg.SearchRequest != nil {
		fmt.Fprintf(res, "Search Request: %v", *c.Msg.SearchRequest)
	}
	if c.Msg.SearchReply != nil {
		fmt.Fprintf(res, "Search Reply: %v", *c.Msg.SearchReply)
	}
	res.WriteString("} ")

	return res.String()
}

// NewMessageCallback is the type of function that users of the library should
// provide to get a feedback on new messages detected in the gossip network.
type NewMessageCallback func(origin string, message GossipPacket)

// GossipFactory provides the primitive to instantiate a new Gossiper
type GossipFactory interface {
	New(address, identifier string, antiEntropy int, routeTimer int,
		rootSharedData string, rootDownloadedFiles string, numParticipant int,
		nodeIndex, paxosRetry int) (BaseGossiper, error)
}

// BaseGossiper ...
type BaseGossiper interface {
	BroadcastMessage(GossipPacket)
	RegisterHandler(handler interface{}) error
	// GetNodes returns the list of nodes this gossiper knows currently in the
	// network.
	GetNodes() []string
	// GetDirectNodes returns the list of nodes this gossiper knows  in its routing table
	GetDirectNodes() []string
	// SetIdentifier changes the identifier sent with messages originating from this
	// gossiper.
	SetIdentifier(id string)
	// GetIdentifier returns the currently used identifier for outgoing messages from
	// this gossiper.
	GetIdentifier() string
	// AddSimpleMessage takes a text that will be spread through the gossip network
	// with the identifier of g. It returns the ID of the message
	AddSimpleMessage(text string)
	// AddMessage takes a text that will be spread through the gossip network
	// with the identifier of g. It returns the ID of the message
	AddMessage(text string) uint32
	// AddPrivateMessage
	AddPrivateMessage(text string, dest string, origin string, hoplimit int)
	// AddAddresses takes any number of node addresses that the gossiper can contact
	// in the gossiping network.
	AddAddresses(addresses ...string) error
	// AddRoute updates the gossiper's routing table by adding a next hop for the given
	// peer node
	AddRoute(peerName, nextHop string)
	// RegisterCallback registers a callback needed by the controller to update
	// the view.
	RegisterCallback(NewMessageCallback)
	// Run creates the UPD connection and starts the gossiper. This function is
	// assumed to be blocking until Stop is called. The ready chan should be
	// closed when the Gossiper is started.
	Run(ready chan struct{})
	// Stop stops the Gossiper
	Stop()
	// Watch returns a chan that is populated with new incoming packets if
	// fromIncoming is true, otherwise from sent messages.
	Watch(ctx context.Context, fromIncoming bool) <-chan CallbackPacket
	// GetRoutingTable returns the routing table of the node.
	GetRoutingTable() map[string]*RouteStruct
	// GetLocalAddr returns the local address (ip:port) used for sending and receiving packets to/from the network.
	GetLocalAddr() string
	// IndexShares takes a list of files and indexes them in the gossiper. For each file, it generates the metafile,
	// computes its hash, and makes the gossiper of the filename and the hashes (of the metafile and the chunks) that
	// might be requested. It expect the file to be present in the current working directory: os.Getwd()
	IndexShares(shares string)
	// GetIndexedFiles returns a list of Files currently indexed by the gossiper and available for search
	GetIndexedFiles() []*File
	// RetrieveMetaFile downloads the metafile with the given hexadecimal request hash, and returns
	// the metafile's content (the hashes of the file's chunks) or an error
	RetrieveMetaFile(dest, request string) ([]byte, error)
	// DownloadFile firsts requests a metafile of a given file and then retrieves it in chunks. If dest is not empty,
	// the gossiper must download all the chunks from dest node. Otherwise (dest == ""), the gossiper must use
	// the results of previously run searches (the results do *not* need to be persistent across reboots though)
	// to download each chunk from a random peer among those who have advertised having this chunk.
	DownloadFile(dest, filename, request string)
	// AddSearchMessage initiates a file search by broadcasting a search request for the given keywords
	// and waits for responses. The search is performed by the expanding-ring search scheme using the budget
	// provided or, otherwise, a default starting budget of 2
	AddSearchMessage(origin string, budget uint64, keywords []string)
	// GetRemoteChunksPossession returns locally stored information (obtained via search) about which chunks each node has for
	// the given file metahash. The key of the return map is a peer's ID and the value is a list of chunk IDs.
	GetRemoteChunksPossession(metaHash string) map[string][]uint32
	// RemoveChunkFromLocal removes the chunk with the given hash from the local index (so that when the gossiper
	// receives a search query for a file that has this chunk, the gossiper does not include the chunk in the chunk map
	// of the search reply. chunkHash is the hash of the chunk as a hexadecimal string.
	RemoveChunkFromLocal(chunkHash string)
	// GetBlocks returns all the blocks added so far. Key should be hexadecimal
	// representation of the block's hash. The first return is the hexadecimal
	// hash of the last block.
	GetBlocks() (string, map[string]types.Block)
}
