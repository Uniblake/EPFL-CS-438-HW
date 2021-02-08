package gossip

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/onet/v3/log"
)

// ============Tools for check/ read/ write/ verify files================

// GetDirRecords return an array of file path that can be shared (already downloaded or shared)
func GetDirRecords(rootDir string, onlyDir bool) []string {
	var files []string
	if ok, _ := PathExists(rootDir); ok {
		err := filepath.Walk(
			rootDir,
			func(path string, info os.FileInfo, err error) error {
				if onlyDir {
					if info.IsDir() {
						files = append(files, path)
					}
				} else {
					files = append(files, info.Name())
				}
				return nil
			})
		if err != nil {
			panic(fmt.Sprintln("GetDirRecords:", err))
		}
	}
	if len(files) != 0 {
		files = files[:len(files)-1]
	}
	return files
}

// GetHistoryName return the past downloaded and shared files metaHash (namely the dir)
func (g *Gossiper) GetHistoryName() []string {
	sharedHistory := GetDirRecords(g.rootSharedData, true)
	downloadedHistory := GetDirRecords(g.rootDownloadedFiles, true)
	return append(sharedHistory, downloadedHistory...)
}

// GetHistoryInfo solve one dir and return fileName, metaHashStr, metaData, blockIDs
func (g *Gossiper) GetHistoryInfo(dir string) (string, string, []byte, []uint32) {
	var fileName string
	dirLevels := strings.Split(dir, "/")
	// identify the metaHash from path
	metaHashStr := dirLevels[len(dirLevels)-1]
	files := GetDirRecords(dir, false)
	// record the ID of blocks I have
	blockIDs := make([]uint32, 0)
	// identify which is the file with its name
	for _, eachFile := range files {
		if strings.Contains(eachFile, "chunk") {
			ids, err := strconv.Atoi(eachFile[len("chunk"):])
			if err == nil {
				blockIDs = append(blockIDs, uint32(ids))
			} else {
				panic(fmt.Sprintln("GetHistoryInfo: wrong in load chunk id"))
			}
		} else if !strings.Contains(eachFile, "MetaFile") {
			fileName = eachFile
		}
	}
	if fileName == "" {
		panic(fmt.Sprintln("GetHistoryInfo: no file name in this dir"))
	}
	// load the metaData
	dataContent, err := ioutil.ReadFile(fmt.Sprintf("%s/MetaFile", dir))
	if err != nil {
		panic(fmt.Sprintln("GetHistoryInfo: fail to load metaFile"))
	}
	return fileName, metaHashStr, dataContent, blockIDs
}

// StringToBytes transform a string to bytes
func StringToBytes(data string) []byte {
	bytes, err := hex.DecodeString(data)
	if err != nil {
		panic(fmt.Sprintln("StringToBytes:", err))
	}
	return bytes
}

// VerifyHash the data match the hash
func VerifyHash(rawData []byte, vHash []byte) bool {
	reHash := sha256.Sum256(rawData)
	return hex.EncodeToString(reHash[:]) == hex.EncodeToString(vHash)
}

// PathExists checks if a path exist
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// CheckOrCreatePath check if a dir exists, if not, then create the dir
func CheckOrCreatePath(path string) (bool, error) {
	exists, err := PathExists(path)
	if !exists && err == nil {
		os.Mkdir(path, os.ModePerm)
	}
	return exists, err
}

// WriteOneChunk write a chunk to path
func WriteOneChunk(data []byte, path string) error {
	// store the chunk in shared dir
	chunkFile, err := os.Create(path)
	if err != nil {
		panic(fmt.Sprintln("WriteOneChunk: error in create one chunk file.", err))
	} else {
		if _, err = chunkFile.Write(data); err != nil {
			panic(fmt.Sprintln("WriteOneChunk: error in write one chunk data.", err))
		}
	}
	chunkFile.Close()
	return err
}

// ============Tools for indexed files================

// IndexedHelper struct represent a local file indexed to be searchable by other peers
type IndexedHelper struct {
	sync.RWMutex
	Files []*File
}

// SearchByKeyWords return a list of files that match the keywords patterns
func (f *IndexedHelper) SearchByKeyWords(keywords []string) ([]*File, bool) {
	f.RLock()
	defer f.RUnlock()
	matched := false
	match := make([]*File, 0)
	for _, file := range f.Files {
		for _, keyword := range keywords {
			if strings.Contains(file.Name, keyword) {
				matched = true
				match = append(match, &File{MetaHash: file.MetaHash, Name: file.Name})
				break
			}
		}
	}
	return match, matched
}

// CheckExist check if a file with same metahash exist
func (f *IndexedHelper) CheckExist(metaHash string) bool {
	for _, each := range f.Files {
		if each.MetaHash == metaHash {
			return true
		}
	}
	return false
}

// Update append a file to the tail
func (f *IndexedHelper) Update(file *File) bool {
	f.Lock()
	defer f.Unlock()
	if !f.CheckExist(file.MetaHash) {
		f.Files = append(f.Files, file)
		return true
	}
	return false
}

// GetFiles return a copy of all files
func (f *IndexedHelper) GetFiles() []*File {
	f.RLock()
	defer f.RUnlock()
	filesCopy := make([]*File, 0)
	for _, each := range f.Files {
		filesCopy = append(filesCopy, &File{Name: each.Name, MetaHash: each.MetaHash})
	}
	return filesCopy
}

// ============Extra Tools for gossiper================

// SendPacket send a packet to dest addr
func (g *Gossiper) SendPacket(dest string, packet *GossipPacket) {
	jsonPacket, err := json.Marshal(packet)
	if err != nil {
		log.Fatal("SendPacket: wrong serialization: ", err)
	}
	UDPAddress, err := net.ResolveUDPAddr(udpVer, dest)
	if err != nil {
		panic(fmt.Sprintln("SendPacket: Fail to resolve udp adress:", err))
	}
	g.GossipConn.WriteToUDP(jsonPacket, UDPAddress)
	g.outWatcher.Notify(CallbackPacket{Addr: dest, Msg: *packet})
}

// SendToNextHop send the packet to the next hop
func (g *Gossiper) SendToNextHop(dest string, packet *GossipPacket) error {
	routes := g.GetRoutingTable()
	if _, ok := routes[dest]; ok {
		g.SendPacket(g.routes[dest].NextHop, packet)
		return nil
	}
	return errors.New("No route to the destination")
}

// SendDataReply send the data reply packet
func (g *Gossiper) SendDataReply(dataHash string, dataBytes []byte, dest string) error {
	dataReply := &DataReply{
		Origin:      g.Name,
		Destination: dest,
		HopLimit:    downloadHopLimit,
		HashValue:   StringToBytes(dataHash),
		Data:        dataBytes,
	}
	packet := &GossipPacket{DataReply: dataReply}
	err := g.SendToNextHop(dest, packet)
	return err
}

// SendSearchReply send the request reply packet
func (g *Gossiper) SendSearchReply(matchFiles []*File, dest string) error {
	result := make([]*SearchResult, 0)
	for _, file := range matchFiles {
		dataIDs := g.RemotePos.PossessMap[file.MetaHash][g.Name]
		IDs := make([]uint32, len(dataIDs))
		copy(IDs, dataIDs)
		result = append(result, &SearchResult{
			FileName: file.Name,
			MetaHash: StringToBytes(file.MetaHash),
			ChunkMap: IDs,
		})
	}
	searchReply := &SearchReply{
		Origin:      g.Name,
		Destination: dest,
		HopLimit:    searchHopLimit,
		Results:     result,
	}
	packet := &GossipPacket{SearchReply: searchReply}
	err := g.SendToNextHop(dest, packet)
	return err
}

// RingFlood broadcast a search request
func (g *Gossiper) RingFlood(msg *SearchRequest, addr string) {
	var i int
	var packet *GossipPacket
	peers := g.GetNodes()
	// remove the node that send me the msg
	for i, peer := range peers {
		if peer == addr {
			peers[i] = peers[len(peers)-1]
			peers = peers[:len(peers)-1]
			break
		}
	}
	// send the msg as ring flood
	budge := msg.Budget
	if len(peers) > 0 {
		if msg.Budget < uint64(len(peers)) {
			idx := GenRanInt(len(peers))
			msg.Budget = 1
			packet := GossipPacket{SearchRequest: msg}
			for i = 0; uint64(i) < budge; i++ {
				g.SendPacket(peers[(i+idx)%len(peers)], &packet)
			}
		} else {
			base := msg.Budget / uint64(len(peers))
			remains := int(msg.Budget % uint64(len(peers)))
			for j, peer := range peers {
				if j < remains {
					msg.Budget = base + 1
					packet = &GossipPacket{SearchRequest: msg}
				} else {
					msg.Budget = base
					packet = &GossipPacket{SearchRequest: msg}
				}
				g.SendPacket(peer, packet)
			}
		}
	}
}

// ========== Struct to register current downloading / request / search state =================

// MetaDataHelper store all the meta data it has, access by metaData hash
type MetaDataHelper struct {
	sync.RWMutex
	DataPool map[string]map[string]uint32 // metahash hash ifExist
}

// GetDataPool copy a DataPool
func (m *MetaDataHelper) GetDataPool() map[string]map[string]uint32 {
	m.RLock()
	defer m.RUnlock()
	newDP := make(map[string]map[string]uint32)
	for metaHash := range m.DataPool {
		newMap := make(map[string]uint32)
		for hash := range m.DataPool[metaHash] {
			newMap[hash] = m.DataPool[metaHash][hash]
		}
		newDP[metaHash] = newMap
	}
	return newDP
}

// RegisterMetaData register a download metadata corresponding hash
func (m *MetaDataHelper) RegisterMetaData(metaHash string) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.DataPool[metaHash]; !ok {
		m.DataPool[metaHash] = make(map[string]uint32)
	}
}

// Update the sharing block pool according to new dataID (1,2,3,4...)
func (m *MetaDataHelper) Update(metaHash string, metaData []byte, dataID []uint32) {
	m.Lock()
	defer m.Unlock()
	// num := len(metaHash) / hashLength
	if _, ok := m.DataPool[metaHash]; !ok {
		m.DataPool[metaHash] = make(map[string]uint32)
	}
	// for i := 0; i < num; i++ {
	// 	key := hex.EncodeToString(metaData[i*hashLength : (i+1)*hashLength])
	// 	m.DataPool[metaHash][key] = 0
	// }
	for _, idx := range dataID {
		// fmt.Println("metadata helper update data id: ", idx)
		key := hex.EncodeToString(metaData[idx*hashLength : (idx+1)*hashLength])
		m.DataPool[metaHash][key] = idx
	}
}

// Add m.DataPool[metaHash][blockHash]
func (m *MetaDataHelper) Add(metaHash string, blockHash string, dataID uint32) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.DataPool[metaHash]; !ok {
		m.DataPool[metaHash] = make(map[string]uint32)
	}
	m.DataPool[metaHash][blockHash] = dataID
}

// Remove m.DataPool[metaHash][blockHash]
func (m *MetaDataHelper) Remove(blockHash string) (string, uint32) {
	m.Lock()
	defer m.Unlock()
	for metaHash := range m.DataPool {
		if _, ok := m.DataPool[metaHash][blockHash]; ok {
			delIdx := m.DataPool[metaHash][blockHash]
			delete(m.DataPool[metaHash], blockHash)
			return metaHash, delIdx
		}
	}
	return "", 0
}

// Find return the data metahash and the data ID,
// (metaHash, idx, if input is metaHash, if search successful)
// e.g.:
// 		(metahash, 0, true, true) for metaFile,
// 		(metahash, idx, false, true) for other file,
// 		("", 0, false, false) for other file.
func (m *MetaDataHelper) Find(hash string) (string, uint32, bool, bool) {
	m.RLock()
	m.RUnlock()
	if _, ok := m.DataPool[hash]; ok {
		return hash, 0, true, true
	}
	for metaHash := range m.DataPool {
		if _, ok := m.DataPool[metaHash][hash]; ok {
			ind := m.DataPool[metaHash][hash]
			return metaHash, ind, false, true
		}
	}
	return "", 0, false, false
}

// RemotePossession store the known global possession of chunks of all files
type RemotePossession struct {
	sync.RWMutex
	PossessMap map[string]map[string][]uint32 // metahash, node
}

// CheckMax check max remote possession
func (r *RemotePossession) CheckMax(metaHash string, length uint32) bool {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.PossessMap[metaHash]; ok {
		for node := range r.PossessMap[metaHash] {
			if uint32(len(r.PossessMap[metaHash][node])) == length {
				return true
			}
		}
	}
	return false
}

// GetPossessCopy return copy of current remote possession
func (r *RemotePossession) GetPossessCopy() map[string]map[string][]uint32 {
	r.RLock()
	defer r.RUnlock()
	mapHash := make(map[string]map[string][]uint32)
	for keyHash := range r.PossessMap {
		mapNode := make(map[string][]uint32)
		for keyNode := range r.PossessMap[keyHash] {
			modePossession := make([]uint32, len(r.PossessMap[keyHash][keyNode]))
			copy(modePossession, r.PossessMap[keyHash][keyNode])
			mapNode[keyNode] = modePossession
		}
		mapHash[keyHash] = mapNode
	}
	return mapHash
}

// GetPossessByHash return copy of current remote possession
func (r *RemotePossession) GetPossessByHash(hash string) map[string][]uint32 {
	r.RLock()
	defer r.RUnlock()
	result := make(map[string][]uint32)
	if _, ok := r.PossessMap[hash]; ok {
		for node := range r.PossessMap[hash] {
			thisCopy := make([]uint32, len(r.PossessMap[hash][node]))
			copy(thisCopy, r.PossessMap[hash][node])
			result[node] = thisCopy
		}
		return result
	}
	return nil
}

// CheckExist check if a file with same metahash exist
func (r *RemotePossession) CheckExist(dest string, hash string, dataID uint32) bool {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.PossessMap[hash]; ok {
		if _, ok := r.PossessMap[hash][dest]; ok {
			for _, idx := range r.PossessMap[hash][dest] {
				if dataID == idx {
					return true
				}
			}
		}
	}
	return false
}

// Remove a known chunk
func (r *RemotePossession) Remove(dest string, hash string, delID uint32) {
	r.Lock()
	defer r.Unlock()
	newIDs := make([]uint32, 0)
	for _, idx := range r.PossessMap[hash][dest] {
		if idx != delID {
			newIDs = append(newIDs, idx)
		}
	}
	r.PossessMap[hash][dest] = newIDs
}

// Add add a new data id
func (r *RemotePossession) Add(dest string, hash string, dataID uint32) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.PossessMap[hash]; !ok {
		r.PossessMap[hash] = make(map[string][]uint32)
	}
	r.PossessMap[hash][dest] = append(r.PossessMap[hash][dest], dataID)
}

// Update add a piece of new possession information
func (r *RemotePossession) Update(dest string, hash string, dataIDs []uint32) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.PossessMap[hash]; !ok {
		r.PossessMap[hash] = make(map[string][]uint32)
	}
	r.PossessMap[hash][dest] = dataIDs
}

// GetDestByPastSearch return a random dest hold the request hash file according to the last search
func (r *RemotePossession) GetDestByPastSearch(hash string, chunkID uint32) (string, error) {
	r.RLock()
	defer r.RUnlock()
	if _, ok := r.PossessMap[hash]; ok {
		candidate := make([]string, 0)
		for node := range r.PossessMap[hash] {
			for _, idx := range r.PossessMap[hash][node] {
				if idx == chunkID {
					candidate = append(candidate, node)
					break
				}
			}
		}
		if len(candidate) != 0 {
			// fmt.Println("candidate for hash is: ", candidate[rand.Intn(len(candidate))])
			return candidate[rand.Intn(len(candidate))], nil
		}
		return "", errors.New("Chunk ID not exist in possession")
	}
	return "", errors.New("No search result on current MetaHash")
}

// DownloadInfo is a reply to data request with mirrored hash value and the actual data
type DownloadInfo struct {
	sync.RWMutex
	Ctrls map[string]map[string]*DownloadController // dest hashStr
}

// DownloadController store a data request sent and waiting for reply
type DownloadController struct {
	DataSeqID uint32
	Trigger   chan *GossipPacket
}

// Contains check if three attr are equal
func (d *DownloadInfo) Contains(dest string, dataHash string) bool {
	if _, ok := d.Ctrls[dest]; ok {
		if _, ok := d.Ctrls[dest][dataHash]; ok {
			return true
		}
	}
	return false
}

// SendAndWaitForReply send the req and wait until a exec handler send a reply to its ch
func (g *Gossiper) SendAndWaitForReply(dest string, hash string, dtype string, DataSeqID uint32) (*DataReply, error) {
	// create the gossip packet
	hashVal, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	req := &DataRequest{
		Origin:      g.Name,
		Destination: dest,
		HopLimit:    downloadHopLimit,
		HashValue:   hashVal,
	}
	Packet := &GossipPacket{DataRequest: req}
	ch := make(chan *GossipPacket)
	ctrl := &DownloadController{
		DataSeqID: DataSeqID,
		Trigger:   ch,
	}
	// store the current download information and send the packet
	g.appendDownloadController(dest, hash, ctrl)
	defer g.releaseDownloadController(dest, hash, DataSeqID)
	// use the backoff mechanism
	var tries uint32
	for tries = 0; tries < 5; tries++ {
		// fmt.Println("SendAndWaitForReply: try for the time: ", tries)
		for {
			if err = g.SendToNextHop(dest, Packet); err == nil {
				// return nil, errors.New("fail to find next hop")
				break
			}
		}
		// routes := g.GetRoutingTable()
		// for key := range routes {
		// 	fmt.Printf("routing+++: %s : %s %+v\n", g.Name, key, routes[key])
		// }
		timer := time.NewTimer(time.Duration(math.Pow(backOffBase, float64(tries+1))) * time.Second)
		// Wait for the reply
		select {
		case <-g.stopSignal:
			return nil, errors.New("gossiper called stop")
		case <-timer.C:
			continue
		case reply := <-ch:
			return reply.DataReply, nil
		}
	}
	return nil, errors.New("Fail to get the reply from the dest")
}

// appendDownloadController send the req and wait until a exec handler send a reply to its ch
func (g *Gossiper) appendDownloadController(dest string, hash string, ctrl *DownloadController) {
	g.DownloadTrace.Lock()
	defer g.DownloadTrace.Unlock()
	if _, ok := g.DownloadTrace.Ctrls[dest]; !ok {
		g.DownloadTrace.Ctrls[dest] = make(map[string]*DownloadController)
	}
	if _, ok := g.DownloadTrace.Ctrls[dest][hash]; !ok {
		g.DownloadTrace.Ctrls[dest][hash] = ctrl
	}
}

// SendAndWaitForReply send the req and wait until a exec handler send a reply to its ch
func (g *Gossiper) releaseDownloadController(dest string, hashStr string, DataSeqID uint32) {
	g.DownloadTrace.Lock()
	defer g.DownloadTrace.Unlock()
	if _, ok := g.DownloadTrace.Ctrls[dest]; ok {
		if _, ok := g.DownloadTrace.Ctrls[dest][hashStr]; ok {
			if g.DownloadTrace.Ctrls[dest][hashStr].DataSeqID == DataSeqID {
				delete(g.DownloadTrace.Ctrls[dest], hashStr)
			}
		}
	}
}

// DuplicateHelper store the info of search request if it is duplicated
type DuplicateHelper struct {
	sync.RWMutex
	Records map[string]map[uint32][]string // [origin][num][keywords]
}

// CheckExist check if the request is duplicate within 0.5s
func (d *DuplicateHelper) CheckExist(origin string, sKeywords []string) bool {
	d.RLock()
	defer d.RUnlock()
	if _, ok := d.Records[origin]; ok {
		for reqID := range d.Records[origin] {
			// compare strings
			if len(d.Records[origin][reqID]) == len(sKeywords) {
				for _, l := range d.Records[origin][reqID] {
					match := false
					for _, r := range sKeywords {
						if l == r {
							match = true
						}
					}
					if match == false {
						return false
					}
				}
				return true
			}
		}
	}
	return false
}

// RegisterNewSearch register the new search request
func (d *DuplicateHelper) RegisterNewSearch(origin string, sKeywords []string) {
	d.Lock()
	if _, ok := d.Records[origin]; !ok {
		d.Records[origin] = make(map[uint32][]string)
	}
	idx := uint32(len(d.Records[origin]))
	d.Records[origin][idx] = sKeywords
	d.Unlock()
	go func() {
		timer := time.NewTimer(time.Duration(duplicateLimit) * time.Millisecond)
		t1 := time.Now() // get current time
		<-timer.C
		d.Lock()
		delete(d.Records[origin], idx)
		d.Unlock()
		elapsed := time.Since(t1)
		fmt.Println("RegisterNewSearch elapsed: ", elapsed)
	}()
}

// FetchLocalFile check the download and the shared dir and load the file
func (g *Gossiper) FetchLocalFile(metaHash string, dataID uint32, isMeta bool) ([]byte, bool) {
	var fileSuffix, path string
	if isMeta {
		fileSuffix = "MetaFile"
	} else {
		fileSuffix = fmt.Sprintf("chunk%d", dataID)
	}
	// find the path in shared or download
	downloadPath := fmt.Sprintf("%s/%s/%s", g.rootDownloadedFiles, metaHash, fileSuffix)
	sharedPath := fmt.Sprintf("%s/%s/%s", g.rootSharedData, metaHash, fileSuffix)
	// fmt.Println("try to find file in : ", downloadPath, " and ", sharedPath)
	if ok, _ := PathExists(downloadPath); ok {
		path = downloadPath
	} else if ok, _ := PathExists(sharedPath); ok {
		path = sharedPath
	}
	// open the file
	if path != "" {
		dataContent, err := ioutil.ReadFile(path)
		if err == nil {
			return dataContent, true
		}
	}
	return nil, false
}

// IndexOneFile index one file according to its name
func (g *Gossiper) IndexOneFile(share string) (bool, []byte, []uint32, []byte)  {
	// open the file
	file, err := os.Open(share)
	if err != nil {
		panic(fmt.Sprintln("IndexShares: fail to open shared files.", err))
	}
	defer file.Close()
	// create a _tmp folder to store the processing chunks and later rename to its metahash
	tmpDir := fmt.Sprintf("%s/%s", g.rootSharedData, "_tmp/")
	if _, err = CheckOrCreatePath(tmpDir); err != nil {
		panic(fmt.Sprintln("Error in creating Dir: rootSharedData.", err))
	}
	// store the hash of all chunks
	var MetaContent []byte
	var idx uint32 = 0
	idList := make([]uint32, 0)
	buffer := make([]byte, myChunkSize)
	for {
		bytesread, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(fmt.Sprintln("IndexShares: error in read chunks.", err))
		} else {
			idList = append(idList, idx)
		}
		// Hash(this chunk)
		sum := sha256.Sum256(buffer[:bytesread])
		MetaContent = append(MetaContent, sum[:]...)
		// store the chunk in shared dir
		if err = WriteOneChunk(buffer[:bytesread], fmt.Sprintf("%schunk%d", tmpDir, idx)); err != nil {
			panic(fmt.Sprintln("IndexShares: error in write one chunk.", err))
		}
		idx++
	}
	// generate MetaFile
	if err = WriteOneChunk(MetaContent, fmt.Sprintf("%s%s", tmpDir, "MetaFile")); err != nil {
		panic(fmt.Sprintln("IndexShares: error in write MetaFile.", err))
	}
	MetaHash := sha256.Sum256(MetaContent)
	// rename the _tmp dir to metaHash
	metaHashStr := hex.EncodeToString(MetaHash[:])
	MetaName := fmt.Sprintf("%s/%s/", g.rootSharedData, metaHashStr)
	if err = os.Rename(tmpDir, MetaName); err != nil {
		// panic(fmt.Sprintln("IndexShares: error in rename _tmp dir.", err))
		return false, nil, nil, nil
	}
	return true, MetaHash[:], idList, MetaContent
}

// RecoverIndex recover from break
func (g *Gossiper) RecoverIndex() {
	fileList := g.GetHistoryName()
	for _, file := range fileList {
		fileName, metaHashStr, metaData, blockIDs := g.GetHistoryInfo(file)
		g.IndexedFiles.Update(&File{Name: fileName, MetaHash: metaHashStr})
		g.RemotePos.Update(g.Name, metaHashStr, blockIDs)
		g.MetaDataStorage.Update(metaHashStr, metaData, blockIDs)
	}
}

// SearchInfo records all the search controller running in current gossiper
type SearchInfo struct {
	sync.RWMutex
	Ctrls []*SearchController // dest hashStr
}

// ContainsAndTrigger check if a search result is a match, if is then trigger
func (s *SearchInfo) ContainsAndTrigger(packet *GossipPacket) bool {
	s.RLock()
	defer s.RUnlock()
	for _, ctrl := range s.Ctrls {
		if ctrl.Expects(packet.SearchReply.Results) {
			ctrl.Trigger <- packet
			return true
		}
	}
	return false
}

// SearchController store a search request sent and waiting for reply
type SearchController struct {
	sync.RWMutex
	keywords      []string
	Trigger       chan *GossipPacket
	matchedStatus map[string]uint32 // len of all blocks
}

// UpdateMatchedStatus update a new incoming result
func (s *SearchController) UpdateMatchedStatus(result *SearchResult) bool {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.matchedStatus[result.HashStr()]; !ok {
		s.matchedStatus[result.HashStr()] = 0
		return true
	}
	return false
}

// Contains check if the file name in one result matches patterns in searchController
func (s *SearchController) Contains(fileName string) bool {
	for _, pattern := range s.keywords {
		if strings.Contains(fileName, pattern) {
			return true
		}
	}
	return false
}

// Expects check if the search result is expected by one search process.
func (s *SearchController) Expects(results []*SearchResult) bool {
	for _, piece := range results {
		if !s.Contains(piece.FileName) {
			return false
		}
	}
	return true
}

// appendSearchController
func (g *Gossiper) appendSearchController(ctrl *SearchController) {
	g.SearchTrace.Lock()
	defer g.SearchTrace.Unlock()
	g.SearchTrace.Ctrls = append(g.SearchTrace.Ctrls, ctrl)
}

// releaseSearchController
// func (g *Gossiper) releaseSearchController(keywords []string) {
// 	var match bool
// 	g.SearchTrace.Lock()
// 	defer g.SearchTrace.Unlock()
// 	for id, ctrl := range g.SearchTrace.Ctrls {
// 		match = true
// 		if len(ctrl.keywords) == len(keywords) {
// 			for i := 0; i < len(keywords); i++ {
// 				if ctrl.keywords[i] != keywords[i] {
// 					match = false
// 					break
// 				}
// 			}
// 			if match {
// 				g.SearchTrace.Ctrls[id] = g.SearchTrace.Ctrls[len(g.SearchTrace.Ctrls)-1]
// 				g.SearchTrace.Ctrls[len(g.SearchTrace.Ctrls)-1] = nil
// 				g.SearchTrace.Ctrls = g.SearchTrace.Ctrls[:len(g.SearchTrace.Ctrls)-1]
// 				break
// 			}
// 		}
// 	}
// }

// releaseSearchController
func (g *Gossiper) releaseSearchController(ctrl *SearchController) {
	g.SearchTrace.Lock()
	defer g.SearchTrace.Unlock()
	ctrl = nil
	for id, ctrl := range g.SearchTrace.Ctrls {
		if ctrl == nil {
			g.SearchTrace.Ctrls[id] = g.SearchTrace.Ctrls[len(g.SearchTrace.Ctrls)-1]
			g.SearchTrace.Ctrls[len(g.SearchTrace.Ctrls)-1] = nil
			g.SearchTrace.Ctrls = g.SearchTrace.Ctrls[:len(g.SearchTrace.Ctrls)-1]
		}
	}
}

// SearchAndWaitForReply conduct a search until get enough response.
func (g *Gossiper) SearchAndWaitForReply(origin string, keywords []string, budget uint64) (bool, error) {
	if budget != 0 {
		// a normal search
		req := &SearchRequest{Origin: origin, Budget: budget, Keywords: keywords}
		g.BroadcastMessage(GossipPacket{SearchRequest: req})
		// select {
		// case <-g.stopSignal:
		// 	return false, errors.New("gossiper called stop")
		// case reply := <-ch:
		// 	return true, nil
		// }
	} else {
		ch := make(chan *GossipPacket)
		ctrl := &SearchController{
			keywords:      keywords,
			Trigger:       ch,
			matchedStatus: make(map[string]uint32),
		}
		go func() {
			// store search request and send it
			g.appendSearchController(ctrl)
			defer g.releaseSearchController(ctrl)
			g.SearchUntilSatisfied(origin, keywords, ctrl)
		}()
	}
	return true, nil
}

// SearchUntilSatisfied conduct a search until reach a threshold or max budget.
func (g *Gossiper) SearchUntilSatisfied(origin string, keywords []string, ctrl *SearchController) (bool, error) {
	// a client infinite search request
	req := &SearchRequest{Origin: origin, Budget: 2, Keywords: keywords}
	var tries uint64
	// set the ticker
	ticker := time.NewTicker(time.Duration(searchInterval) * time.Second)
	for tries = 1; tries <= searchMaxExp; tries++ {
		req.Budget = uint64(math.Pow(backOffBase, float64(tries)))
		// fmt.Println("0 budget --> send with budget: ", req.Budget)
		g.BroadcastMessage(GossipPacket{SearchRequest: req})
		// Wait for the reply
		select {
		case <-g.stopSignal:
			return false, errors.New("gossiper called stop")
		case <-ticker.C:
			continue
		case reply := <-ctrl.Trigger:
			fulfilled := g.registerMatchedStatusAndRetrieveMeta(ctrl, reply.SearchReply)
			// check remote possession and metaDataBlocks, if we can stop the search process.
			if fulfilled >= searchThreshold {
				return true, nil
			}
		}
	}
	return true, nil
}

// registerMatchedStatusAndRetrieveMeta
func (g *Gossiper) registerMatchedStatusAndRetrieveMeta(
	ctrl *SearchController, reply *SearchReply) int {
	// update new metaHash to matched status
	fulfilled := 0
	for _, result := range reply.Results {
		// MetaData / data not exist locally
		if _, _, exist, _ := g.MetaDataStorage.Find(hex.EncodeToString(result.MetaHash)); !exist {
			// if first get this search result, ask for the metaData
			metaHashStr := result.HashStr()
			if ctrl.UpdateMatchedStatus(result) {
				metaData, _ := g.RetrieveMetaFile("", hex.EncodeToString(result.MetaHash))
				g.MetaDataStorage.RegisterMetaData(metaHashStr)
				metaDataLen := uint32(len(metaData) / hashLength)
				ctrl.matchedStatus[metaHashStr] = metaDataLen
			}
			if g.RemotePos.CheckMax(metaHashStr, ctrl.matchedStatus[metaHashStr]) {
				fulfilled++
			}
		}
	}
	return fulfilled
}

// ConfigHelper help to save indexing config
type ConfigHelper struct {
	FilePool []*File
	MetaPool map[string]map[string]uint32
}

// GenerateCrashConfig generate config file to store indexed infomation
func (g *Gossiper) GenerateCrashConfig() {
	FileIndexPool := g.IndexedFiles.GetFiles()
	MetaDataPool := g.MetaDataStorage.GetDataPool()
	cfg := ConfigHelper{FilePool: FileIndexPool, MetaPool: MetaDataPool}
	content, err := json.Marshal(cfg)
	if err != nil {
		panic(fmt.Sprintln("GenerateCrashConfig: ", err))
	}
	fileName := fmt.Sprintf("%s/%s", g.rootSharedData, "config.json")
	if err = WriteOneChunk(content, fileName); err != nil {
		panic(fmt.Sprintln("GenerateCrashConfig: ", err))
	}
}

// LoadCrashConfig load config
func (g *Gossiper) LoadCrashConfig() {
	fileName := fmt.Sprintf("%s/%s", g.rootSharedData, "config.json")
	if ok, _ := PathExists(fileName); ok {
		dataContent, err := ioutil.ReadFile(fileName)
		if err != nil {
			panic(fmt.Sprintln("LoadCrashConfig: ", err))
		}
		var cfg ConfigHelper
		if err = json.Unmarshal(dataContent, &cfg); err != nil {
			panic(fmt.Sprintln("LoadCrashConfig: ", err))
		}
		g.MetaDataStorage.RLock()
		defer g.MetaDataStorage.RUnlock()
		g.MetaDataStorage.DataPool = cfg.MetaPool
		g.IndexedFiles.RLock()
		defer g.IndexedFiles.RUnlock()
		g.IndexedFiles.Files = cfg.FilePool
		for metaHash := range g.MetaDataStorage.DataPool {
			idList := make([]uint32, 0)
			for blockHash := range g.MetaDataStorage.DataPool[metaHash] {
				idList = append(idList, g.MetaDataStorage.DataPool[metaHash][blockHash])
			}
			g.RemotePos.Update(g.Name, metaHash, idList)
		}
	}
}
