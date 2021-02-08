package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/hw3/client"
	"go.dedis.ch/cs438/hw3/gossip/types"
	"go.dedis.ch/cs438/hw3/gossip/watcher"
	"golang.org/x/xerrors"
)

const (
	callbackEndpoint = "/callback"
	watchInEndpoint  = "/watchIn"
	watchOutEndpoint = "/watchOut"
)

var (
	// defaultLevel can be changed to set the desired level of the logger
	defaultLevel = zerolog.InfoLevel

	// logout is the logger configuration
	logout = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
)

// BinFacOption is the type of option for the bingossip factory.
type BinFacOption func(*BinGossiperTemplate)

// WithBroadcast specifies the broadcast mode.
func WithBroadcast() BinFacOption {
	return func(b *BinGossiperTemplate) {
		b.withBroadcast = true
	}
}

// BinGossiperTemplate contains the bingossip factory's options.
type BinGossiperTemplate struct {
	withBroadcast bool
}

// NewBinGossiperTemplate creates a new template with the default options.
func NewBinGossiperTemplate() BinGossiperTemplate {
	return BinGossiperTemplate{
		withBroadcast: false,
	}
}

// NewBinGossipFactory creates a new bin gossip factory, which build a gossiper
// based on a provided binary.
func NewBinGossipFactory(binPath string, opts ...BinFacOption) GossipFactory {
	return Factory{
		binPath: binPath,
		opts:    opts,
	}
}

// Factory provides a factory to instantiate a Gossiper
//
// - implements gossip.GossipFactory
type Factory struct {
	binPath string
	opts    []BinFacOption
}

// New implements gossip.GossipFactory.
func (f Factory) New(address, identifier string, antiEntropy int, routeTimer int,
	rootSharedData string, rootDownloadedFiles string, numParticipant,
	nodeIndex, paxosRetry int) (BaseGossiper, error) {

	template := NewBinGossiperTemplate()

	for _, opt := range f.opts {
		opt(&template)
	}

	// The UI port is used to communicate with the gossiper. This is our API
	// endpoint to talk to the gossiper that is embedded in the binary.
	uiPortStr := getRandomPort()

	uiURL, err := url.Parse("http://127.0.0.1:" + uiPortStr)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse uiURL: %v", err)
	}

	g := BinGossiper{
		address:    address,
		identifier: identifier,
		stop:       make(chan struct{}),
		uiURL:      uiURL,
		stopwait:   new(sync.WaitGroup),

		inWatcher:  watcher.NewSimpleWatcher(),
		outWatcher: watcher.NewSimpleWatcher(),

		log: zerolog.New(logout).
			Level(defaultLevel).
			With().Timestamp().Logger().
			With().Caller().Logger().
			With().Str("addr", address).Logger().
			With().Str("id", identifier).Logger(),
	}

	// We start a new callback http server. This server is used for the
	// controller to notify us when a new message arrives. We need it to call
	// the callback registered by the RegisterCallback() function: Each time
	// this server receives a notification (ie. a POST /callback), it can then
	// notify the callback. The controller knows that it must contact this
	// server because we specify it in the binary argument when we launch it.
	callbackAddr, err := g.setupCallbackSrv()

	g.log.Info().Msgf("starting the gossiper and its controller with the binary at %s", f.binPath)

	err = g.startBinGossip(f.binPath, uiPortStr, antiEntropy,
		routeTimer, callbackAddr, rootSharedData, rootDownloadedFiles,
		template.withBroadcast, numParticipant, nodeIndex, paxosRetry)
	if err != nil {
		return nil, xerrors.Errorf("failed to start binary: %v", err)
	}

	time.Sleep(time.Millisecond * 300)

	return &g, nil
}

// BinGossiper implements a gossiper based on a binary.
//
// - implements gossip.BaseGossiper.
type BinGossiper struct {
	address    string
	identifier string
	callback   NewMessageCallback
	uiURL      *url.URL
	stop       chan struct{}
	stopwait   *sync.WaitGroup

	inWatcher  watcher.Watcher
	outWatcher watcher.Watcher

	log zerolog.Logger
}

// BroadcastMessage implements gossip.BaseGossiper.
func (g BinGossiper) BroadcastMessage(p GossipPacket) {
	panic("not implemented") // TODO: Implement
}

// RegisterHandler implements gossip.BaseGossiper.
func (g BinGossiper) RegisterHandler(handler interface{}) error {
	panic("not implemented") // TODO: Implement
}

// GetBlocks implements gossip.BaseGossiper
func (g *BinGossiper) GetBlocks() (string, map[string]types.Block) {
	URL := g.uiURL.String() + "/blocks"

	resp, err := http.Get(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to GET /blocks")
		return "", nil
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("unexpected status %s", resp.Status)
		return "", nil
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err).Msg("failed to read response")
		return "", nil
	}

	chainResp := client.ChainResp{}

	err = json.Unmarshal(buf, &chainResp)
	if err != nil {
		g.log.Err(err).Msg("failed to unmarshal chainResp")
		return "", nil
	}

	return chainResp.Last, chainResp.Chain
}

// GetNodes implements gossip.BaseGossiper.
func (g BinGossiper) GetNodes() []string {
	URL := g.uiURL.String() + "/node"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url")
		return nil
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err)
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return nil
	}

	res := make([]string, 0)

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err)
	}

	nullStr := `null
`

	if string(resBuf) == nullStr {
		return []string{}
	}

	err = json.Unmarshal(resBuf, &res)
	if err != nil {
		g.log.Err(err)
	}

	return res
}

// GetDirectNodes implements gossip.BaseGossiper.
func (g BinGossiper) GetDirectNodes() []string {
	URL := g.uiURL.String() + "/origin"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url")
		return nil
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err)
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return nil
	}

	res := make([]string, 0)

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err)
	}

	nullStr := `null
`

	if string(resBuf) == nullStr {
		return []string{}
	}

	err = json.Unmarshal(resBuf, &res)
	if err != nil {
		g.log.Err(err)
	}

	return res
}

// GetRoutingTable implements gossip.BaseGossiper.
func (g BinGossiper) GetRoutingTable() map[string]*RouteStruct {
	URL := g.uiURL.String() + "/routing"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url")
		return nil
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err)
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return nil
	}

	res := make(map[string]*RouteStruct)

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err)
	}

	err = json.Unmarshal(resBuf, &res)
	if err != nil {
		g.log.Err(err)
	}

	return res
}

// GetLocalAddr returns the local address (ip:port) used for sending and
// receiving packets to/from the network.
func (g BinGossiper) GetLocalAddr() string {
	URL := g.uiURL.String() + "/address"

	resp, err := http.Get(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to get address")
		return ""
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return ""
	}

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err).Msg("failed to read body")
		return ""
	}

	return string(resBuf)
}

// IndexShares takes a list of files and indexes them in the gossiper. For each
// file, it generates the metafile, computes its hash, and makes the gossiper of
// the filename and the hashes (of the metafile and the chunks) that might be
// requested. It expect the file to be present in the current working directory:
// os.Getwd()
func (g BinGossiper) IndexShares(shares string) {
	m := client.ClientMessage{
		Share: shares,
	}

	g.postMessage(&m)
}

// GetIndexedFiles returns a list of Files currently indexed by the gossiper and
// available for search
func (g BinGossiper) GetIndexedFiles() []*File {
	URL := g.uiURL.String() + "/fullshare"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url")
		return nil
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err)
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return nil
	}

	files := make([]*File, 0)

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err)
	}

	err = json.Unmarshal(resBuf, &files)
	if err != nil {
		g.log.Err(err)
		return nil
	}

	return files
}

// RetrieveMetaFile downloads the metafile with the given hexadecimal request
// hash, and returns the metafile's content (the hashes of the file's chunks) or
// an error
func (g BinGossiper) RetrieveMetaFile(dest, request string) ([]byte, error) {
	URL := g.uiURL.String() + "/metafile"

	resp, err := http.PostForm(URL, url.Values{"dest": {dest}, "request": {request}})
	if err != nil {
		g.log.Err(err).Msg("failed to POST /metafile")
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("unexpected status %s", resp.Status)
		return nil, err
	}

	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err).Msg("failed to read response")
		return nil, err
	}

	return res, nil
}

// DownloadFile firsts requests a metafile of a given file and then retrieves it
// in chunks. If dest is not empty, the gossiper must download all the chunks
// from dest node. Otherwise (dest == ""), the gossiper must use the results of
// previously run searches (the results do *not* need to be persistent across
// reboots though) to download each chunk from a random peer among those who
// have advertised having this chunk.
func (g BinGossiper) DownloadFile(dest, filename, request string) {
	URL := g.uiURL.String() + "/downloadfile"

	values := url.Values{
		"dest":     {dest},
		"filename": {filename},
		"request":  {request},
	}

	resp, err := http.PostForm(URL, values)
	if err != nil {
		g.log.Err(err).Msg("failed to POST /downloadfile")
		return
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("unexpected status %s", resp.Status)
		return
	}
}

// AddSearchMessage initiates a file search by broadcasting a search request for
// the given keywords and waits for responses. The search is performed by the
// expanding-ring search scheme using the budget provided or, otherwise, a
// default starting budget of 2
// Origin is not used because it will be automatically filled by the controller.
func (g BinGossiper) AddSearchMessage(origin string, budget uint64, keywords []string) {
	m := client.ClientMessage{
		Keywords: strings.Join(keywords, ","),
		Budget:   strconv.FormatUint(budget, 10),
	}

	g.postMessage(&m)
}

// GetRemoteChunksPossession returns locally stored information (obtained via
// search) about which chunks each node has for the given file metahash. The key
// of the return map is a peer's ID and the value is a list of chunk IDs.
func (g BinGossiper) GetRemoteChunksPossession(metaHash string) map[string][]uint32 {
	URL := g.uiURL.String() + "/remotechunkspossession"

	resp, err := http.PostForm(URL, url.Values{"metaHash": {metaHash}})
	if err != nil {
		g.log.Err(err).Msg("failed to POST /remotechunkspossession")
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("unexpected status %s", resp.Status)
		return nil
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err).Msg("failed to read response")
		return nil
	}

	res := make(map[string][]uint32)
	err = json.Unmarshal(buf, &res)
	if err != nil {
		g.log.Err(err).Msg("failed to unmarshal result")
		return nil
	}

	return res
}

// RemoveChunkFromLocal removes the chunk with the given hash from the local
// index (so that when the gossiper receives a search query for a file that has
// this chunk, the gossiper does not include the chunk in the chunk map of the
// search reply. chunkHash is the hash of the chunk as a hexadecimal string.
func (g BinGossiper) RemoveChunkFromLocal(chunkHash string) {
	URL := g.uiURL.String() + "/removechunkfromlocal"

	resp, err := http.PostForm(URL, url.Values{"chunkHash": {chunkHash}})
	if err != nil {
		g.log.Err(err).Msg("failed to POST /removechunkfromlocal")
		return
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("unexpected status %s", resp.Status)
		return
	}
}

// SetIdentifier implements gossip.BaseGossiper.
func (g BinGossiper) SetIdentifier(id string) {
	URL := g.uiURL.String() + "/id"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url: %v")
		return
	}

	req := &http.Request{
		Method: http.MethodPost,
		URL:    parsedURL,
		Header: map[string][]string{
			"Content-Type": {"text/plain; charset=UTF-8"},
		},
		Body: ioutil.NopCloser(strings.NewReader(id)),
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err).Msg("failed to add address")
		return
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return
	}
}

// GetIdentifier implements gossip.BaseGossiper.
func (g BinGossiper) GetIdentifier() string {
	URL := g.uiURL.String() + "/id"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url")
		return ""
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err).Msg("failed to call request")
		return ""
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return ""
	}

	resBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err).Msg("failed to read body")
		return ""
	}

	return string(resBuf)
}

// AddMessage implements gossip.BaseGossiper.
func (g BinGossiper) AddMessage(text string) uint32 {
	g.log.Info().Msg("Sending a message")

	// The controller will act differently based on the --broadcast argument
	m := client.ClientMessage{
		Contents: text,
	}

	resp := g.postMessage(&m)
	if resp == nil {
		g.log.Error().Msg("got a nil response")
		return 0
	}

	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		g.log.Err(err).Msg("failed to read response from addMessage")
		return 0
	}

	id := binary.LittleEndian.Uint32(res)

	return id
}

// AddPrivateMessage implements gossip.BaseGossiper.
func (g BinGossiper) AddPrivateMessage(text, dest, origin string, hoplimit int) {
	// the controller will take the gossiper's origin and handle this message as
	// private because it has a destination.
	m := client.ClientMessage{
		Contents:    text,
		Destination: dest,
	}

	g.postMessage(&m)
}

// AddSimpleMessage implements gossip.BaseGossiper.
func (g BinGossiper) AddSimpleMessage(text string) {
	m := client.ClientMessage{
		Contents: text,
	}

	g.postMessage(&m)
}

func (g BinGossiper) postMessage(m *client.ClientMessage) *http.Response {
	messageBuf, err := json.Marshal(m)
	if err != nil {
		g.log.Err(err).Msg("failed to marshal client message")
	}

	URL := g.uiURL.String() + "/message"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		g.log.Err(err).Msg("failed to parse url")
		return nil
	}

	req := &http.Request{
		Method: "POST",
		URL:    parsedURL,
		Header: map[string][]string{
			"Content-Type": {"application/json; charset=UTF-8"},
		},
		Body: ioutil.NopCloser(bytes.NewBuffer(messageBuf)),
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.log.Err(err).Msg("failed to add simple message")
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("expected 200 response, got: %s", resp.Status)
		return nil
	}

	g.log.Info().Msgf("DONE sending gossip packet to %s resp is %v", g.address, resp)
	return resp
}

// AddAddresses implements gossip.BaseGossiper.
func (g BinGossiper) AddAddresses(addresses ...string) error {
	// since the controller only accept on address at a time, we must send N
	// requests
	URL := g.uiURL.String() + "/node"
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return xerrors.Errorf("failed to parse url: %v", err)
	}

	for _, addr := range addresses {
		req := &http.Request{
			Method: http.MethodPost,
			URL:    parsedURL,
			Header: map[string][]string{
				"Content-Type": {"text/plain; charset=UTF-8"},
			},
			Body: ioutil.NopCloser(strings.NewReader(addr)),
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return xerrors.Errorf("failed to add address: %v", err)
		}

		if resp.StatusCode != http.StatusOK {
			return xerrors.Errorf("expected 200 response, got: %s", resp.Status)
		}
	}

	return nil
}

// AddRoute updates the gossiper's routing table by adding a next hop for
// the given peer node
func (g BinGossiper) AddRoute(peerName, nextHop string) {
	URL := g.uiURL.String() + "/routing"

	resp, err := http.PostForm(URL, url.Values{"peerName": {peerName}, "nextHop": {nextHop}})
	if err != nil {
		g.log.Err(err).Msg("failed to POST /routing")
		return
	}

	if resp.StatusCode != http.StatusOK {
		g.log.Error().Msgf("unexpected status %s", resp.Status)
		return
	}
}

// RegisterCallback implements gossip.BaseGossiper.
func (g *BinGossiper) RegisterCallback(c NewMessageCallback) {
	g.callback = c
}

// Watch implements gossip.BaseGossiper.
func (g BinGossiper) Watch(ctx context.Context, fromIncoming bool) <-chan CallbackPacket {
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

// Run implements gossip.BaseGossiper.
func (g BinGossiper) Run(ready chan struct{}) {
	close(ready)
}

// Stop implements gossip.BaseGossiper.
func (g BinGossiper) Stop() {
	close(g.stop)
	g.stopwait.Wait()

	g.log.Info().Msg("process and callback server stopped!")
}

// setupCallbackSrv starts the callback server and attaches the needed handler
// to it. It also proprely stops it when the stop chan closes.
func (g *BinGossiper) setupCallbackSrv() (string, error) {
	g.stopwait.Add(1)

	mux := http.NewServeMux()
	server := &http.Server{Addr: "127.0.0.1:0", Handler: mux}

	// We create the connection
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return "", xerrors.Errorf("failed to listen: %v", err)
	}
	g.log.Info().Msgf("callback server started at %s", ln.Addr())

	handleCallbacks := func(w http.ResponseWriter, req *http.Request) {
		buf, err := ioutil.ReadAll(req.Body)
		if err != nil {
			g.log.Err(err).Msg("failed to read buf in callback")
			return
		}

		packet := CallbackPacket{}
		err = json.Unmarshal(buf, &packet)
		if err != nil {
			g.log.Err(err).Msg("failed to unmarshal packet")
			return
		}

		if g.callback != nil {
			g.callback(packet.Addr, packet.Msg)
		}
	}

	getHandleWatch := func(wa watcher.Watcher) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, req *http.Request) {
			buf, err := ioutil.ReadAll(req.Body)
			if err != nil {
				g.log.Err(err).Msg("failed to read buf in watchIn")
				return
			}

			packet := CallbackPacket{}
			err = json.Unmarshal(buf, &packet)
			if err != nil {
				g.log.Err(err).Msg("failed to unmarshal packet")
				return
			}

			wa.Notify(packet)
		}
	}

	mux.HandleFunc(callbackEndpoint, handleCallbacks)
	mux.HandleFunc(watchInEndpoint, getHandleWatch(g.inWatcher))
	mux.HandleFunc(watchOutEndpoint, getHandleWatch(g.outWatcher))

	wait := make(chan struct{})
	go func() {
		close(wait)
		err := server.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			g.log.Err(err).Msg("failed to start http server")
		}
	}()

	<-wait

	// Routing to wait for the close signal, which tells us to stop the callback
	// server.
	go func() {
		defer g.stopwait.Done()
		<-g.stop
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		err := server.Shutdown(ctx)
		if err != nil {
			g.log.Err(err).Msg("failed to stop callback server")
		}
	}()

	return ln.Addr().String(), nil
}

// startBinGossip calls the binary that starts the gossiper. It also ensures
// that the process is proprely closed once the stop chan is closed.
func (g *BinGossiper) startBinGossip(binPath, uiPortStr string, antiEntropy,
	routeTimer int, callbackAddr, rootSharedData, rootDownloadedFiles string,
	withBroadcast bool, numParticipant, nodeIndex, paxosRetry int) error {

	g.stopwait.Add(1)

	args := []string{
		"--UIPort", uiPortStr,
		"--gossipAddr", g.address,
		"--name", g.identifier,
		"--antiEntropy", strconv.Itoa(antiEntropy),
		"--rtimer", strconv.Itoa(routeTimer),
		"--hookURL", "http://" + callbackAddr + callbackEndpoint,
		"--watchInURL", "http://" + callbackAddr + watchInEndpoint,
		"--watchOutURL", "http://" + callbackAddr + watchOutEndpoint,
		"--sharedir", rootSharedData,
		"--downdir", rootDownloadedFiles,
		"--numParticipants", strconv.Itoa(numParticipant),
		"--nodeIndex", strconv.Itoa(nodeIndex),
		"--paxosRetry", strconv.Itoa(paxosRetry),
	}

	if withBroadcast {
		args = append(args, "--broadcast")
	}

	g.log.Info().Msgf("starting bingossip with the following args: %v", args)

	cmd := exec.Command(binPath, args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return xerrors.Errorf("failed to run node: %v", err)
	}

	go func() {
		defer g.stopwait.Done()

		<-g.stop
		err := cmd.Process.Signal(os.Interrupt)
		if err != nil {
			g.log.Err(err).Msg("failed to stop process")
			err = cmd.Process.Kill()
			if err != nil {
				g.log.Err(err).Msg("failed to kill")
				return
			}
		}

		_, err = cmd.Process.Wait()
		if err != nil {
			g.log.Err(err).Msg("failed to wait for stop process")
			return
		}
	}()

	return nil
}

// getRandomPort returns a random port that is not used at the time of testing.
func getRandomPort() string {
	var uiPortStr string
	rand.Seed(time.Now().UnixNano())

	for {
		// minimum port value is 1025
		uiPort := rand.Intn(65534-1025) + 1025
		uiPortStr = strconv.Itoa(uiPort)

		ln, err := net.Listen("tcp", ":"+uiPortStr)

		// If we can listen, that means the port is free
		if err == nil {
			if err := ln.Close(); err == nil {
				break
			}
		}
	}

	return uiPortStr
}
