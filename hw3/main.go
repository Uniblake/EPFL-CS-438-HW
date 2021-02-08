// ========== CS-438 HW0 Skeleton ===========
// *** Do not change this file ***

// This file should be the entering point to your program.
// Here, we only parse the input and start the logic implemented
// in other files.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/hw3/gossip"

	"go.dedis.ch/cs438/hw3/client"
)

const defaultGossipAddr = "127.0.0.1:33000" // IP address:port number for gossiping
const defaultName = "peerXYZ"               // Give a unique default name
const defaultPaxosRetry = 3                 // in seconds

var (
	// defaultLevel can be changed to set the desired level of the logger
	defaultLevel = zerolog.WarnLevel

	// logout is the logger configuration
	logout = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	// Logger is a globally available logger.
	Logger = zerolog.New(logout).Level(defaultLevel).
		With().Timestamp().Logger().
		With().Caller().Logger()
)

func main() {

	UIPort := flag.String("UIPort", client.DefaultUIPort, "port for gossip communication with peers")
	antiEntropy := flag.Int("antiEntropy", 10, "timeout in seconds for anti-entropy (relevant only fo rPart2)' default value 10 seconds.")
	gossipAddr := flag.String("gossipAddr", defaultGossipAddr, "ip:port for gossip communication with peers")
	ownName := flag.String("name", defaultName, "identifier used in the chat")
	peers := flag.String("peers", "", "peer addresses used for bootstrap")
	broadcastMode := flag.Bool("broadcast", false, "run gossiper in broadcast mode")
	hookURL := flag.String("hookURL", "", "A URL that is called each time a new message comes, for example http://127.0.0.1:4000/callback")
	watchInURL := flag.String("watchInURL", "", "A URL that is called each time the watcher notifies for an incoming message, for example http://127.0.0.1:4000/watchIn")
	watchOutURL := flag.String("watchOutURL", "", "A URL that is called each time the watcher notifies for an outgoing message, for example http://127.0.0.1:4000/watchOut")
	routeTimer := flag.Int("rtimer", 0, "route rumors sending period in seconds, 0 to disable sending of route rumors (default)")
	rootSharedData := flag.String("sharedir", "_SharedData/", "the directory to store data about indexed files")
	rootDownloadedFiles := flag.String("downdir", "_Downloads/", "the directory to store downloaded and reconstructed files")

	numParticipants := flag.Int("numParticipants", -1, "number of participants in the Paxos consensus box.")
	nodeIndex := flag.Int("nodeIndex", -1, "index of the node with respect to all the participants")
	paxosRetry := flag.Int("paxosRetry", defaultPaxosRetry, "number of seconds a Paxos proposer waits until retrying")

	flag.Parse()

	if *numParticipants < 0 {
		Logger.Error().Msg("please specify a number of participants with --numParticipants")
		return
	}

	if *nodeIndex < 0 {
		Logger.Error().Msg("please specify a node index --nodeIndex")
		return
	}

	UIAddress := "127.0.0.1:" + *UIPort
	gossipAddress := *gossipAddr
	bootstrapAddr := strings.Split(*peers, ",")

	// The work happens in the gossip folder. You should not touch the code in
	// this package.
	fac := gossip.GetFactory()
	g, err := fac.New(gossipAddress, *ownName, *antiEntropy, *routeTimer,
		*rootSharedData, *rootDownloadedFiles, *numParticipants, *nodeIndex,
		*paxosRetry)
	if err != nil {
		panic(err)
	}

	controller := NewController(*ownName, UIAddress, gossipAddress, *broadcastMode, g, bootstrapAddr...)

	if *hookURL != "" {
		parsedURL, err := url.Parse(*hookURL)
		if err != nil {
			Logger.Err(err)
		} else {
			controller.hookURL = parsedURL
		}
	}

	if *watchInURL != "" {
		parsedURL, err := url.Parse(*watchInURL)
		if err != nil {
			Logger.Err(err)
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ins := g.Watch(ctx, true)
			go func() {
				for {
					p := <-ins
					sendWatch(p, parsedURL)
				}
			}()
		}
	}

	if *watchOutURL != "" {
		parsedURL, err := url.Parse(*watchOutURL)
		if err != nil {
			Logger.Err(err)
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ins := g.Watch(ctx, false)
			go func() {
				for {
					p := <-ins
					sendWatch(p, parsedURL)
				}
			}()
		}
	}

	ready := make(chan struct{})
	go g.Run(ready)
	defer g.Stop()
	<-ready

	if bootstrapAddr[0] != "" {
		g.AddAddresses(bootstrapAddr...)
	}

	controller.Run()
}

func sendWatch(p gossip.CallbackPacket, u *url.URL) {
	msgBuf, err := json.Marshal(p)
	if err != nil {
		Logger.Err(err).Msg("failed to marshal packet: %v")
		return
	}

	req := &http.Request{
		Method: "POST",
		URL:    u,
		Header: map[string][]string{
			"Content-Type": {"application/json; charset=UTF-8"},
		},
		Body: ioutil.NopCloser(bytes.NewReader(msgBuf)),
	}

	Logger.Info().Msgf("sending a post watch to %s", u)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		Logger.Err(err).Msgf("failed to call watch to %s", u)
	}
}
