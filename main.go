package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"regexp"
	"sync"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	dtgs "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	finder_model "github.com/filecoin-project/storetheindex/api/v0/finder/model"
	finder_schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-core"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/willscott/index-observer/safemapds"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var log = logging.Logger("idxperiment")

func main() {
	logging.SetLogLevel("idxperiment", "debug")

	ctx := context.Background()

	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatal(err)
	}

	// ds, err := leveldb.NewDatastore("", nil)
	ds, err := leveldb.NewDatastore("data/datastore", nil)
	if err != nil {
		log.Fatal(err)
	}

	// bsDS, err := flatfs.CreateOrOpen("data/blockstore", flatfs.NextToLast(3), true)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	bsDS := safemapds.NewMapDatastore()
	bs := blockstore.NewBlockstoreNoPrefix(bsDS)

	// Load or save+generate priv key
	var priv crypto.PrivKey
	privBytes, err := os.ReadFile("data/peerkey")
	if err != nil {
		_priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		if err != nil {
			log.Fatal(err)
		}
		priv = _priv
		privBytes, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			log.Fatal(err)
		}
		os.WriteFile("data/peerkey", privBytes, 0644)
	} else {
		priv, err = crypto.UnmarshalPrivateKey(privBytes)
		if err != nil {
			log.Fatal(err)
		}
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
		libp2p.ResourceManager(nil),
		libp2p.WithDialTimeout(time.Second*15),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Peer ID: %s", h.ID())

	scraper, err := NewScraper(ctx, "http://localhost:3000", bs, ds, h)
	// scraper, err := NewScraper(ctx, "https://cid.contact", bs, ds, h)
	if err != nil {
		log.Fatal(err)
	}

	// Engine gets its own host (but with the same identity) - because it's
	// easiest to let engine create its own datatransfer, but only one is
	// allowed per host
	hEng, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/3203"),
	)
	if err != nil {
		log.Fatal(err)
	}
	eng, err := engine.New(
		engine.WithHost(hEng),
		// engine.WithDataTransfer(scraper.dataTransfer),
		engine.WithDatastore(ds),
		engine.WithPublisherKind(engine.DataTransferPublisher),
		engine.WithPurgeCacheOnStart(true),
	)
	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(os.Stdin)
	log.Infof("Press Enter to continue...")
	reader.ReadLine()

	if err := eng.Start(ctx); err != nil {
		log.Fatal(err)
	}

	providers, err := scraper.GetProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()
	successCount := 0
	index := 0
	var lk sync.Mutex
	var wg sync.WaitGroup
	threadCount := 0

	// Wait for datastore migration
	time.Sleep(time.Millisecond * 100)

	for i := 0; i < 20; i++ {
		wg.Add(1)

		lk.Lock()
		threadCount++
		lk.Unlock()

		go func() {
			defer wg.Done()

			for {
				lk.Lock()
				if index >= len(providers) {
					lk.Unlock()
					break
				}
				provider := providers[index]
				log.Infof("Getting ad chain from provider %s (%d/%d)", provider.AddrInfo, index, len(providers))
				index++
				lk.Unlock()

				func() {
					syncCtx, syncCancel := context.WithCancel(ctx)
					syncTimer := time.AfterFunc(time.Second*30, func() {
						log.Warnf("Sync timed out for %s, attempting to cancel", provider.AddrInfo.ID)
						syncCancel()
					})

					handle, err := scraper.NewProviderHandle(syncCtx, provider)
					if err != nil {
						log.Error(err)
						syncTimer.Stop()
						syncCancel()
						return
					}

					log := log.With("peerID", provider.AddrInfo.ID)

					lastAd, err := handle.syncAds(syncCtx)
					if err != nil {
						log.Error(err)
						syncTimer.Stop()
						syncCancel()
						return
					}

					log.Infof("Last ad: %s", lastAd)

					ads, err := handle.loadAds(syncCtx, lastAd)
					if err != nil {
						log.Error(err)
						syncTimer.Stop()
						syncCancel()
						return
					}

					syncTimer.Stop()
					syncCancel()

					lk.Lock()
					successCount++
					log.Infof("Got %d ads", len(ads))
					lk.Unlock()

					var adLinks []ipld.Link
					for i, ad := range ads {
						var md metadata.Metadata
						if err := md.UnmarshalBinary(ad.Metadata); err != nil {
							log.Error("failed to unmarshal ad metadata: %v", err)
							return
						}

						if md.Get(multicodec.TransportGraphsyncFilecoinv1) == nil {
							log.Warn("Provider wasn't advertising for graphsync")
							return
						}

						mdUpdated := metadata.New(metadata.Bitswap{})
						mdUpdatedBytes, err := mdUpdated.MarshalBinary()
						if err != nil {
							log.Error("Could not marshal updated metadata")
							return
						}
						ad.Metadata = mdUpdatedBytes

						if i != 0 {
							ad.PreviousID = &adLinks[i-1]
						}

						if err := ad.Sign(priv); err != nil {
							log.Error("Failed to sign ad: %v", err)
							return
						}

						cid, err := eng.Publish(ctx, ad)
						if err != nil {
							log.Error("Failed to publish ad chain: %v", err)
							return
						}

						adLinks = append(adLinks, cidlink.Link{Cid: cid})

						log.Infof("Published ad chain with CID %s", cid)
					}

					if _, err := eng.PublishLatestHTTP(ctx, "127.0.0.1:3001"); err != nil {
						log.Error("Failed to publish HTTP: %v", err)
					}
				}()
			}

			lk.Lock()
			threadCount--
			log.Infof("Thread finished (%d remaining)", threadCount)
			lk.Unlock()
		}()
	}

	wg.Wait()

	log.Infof("Finished indexing after %s", time.Since(startTime))
	log.Infof(
		"Successes: %d | Failures: %d | Success Rate: %f%%",
		successCount,
		len(providers)-successCount,
		float32(successCount)/float32(len(providers))*100.0,
	)

	doneChan := make(chan os.Signal, 1)
	signal.Notify(doneChan, os.Interrupt)
	<-doneChan
	signal.Stop(doneChan)

	log.Info("Shutting down...")

	if err := eng.Shutdown(); err != nil {
		log.Fatal(err)
	}

	log.Info("Stopped")
}

type Scraper struct {
	indexerURL   string
	blockstore   blockstore.Blockstore
	datastore    datastore.Batching
	host         host.Host
	linkSystem   ipld.LinkSystem
	dataTransfer datatransfer.Manager
	gsExchange   graphsync.GraphExchange
	dtSync       *dtsync.Sync
	httpSync     *httpsync.Sync
}

func NewScraper(
	ctx context.Context,
	indexerURL string,
	bs blockstore.Blockstore,
	ds datastore.Batching,
	h host.Host,
) (*Scraper, error) {
	// Set up link system
	lsys := storeutil.LinkSystemForBlockstore(bs)

	dtNetwork := dtnet.NewFromLibp2pHost(h)
	gsNetwork := gsnet.NewFromLibp2pHost(h)
	gsExchange := gsimpl.New(
		ctx,
		gsNetwork,
		lsys,
		gsimpl.MaxInProgressIncomingRequests(100),
		gsimpl.MaxMemoryResponder(8<<30),
		gsimpl.MaxMemoryPerPeerResponder(1<<30),
		gsimpl.SendMessageTimeout(time.Second*10),
		gsimpl.MessageSendRetries(2),
	)
	gsTransport := dtgs.NewTransport(h.ID(), gsExchange)

	dt, err := dtimpl.NewDataTransfer(ds, dtNetwork, gsTransport)
	if err != nil {
		return nil, err
	}

	if err := dt.Start(ctx); err != nil {
		return nil, err
	}

	httpSync := httpsync.NewSync(lsys, &http.Client{}, func(i peer.ID, c cid.Cid) {})

	dtSync, err := dtsync.NewSyncWithDT(
		h,
		dt,
		gsExchange,
		&lsys,
		func(i peer.ID, c cid.Cid) {},
	)
	if err != nil {
		return nil, err
	}

	return &Scraper{
		indexerURL:   indexerURL,
		blockstore:   bs,
		datastore:    ds,
		host:         h,
		linkSystem:   lsys,
		dataTransfer: dt,
		gsExchange:   gsExchange,
		httpSync:     httpSync,
		dtSync:       dtSync,
	}, nil
}

func (scraper *Scraper) Close() {
	scraper.dtSync.Close()
	scraper.httpSync.Close()
}

func (scraper *Scraper) GetProviders(ctx context.Context) ([]finder_model.ProviderInfo, error) {
	endpoint, err := url.Parse(scraper.indexerURL)
	if err != nil {
		return nil, err
	}
	endpoint.Path = path.Join(endpoint.Path, "/providers")

	providersRes, err := http.DefaultClient.Get(endpoint.String())
	if err != nil {
		return nil, err
	}
	defer providersRes.Body.Close()

	var providers []finder_model.ProviderInfo
	if err := json.NewDecoder(providersRes.Body).Decode(&providers); err != nil {
		return nil, err
	}

	return providers, nil
}

func (scraper *Scraper) NewProviderHandle(
	ctx context.Context,
	provider finder_model.ProviderInfo,
) (*ProviderHandle, error) {
	// Namespaced logger
	namespace := provider.AddrInfo.ID.String()
	log := log.Named("handle").Named(namespace[len(namespace)-5:])

	return &ProviderHandle{
		scraper:  scraper,
		provider: provider,
		info:     provider,
		log:      log,
	}, nil
}

// A handle allowing communication with a specific provider - must be closed
// when done
type ProviderHandle struct {
	scraper  *Scraper
	provider finder_model.ProviderInfo
	info     finder_model.ProviderInfo
	log      *zap.SugaredLogger
}

func (handle *ProviderHandle) syncer(ctx context.Context) (legs.Syncer, error) {
	// Error shorthand
	fail := func(err error) (legs.Syncer, error) {
		return nil, err
	}

	var syncer legs.Syncer

	rl := rate.NewLimiter(rate.Inf, 0)

	// Open either HTTP or DataTransfer syncer depending on addrInfo's supported
	// protocols
	if isHTTP(handle.provider.AddrInfo) {
		_syncer, err := handle.scraper.httpSync.NewSyncer(handle.provider.AddrInfo.ID, handle.provider.AddrInfo.Addrs[0], rl)
		if err != nil {
			return fail(fmt.Errorf("could not create http syncer: %w", err))
		}

		syncer = _syncer
	} else {
		handle.scraper.host.Peerstore().AddAddrs(handle.provider.AddrInfo.ID, handle.provider.AddrInfo.Addrs, time.Hour*24*7)

		if err := handle.scraper.host.Connect(ctx, handle.provider.AddrInfo); err != nil {
			return fail(fmt.Errorf("failed to connect to host: %w", err))
		}

		protos, err := handle.scraper.host.Peerstore().GetProtocols(handle.provider.AddrInfo.ID)
		if err != nil {
			return fail(fmt.Errorf("could not get protocols: %w", err))
		}

		topic := topicFromSupportedProtocols(protos)

		syncer = handle.scraper.dtSync.NewSyncer(handle.provider.AddrInfo.ID, topic, rl)
	}

	return syncer, nil
}

// Recursively syncs all ads from last to first, returning the last ad cid on
// success
func (handle *ProviderHandle) syncAds(ctx context.Context) (cid.Cid, error) {
	// Error shorthand
	fail := func(err error) (cid.Cid, error) {
		return cid.Undef, err
	}

	adsSSB := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adsSelector := adsSSB.ExploreRecursive(
		selector.RecursionLimitNone(),
		adsSSB.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", adsSSB.ExploreRecursiveEdge())
			efsb.Insert("Provider", adsSSB.Matcher())
			efsb.Insert("Addresses", adsSSB.Matcher())
			efsb.Insert("Signature", adsSSB.Matcher())
			efsb.Insert("ContextID", adsSSB.Matcher())
			efsb.Insert("Metadata", adsSSB.Matcher())
			efsb.Insert("IsRm", adsSSB.Matcher())
		}),
	).Node()

	// Start with the last advertisement
	syncer, err := handle.syncer(ctx)
	if err != nil {
		return cid.Undef, err
	}
	lastAd, err := syncer.GetHead(ctx)
	if err != nil {
		return fail(fmt.Errorf("syncer could not get advertisements head: %v", err))
	}

	// syncer, err = handle.syncer(ctx)
	if err != nil {
		return cid.Undef, err
	}
	if err := syncer.Sync(ctx, lastAd, adsSelector); err != nil {
		return fail(fmt.Errorf("failed to sync advertisements: %v", err))
	}

	return lastAd, nil
}

// Reads ads backwards from the starting point, and writes them into a slice in
// the correct forward order
func (handle *ProviderHandle) loadAds(
	ctx context.Context,
	lastAdvertisement cid.Cid,
) ([]finder_schema.Advertisement, error) {
	// Error shorthand
	fail := func(err error) ([]finder_schema.Advertisement, error) {
		return nil, err
	}

	currAdCid := lastAdvertisement

	var adsReversed []finder_schema.Advertisement
	for currAdCid != cid.Undef {
		// Respect context cancellation
		if err := ctx.Err(); err != nil {
			return fail(err)
		}

		// Load the current advertisement node
		adNode, err := handle.scraper.linkSystem.Load(
			ipld.LinkContext{},
			cidlink.Link{Cid: currAdCid},
			finder_schema.AdvertisementPrototype,
		)
		if err != nil {
			return fail(fmt.Errorf("failed to load advertisement: %v", err))
		}

		// Convert node to advertisement struct
		ad, err := finder_schema.UnwrapAdvertisement(adNode)
		if err != nil {
			return fail(fmt.Errorf("failed to unwrap advertisement: %v", err))
		}

		// Write the ad
		adsReversed = append(adsReversed, *ad)

		// Write in the next advertisement to be processed (the previous
		// advertisement in the chain), which should be cid.Undef if the end was
		// reached
		if ad.PreviousID == nil {
			currAdCid = cid.Undef
		} else if nextLinkToProcess, ok := (*ad.PreviousID).(cidlink.Link); ok {
			currAdCid = nextLinkToProcess.Cid
		} else {
			return fail(fmt.Errorf("previous advertisement ID wasn't a CID link"))
		}
	}

	// Flip ads into correct order
	ads := make([]finder_schema.Advertisement, len(adsReversed))
	for i, ad := range adsReversed {
		ads[len(ads)-i-1] = ad
	}

	return ads, nil
}

func isHTTP(p peer.AddrInfo) bool {
	isHttpPeerAddr := false
	for _, addr := range p.Addrs {
		for _, p := range addr.Protocols() {
			if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
				isHttpPeerAddr = true
				break
			}
		}
	}
	return isHttpPeerAddr
}

func topicFromSupportedProtocols(protos []string) string {
	topic := "/indexer/ingest/mainnet"
	re := regexp.MustCompile(`^/legs/head/([\w/]+)/[\d\.]+`)
	for _, proto := range protos {
		if re.MatchString(proto) {
			topic = re.FindStringSubmatch(proto)[1]
			break
		}
	}
	return topic
}
