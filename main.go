package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
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
	finder_model "github.com/filecoin-project/storetheindex/api/v0/finder/model"
	finder_schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
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
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var log = logging.Logger("idxperiment")

func main() {
	logging.SetLogLevel("idxperiment", "debug")

	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatal(err)
	}

	ds, err := leveldb.NewDatastore("data/datastore", nil)
	if err != nil {
		log.Fatal(err)
	}

	bsDS, err := flatfs.CreateOrOpen("data/blockstore", flatfs.NextToLast(3), true)
	if err != nil {
		log.Fatal(err)
	}
	bs := blockstore.NewBlockstoreNoPrefix(bsDS)

	h, err := libp2p.New(libp2p.DisableRelay(), libp2p.ResourceManager(nil), libp2p.WithDialTimeout(time.Second*5))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	scraper, err := NewScraper(ctx, "https://cid.contact", bs, ds, h)
	if err != nil {
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
				log.Infof("Getting ad chain from provider %s (%d/%d)", provider.AddrInfo.ID, index, len(providers))
				index++
				lk.Unlock()

				func() {
					syncCtx, syncCancel := context.WithCancel(ctx)
					syncTimer := time.AfterFunc(time.Minute, func() {
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

					log.Infof("Syncing ads for %s", provider.AddrInfo.ID)
					lastAd, err := handle.syncAds(syncCtx)
					if err != nil {
						log.Error(err)
						syncTimer.Stop()
						syncCancel()
						return
					}

					log.Infof("Loading ads for %s", provider.AddrInfo.ID)
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
					log.Infof("Got %d ads from %s", len(ads), provider.AddrInfo.ID)
					lk.Unlock()
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
		float32(successCount)/float32(len(providers)),
	)
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

	log.Debugf("Created new provider handle for %s", provider.AddrInfo.ID)

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
		log.Debugf("Connecting to %s using httpsync", handle.provider.AddrInfo.ID)

		_syncer, err := handle.scraper.httpSync.NewSyncer(handle.provider.AddrInfo.ID, handle.provider.AddrInfo.Addrs[0], rl)
		if err != nil {
			return fail(fmt.Errorf("could not create http syncer: %w", err))
		}

		syncer = _syncer
	} else {
		log.Debugf("Connecting to %s using dtsync", handle.provider.AddrInfo.ID)

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

	// TODO: specific prototype?
	adsSSB := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adsSelector := adsSSB.ExploreRecursive(
		selector.RecursionLimitNone(),
		adsSSB.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", adsSSB.ExploreRecursiveEdge())
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

	syncer, err = handle.syncer(ctx)
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
		//
		// TODO: specific prototype?
		adNode, err := handle.scraper.linkSystem.Load(
			ipld.LinkContext{},
			cidlink.Link{Cid: currAdCid},
			basicnode.Prototype.Any,
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
	defaultTopic := "/indexer/ingest/mainnet"
	re := regexp.MustCompile("^/legs/head/([^/0]+)")
	for _, proto := range protos {
		if re.MatchString(proto) {
			defaultTopic = re.FindStringSubmatch(proto)[1]
			break
		}
	}
	return defaultTopic
}
