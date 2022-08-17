package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sync"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	finder_model "github.com/filecoin-project/storetheindex/api/v0/finder/model"
	finder_schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-core"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/willscott/index-observer/safemapds"
	"golang.org/x/time/rate"
)

var log = logging.Logger("idxperiment")

type AdvertisementResult struct {
	Cid           cid.Cid
	Advertisement finder_schema.Advertisement
}

// Scraper connects to providers listed by the indexer and queries their
// advertisements
type Scraper struct {
	IndexerURL     string
	Datastore      datastore.Datastore
	Advertisements chan AdvertisementResult
	Host           host.Host
}

func NewScraper(indexerURL string, datastore datastore.Datastore, host host.Host) (*Scraper, error) {
	return &Scraper{
		IndexerURL:     indexerURL,
		Datastore:      datastore,
		Advertisements: make(chan AdvertisementResult, 10),
		Host:           host,
	}, nil
}

func (scraper *Scraper) Run(ctx context.Context) error {

	providers, err := scraper.GetProviders(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, provider := range providers {
		wg.Add(1)

		func(provider finder_model.ProviderInfo) {
			defer wg.Done()

			log.Infof("Getting advertisements for provider %s", provider.AddrInfo.ID)

			syncer, lsys, closer, err := scraper.GetSyncer(ctx, provider.AddrInfo)
			if err != nil {
				log.Errorf("Could not get syncer for provider %s: %v", provider.AddrInfo.ID, err)
				return
			}
			defer closer()

			ads, err := scraper.GetAdvertisements(ctx, syncer, lsys, provider)
			if err != nil {
				log.Errorf("Could not get advertisements for provider %s: %v", provider.AddrInfo.ID, err)
				return
			}

			for ad := range ads {
				log.Infof("Getting entries for advertisement %s", ad.AdvertisementCid)

				if err := ad.Err; err != nil {
					log.Errorf("Failed to get ad: %v", err)
					continue
				}

				entries, err := scraper.GetEntries(ctx, syncer, lsys, ad.Advertisement)
				if err != nil {
					log.Errorf("Failed to get entries for ad %s: %v", ad.AdvertisementCid, err)
					continue
				}

				entryCount := 0
				for entry := range entries {
					if err := entry.Err; err != nil {
						log.Errorf("Failed to get entry for ad: %v", err)
						continue
					}
					entryCount++
				}

				log.Infof("Got %d entries for advertisement %s", entryCount, ad.AdvertisementCid)
			}
		}(provider)
	}
	wg.Wait()

	return nil
}

func (scraper *Scraper) GetProviders(ctx context.Context) ([]finder_model.ProviderInfo, error) {
	endpoint, err := url.Parse(scraper.IndexerURL)
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

type GetAdvertisementsResult struct {
	Advertisement    finder_schema.Advertisement
	AdvertisementCid cid.Cid

	// If non-nil, other values are invalid
	Err error
}

// Get advertisements in order starting from the genesis; has to load all the
// ads starting from the most recent and reverse them
func (scraper *Scraper) GetAdvertisements(
	ctx context.Context,
	syncer legs.Syncer,
	lsys ipld.LinkSystem,
	provider finder_model.ProviderInfo,
) (<-chan GetAdvertisementsResult, error) {
	// Error shorthand
	fail := func(err error) (<-chan GetAdvertisementsResult, error) {
		return nil, err
	}

	headCid, err := syncer.GetHead(ctx)
	if err != nil {
		return fail(fmt.Errorf("syncer could not get head: %w", err))
	}

	// emptyMH, err := multihash.Encode([]byte{}, multihash.IDENTITY)
	// if err != nil {
	// 	return fail(err)
	// }
	// emptyNode := cidlink.Link{Cid: cid.NewCidV1(uint64(cid.Raw), emptyMH)}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSelector := legs.ExploreRecursiveWithStop(
		selector.RecursionLimitNone(),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}),
		nil,
	)

	queuedAdvertisement := cidlink.Link{Cid: headCid}

	log.Infof("Syncing...")
	if err := syncer.Sync(ctx, queuedAdvertisement.Cid, adSelector); err != nil {
		return fail(fmt.Errorf("failed to sync: %w", err))
	}
	log.Infof("Done syncing")

	// Advertisements stored from most recent (reverse of required order)
	var adsReverse []GetAdvertisementsResult

	adsChan := make(chan GetAdvertisementsResult, 10)
	go func() {
		defer close(adsChan)

		// Goroutine error shorthand
		sendFail := func(err error) {
			adsChan <- GetAdvertisementsResult{
				Err: err,
			}
		}

		for queuedAdvertisement.Cid != cid.Undef {

			log.Debugf("Reading advertisement %s from %s", queuedAdvertisement, provider.AddrInfo.ID)

			if err := ctx.Err(); err != nil {
				sendFail(err)
				break
			}

			adNode, err := lsys.Load(ipld.LinkContext{}, queuedAdvertisement, basicnode.Prototype.Any)
			if err != nil {
				sendFail(fmt.Errorf("failed to load advertisement: %w", err))
				break
			}

			ad, err := finder_schema.UnwrapAdvertisement(adNode)
			if err != nil {
				sendFail(fmt.Errorf("failed to unwrap advertisement: %w", err))
				break
			}

			log.Infof("Adding ad CID: %s", queuedAdvertisement.Cid)
			adsReverse = append(adsReverse, GetAdvertisementsResult{
				Advertisement:    *ad,
				AdvertisementCid: queuedAdvertisement.Cid,
			})

			if ad.PreviousID == nil {
				log.Debugf("Got to genesis advertisement for provider: %s", provider.AddrInfo.ID)
				break
			}

			previousIDLink := *ad.PreviousID
			if previousIDCidLink, ok := previousIDLink.(cidlink.Link); ok {
				// oldQueuedAdvertisement = queuedAdvertisement
				queuedAdvertisement = previousIDCidLink

				if queuedAdvertisement.Cid == cid.Undef {
					break
				}
			} else {
				sendFail(fmt.Errorf("previous ID wasn't a CID link"))
				break
			}
		}

		// Send ads from end to beginning
		for i := len(adsReverse) - 1; i >= 0; i-- {
			adsChan <- adsReverse[i]
		}
	}()

	return adsChan, nil
}

type GetEntriesResult struct {
	Entry multihash.Multihash

	// If non-nil, other values are invalid
	Err error
}

func (scraper *Scraper) GetEntries(
	ctx context.Context,
	syncer legs.Syncer,
	lsys ipld.LinkSystem,
	ad finder_schema.Advertisement,
) (<-chan GetEntriesResult, error) {
	// Error shorthand
	fail := func(err error) (<-chan GetEntriesResult, error) {
		return nil, err
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	entriesSelector := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Entries", ssb.ExploreAll(ssb.Matcher()))
			efsb.Insert("Next", ssb.ExploreRecursiveEdge())
		}),
	)

	queuedEntry := ad.Entries

	if err := syncer.Sync(ctx, queuedEntry.(cidlink.Link).Cid, entriesSelector.Node()); err != nil {
		return fail(err)
	}

	entriesChan := make(chan GetEntriesResult, 10)
	go func() {
		defer close(entriesChan)

		// Goroutine error shorthand
		sendFail := func(err error) {
			entriesChan <- GetEntriesResult{
				Err: err,
			}
		}

		i := 0

		for queuedEntry != nil {
			entriesNode, err := lsys.Load(ipld.LinkContext{}, queuedEntry, basicnode.Prototype.Any)
			if err != nil {
				sendFail(fmt.Errorf("failed to load entries node %v: %w", queuedEntry, err))
				break
			}

			entryChunk, err := finder_schema.UnwrapEntryChunk(entriesNode)
			if err != nil {
				sendFail(fmt.Errorf("failed to unwrap entry chunk: %w", err))
				break
			}

			for _, entry := range entryChunk.Entries {
				entriesChan <- GetEntriesResult{
					Entry: entry,
				}
			}

			log.Debugf("Processing entry index %d (len %d)", i, len(entryChunk.Entries))

			i++

			if entryChunk.Next == nil {
				break
			}

			queuedEntry = *entryChunk.Next
		}
	}()

	return entriesChan, nil
}

func (scraper *Scraper) GetSyncer(
	ctx context.Context,
	addrInfo peer.AddrInfo,
) (syncer legs.Syncer, lsys linking.LinkSystem, closer func(), err error) {
	// Error shorthand
	fail := func(err error) (legs.Syncer, linking.LinkSystem, func(), error) {
		// Run the closer if a syncer got successfully opened before the error
		if closer != nil {
			closer()
		}
		return nil, linking.LinkSystem{}, nil, err
	}

	// Set up link system
	lsys = cidlink.DefaultLinkSystem()
	store := memstore.Store{}
	lsys.SetReadStorage(&store)
	lsys.SetWriteStorage(&store)
	lsys.TrustedStorage = true

	rl := rate.NewLimiter(rate.Inf, 0)

	// Open either HTTP or DataTransfer syncer depending on addrInfo's supported
	// protocols
	if isHTTP(addrInfo) {
		log.Debugf("Using httpsync for %s", addrInfo.ID)

		sync := httpsync.NewSync(lsys, &http.Client{}, func(i peer.ID, c cid.Cid) {})
		closer = func() {
			sync.Close()
		}

		_syncer, err := sync.NewSyncer(addrInfo.ID, addrInfo.Addrs[0], rl)
		if err != nil {
			return fail(fmt.Errorf("could not create http syncer: %w", err))
		}

		syncer = _syncer
	} else {
		log.Debugf("Using dtsync for %s", addrInfo.ID)

		scraper.Host.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, time.Hour*24*7)

		ds := safemapds.NewMapDatastore()

		if err := scraper.Host.Connect(ctx, addrInfo); err != nil {
			return fail(fmt.Errorf("failed to connect to host: %w", err))
		}
		scraper.Host.ConnManager().Protect(addrInfo.ID, "scraper")

		sync, err := dtsync.NewSync(scraper.Host, ds, lsys, func(i peer.ID, c cid.Cid) {})
		if err != nil {
			return fail(fmt.Errorf("could not create datastore: %w", err))
		}
		closer = func() {
			sync.Close()
			scraper.Host.ConnManager().Unprotect(addrInfo.ID, "scraper")
		}

		protos, err := scraper.Host.Peerstore().GetProtocols(addrInfo.ID)
		if err != nil {
			return fail(fmt.Errorf("could not get protocols: %w", err))
		}

		topic := topicFromSupportedProtocols(protos)

		syncer = sync.NewSyncer(addrInfo.ID, topic, rl)
	}

	return syncer, lsys, closer, nil
}

func main() {
	logging.SetLogLevel("idxperiment", "debug")

	datastore := safemapds.NewMapDatastore()

	host, err := libp2p.New(libp2p.DisableRelay())
	if err != nil {
		log.Fatal(err)
	}

	scraper, err := NewScraper("https://cid.contact", datastore, host)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if err := scraper.Run(ctx); err != nil {
		log.Fatal(err)
	}
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
