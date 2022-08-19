package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/ipld/go-ipld-prime/codec/dagjson"
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

// Scraper connects to providers listed by the indexer and queries their
// advertisements
type Scraper struct {
	IndexerURL string
	Datastore  datastore.Datastore
	Host       host.Host
}

func NewScraper(indexerURL string, datastore datastore.Datastore, host host.Host) (*Scraper, error) {
	return &Scraper{
		IndexerURL: indexerURL,
		Datastore:  datastore,
		Host:       host,
	}, nil
}

func (scraper *Scraper) Run(ctx context.Context) error {

	providers, err := scraper.GetProviders(ctx)
	if err != nil {
		return err
	}

	// Set up link system
	lsys := cidlink.DefaultLinkSystem()
	store := memstore.Store{}
	lsys.SetReadStorage(&store)
	lsys.SetWriteStorage(&store)
	lsys.TrustedStorage = true

	var wg sync.WaitGroup
	for _, provider := range providers {
		wg.Add(1)

		func(provider finder_model.ProviderInfo) {
			defer wg.Done()

			log.Infof("Getting advertisements for provider %s", provider.AddrInfo.ID)

			syncer, closer, err := scraper.GetSyncer(ctx, provider.AddrInfo, lsys)
			if err != nil {
				log.Errorf("Could not get syncer for provider %s: %v", provider.AddrInfo.ID, err)
				return
			}
			defer closer()

			// SAFETY: ads is read-only after this point and needs no lock - do
			// not write to it
			ads, err := scraper.GetAdvertisements(ctx, syncer, lsys, provider)
			if err != nil {
				log.Errorf("Could not get advertisements for provider %s: %v", provider.AddrInfo.ID, err)
				return
			}

			// SAFETY: no lock because each goroutine touches only its own index
			entriesNested := make([][]multihash.Multihash, len(ads))
			var wg sync.WaitGroup

			var adIndexLk sync.Mutex
			nextAdIndex := 0
			threadsCtx, threadsCancel := context.WithCancel(ctx)
			defer threadsCancel()
			for threadIndex := 0; threadIndex < 4; threadIndex++ {
				wg.Add(1)

				go func() {
					defer wg.Done()
					for {
						if threadsCtx.Err() != nil {
							break
						}

						adIndexLk.Lock()
						adIndex := nextAdIndex
						nextAdIndex++
						adIndexLk.Unlock()

						if adIndex >= len(ads) {
							break
						}

						ad := ads[adIndex]

						adNode, err := ad.ToNode()
						if err != nil {
							log.Errorf("Failed to get ad node: %v", err)
							return
						}
						var adBytes bytes.Buffer
						if err := dagjson.Encode(adNode, &adBytes); err != nil {
							log.Errorf("Failed to encode ad: %v", err)
							return
						}
						adHash := sha256.Sum256(adBytes.Bytes())
						adMH, _ := multihash.Encode(adHash[:16], multihash.SHA2_256)
						adCid := cid.NewCidV1(cid.DagJSON, adMH)

						var entries <-chan GetEntriesResult
						maxAttempts := 5
						for i := 1; i <= maxAttempts; i++ {
							log.Infof("Getting entries for advertisement %s (attempt %d/%d)", adCid, i, maxAttempts)
							_entries, err := scraper.GetEntries(threadsCtx, syncer, lsys, ad)
							if err != nil {
								log.Errorf("Failed to get entries for ad %s: %v", adCid, err)
								if i == maxAttempts {
									threadsCancel()
									return
								}
								time.Sleep(time.Second)
								continue
							}
							entries = _entries
							break
						}

						entryCount := 0
						for entry := range entries {
							if err := entry.Err; err != nil {
								log.Errorf("Failed to get entry for ad: %v", err)
								continue
							}

							entriesNested[adIndex] = append(entriesNested[adIndex], entry.Entry)

							entryCount++
						}

						log.Infof("Got %d entries for for advertisement %s (%d/%d)", entryCount, adCid, adIndex, len(ads))
					}
				}()
			}

			wg.Wait()
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

// Get advertisements in order starting from the genesis; has to load all the
// ads starting from the most recent and reverse them
func (scraper *Scraper) GetAdvertisements(
	ctx context.Context,
	syncer legs.Syncer,
	lsys ipld.LinkSystem,
	provider finder_model.ProviderInfo,
) ([]finder_schema.Advertisement, error) {
	// Error shorthand
	fail := func(err error) ([]finder_schema.Advertisement, error) {
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

	var adsReverse []finder_schema.Advertisement
	for queuedAdvertisement.Cid != cid.Undef {

		log.Debugf("Reading advertisement %s from %s", queuedAdvertisement, provider.AddrInfo.ID)

		if err := ctx.Err(); err != nil {
			return fail(err)
		}

		adNode, err := lsys.Load(ipld.LinkContext{}, queuedAdvertisement, basicnode.Prototype.Any)
		if err != nil {
			return fail(fmt.Errorf("failed to load advertisement: %w", err))
		}

		ad, err := finder_schema.UnwrapAdvertisement(adNode)
		if err != nil {
			return fail(fmt.Errorf("failed to unwrap advertisement: %w", err))
		}

		log.Infof("Adding ad CID: %s", queuedAdvertisement.Cid)
		adsReverse = append(adsReverse, *ad)

		if ad.PreviousID == nil {
			log.Debugf("Got to genesis advertisement for provider: %s", provider.AddrInfo.ID)
			break
		}

		previousIDLink := *ad.PreviousID
		if previousIDCidLink, ok := previousIDLink.(cidlink.Link); ok {
			queuedAdvertisement = previousIDCidLink

			if queuedAdvertisement.Cid == cid.Undef {
				break
			}
		} else {
			return fail(fmt.Errorf("previous ID wasn't a CID link"))
		}
	}

	// Flip ads into correct order
	ads := make([]finder_schema.Advertisement, len(adsReverse))
	for i, ad := range adsReverse {
		ads[len(adsReverse)-i-1] = ad
	}

	return ads, nil
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
	entriesChan := make(chan GetEntriesResult, 10)

	// Error shorthand
	fail := func(err error) (<-chan GetEntriesResult, error) {
		close(entriesChan)
		return entriesChan, err
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
	lsys linking.LinkSystem,
) (syncer legs.Syncer, closer func(), err error) {
	// Error shorthand
	fail := func(err error) (legs.Syncer, func(), error) {
		// Run the closer if a syncer got successfully opened before the error
		if closer != nil {
			closer()
		}
		return nil, nil, err
	}

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

	return syncer, closer, nil
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
