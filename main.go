package main

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/dsadapter"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-core"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/willscott/index-observer/safemapds"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var log = logging.Logger("idxperiment")

func main() {
	logging.SetLogLevel("idxperiment", "debug")

	datastore := safemapds.NewMapDatastore()

	host, err := libp2p.New(libp2p.DisableRelay(), libp2p.ResourceManager(nil))
	if err != nil {
		log.Fatal(err)
	}

	scraper, err := NewScraper("https://cid.contact", datastore, host)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	providers, err := scraper.GetProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	handle, err := scraper.NewProviderHandle(ctx, providers[0])
	if err != nil {
		log.Fatal(err)
	}

	entries, err := handle.GetEntries(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for entry := range entries {
		if err := entry.Error; err != nil {
			log.Fatalf("Failed to get entry: %v", err)
		}

		// log.Infof("Entry: %#v", entry)
	}
}

type Scraper struct {
	indexerURL string
	datastore  datastore.Datastore
	host       host.Host
	linkSystem ipld.LinkSystem
}

func NewScraper(
	indexerURL string,
	datastore datastore.Datastore,
	host host.Host,
) (*Scraper, error) {
	// Set up link system
	lsys := cidlink.DefaultLinkSystem()
	store := &dsadapter.Adapter{
		Wrapped: datastore,
		EscapingFunc: func(raw string) string {
			return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(raw))
		},
	}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	lsys.TrustedStorage = true

	return &Scraper{
		indexerURL: indexerURL,
		datastore:  datastore,
		host:       host,
		linkSystem: lsys,
	}, nil
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
	var closer func()

	// Error shorthand
	fail := func(err error) (*ProviderHandle, error) {
		// Run the closer if a syncer got successfully opened before the error
		if closer != nil {
			closer()
		}
		return nil, err
	}

	// Namespaced logger
	namespace := provider.AddrInfo.ID.String()
	log := log.Named("handle").Named(namespace[len(namespace)-5:])

	var syncer legs.Syncer

	rl := rate.NewLimiter(rate.Inf, 0)

	// Open either HTTP or DataTransfer syncer depending on addrInfo's supported
	// protocols
	if isHTTP(provider.AddrInfo) {
		log.Debugf("Connecting to %s using httpsync", provider.AddrInfo.ID)

		sync := httpsync.NewSync(scraper.linkSystem, &http.Client{}, func(i peer.ID, c cid.Cid) {})
		closer = func() {
			sync.Close()
		}

		_syncer, err := sync.NewSyncer(provider.AddrInfo.ID, provider.AddrInfo.Addrs[0], rl)
		if err != nil {
			return fail(fmt.Errorf("could not create http syncer: %w", err))
		}

		syncer = _syncer
	} else {
		log.Debugf("Connecting to %s using dtsync", provider.AddrInfo.ID)

		scraper.host.Peerstore().AddAddrs(provider.AddrInfo.ID, provider.AddrInfo.Addrs, time.Hour*24*7)

		ds := safemapds.NewMapDatastore()

		if err := scraper.host.Connect(ctx, provider.AddrInfo); err != nil {
			return fail(fmt.Errorf("failed to connect to host: %w", err))
		}
		scraper.host.ConnManager().Protect(provider.AddrInfo.ID, "scraper")

		sync, err := dtsync.NewSync(scraper.host, ds, scraper.linkSystem, func(i peer.ID, c cid.Cid) {})
		if err != nil {
			return fail(fmt.Errorf("could not create datastore: %w", err))
		}
		closer = func() {
			sync.Close()
			scraper.host.ConnManager().Unprotect(provider.AddrInfo.ID, "scraper")
		}

		protos, err := scraper.host.Peerstore().GetProtocols(provider.AddrInfo.ID)
		if err != nil {
			return fail(fmt.Errorf("could not get protocols: %w", err))
		}

		topic := topicFromSupportedProtocols(protos)

		syncer = sync.NewSyncer(provider.AddrInfo.ID, topic, rl)
	}

	log.Debugf("Created new provider handle for %s", provider.AddrInfo.ID)

	return &ProviderHandle{
		Scraper: scraper,
		syncer:  syncer,
		info:    provider,
		Close:   closer,
		log:     log,
	}, nil
}

type EntryResult struct {
	// If non-nil, other members are invalid
	Error error

	Multihash multihash.Multihash
	Remove    bool
}

// A handle allowing communication with a specific provider - must be closed
// when done
type ProviderHandle struct {
	*Scraper
	Close  func()
	syncer legs.Syncer
	info   finder_model.ProviderInfo
	log    *zap.SugaredLogger
}

// Internally starts a goroutine to retrieve every entry (in order) from the
// provider - there may be multiple entries for a single multihash in case it
// was added and subsequently removed
//
// The context will be respected in the goroutine even after GetEntries() itself
// returns, so it should not be cancelled until the returned channel is closed
// and drained, or early stop is truly intended
func (handle *ProviderHandle) GetEntries(
	ctx context.Context,
) (<-chan EntryResult, error) {
	// Error shorthand
	fail := func(err error) (<-chan EntryResult, error) {
		return nil, err
	}

	log := handle.log

	log.Debugf("Loading advertisements")

	// Sync and load ads
	lastAdCid, err := handle.syncAds(ctx)
	if err != nil {
		return fail(err)
	}
	ads, err := handle.loadAds(ctx, lastAdCid)
	if err != nil {
		return fail(err)
	}

	// For each advertisement, sync and load its entries and push them to the
	// results channel

	log.Debugf("Getting entries for %d ads", len(ads))

	results := make(chan EntryResult)
	go func() error {
		defer close(results)

		// Goroutine error shorthand
		sendFail := func(err error) error {
			results <- EntryResult{
				Error: err,
			}
			return err
		}

		for i, ad := range ads {
			log.Debugf("Getting entries for ad %d", i)

			firstEntryChunkCid, err := handle.syncEntryChunks(ctx, ad)
			if err != nil {
				return sendFail(err)
			}

			entryChunks := handle.loadEntryChunks(ctx, firstEntryChunkCid)

			for entryChunk := range entryChunks {
				// If there's an error getting an entry chunk, fail early
				if err := entryChunk.Error; err != nil {
					return sendFail(err)
				}

				// Forward each of the entries to the results chan
				for _, entry := range entryChunk.Entries {
					results <- EntryResult{
						Multihash: entry,
						Remove:    ad.IsRm,
					}
				}
			}
		}

		return nil
	}()

	return results, nil
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
	lastAd, err := handle.syncer.GetHead(ctx)
	if err != nil {
		return fail(fmt.Errorf("syncer could not get advertisements head: %v", err))
	}

	if err := handle.syncer.Sync(ctx, lastAd, adsSelector); err != nil {
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
		adNode, err := handle.linkSystem.Load(
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

// Recursively syncs all entry chunks in a chain, returning first entry chunk
// cid on success
func (handle *ProviderHandle) syncEntryChunks(ctx context.Context, ad finder_schema.Advertisement) (cid.Cid, error) {
	// Error shorthand
	fail := func(err error) (cid.Cid, error) {
		return cid.Undef, err
	}

	firstEntry := ad.Entries.(cidlink.Link).Cid

	// TODO: specific prototype?
	entriesSSB := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	entriesSelector := entriesSSB.ExploreRecursive(
		selector.RecursionLimitNone(),
		entriesSSB.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Entries", entriesSSB.ExploreAll(entriesSSB.Matcher()))
			efsb.Insert("Next", entriesSSB.ExploreRecursiveEdge())
		}),
	).Node()

	if err := handle.syncer.Sync(ctx, firstEntry, entriesSelector); err != nil {
		return fail(fmt.Errorf("failed to sync entries: %v", err))
	}

	return firstEntry, nil
}

type EntryChunkResult struct {
	finder_schema.EntryChunk

	// If non-nil, other fields are invalid
	Error error
}

// Context will be respected until the returned channel has been closed, the
// passed context should not be cancelled until completion or early stop is
// desired
func (handle *ProviderHandle) loadEntryChunks(
	ctx context.Context,
	firstEntryChunk cid.Cid,
) <-chan EntryChunkResult {
	entryChunks := make(chan EntryChunkResult, 10)

	currEntryChunk := firstEntryChunk

	go func() error {
		defer close(entryChunks)

		// Goroutine error shorthand
		sendFail := func(err error) error {
			entryChunks <- EntryChunkResult{
				Error: err,
			}
			return err
		}

		for currEntryChunk != cid.Undef {
			entryChunkNode, err := handle.linkSystem.Load(
				ipld.LinkContext{},
				cidlink.Link{Cid: currEntryChunk},
				basicnode.Prototype.Any,
			)
			if err != nil {
				return sendFail(fmt.Errorf("failed to load entries node %v: %v", currEntryChunk, err))
			}

			entryChunk, err := finder_schema.UnwrapEntryChunk(entryChunkNode)
			if err != nil {
				return sendFail(fmt.Errorf("failed to unwrap entry chunk: %v", err))
			}

			entryChunks <- EntryChunkResult{
				EntryChunk: *entryChunk,
			}

			if entryChunk.Next == nil {
				currEntryChunk = cid.Undef
			} else if nextEntryChunkToProcess, ok := (*entryChunk.Next).(cidlink.Link); ok {
				currEntryChunk = nextEntryChunkToProcess.Cid
			} else {
				return sendFail(fmt.Errorf("next entry chunk wasn't a CID link"))
			}
		}

		return nil
	}()

	return entryChunks
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
