package main

import (
	"context"
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

var log = logging.Logger("test")

type AdvertisementResult struct {
	Cid           cid.Cid
	Advertisement finder_schema.Advertisement
}

// Scraper connects to providers listed by the indexer and queries their
// advertisements
//
// See MarkProcessed()
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

	for _, provider := range providers {
		log.Infof("Getting advertisements for provider %s", provider.AddrInfo.ID)

		syncer, lsys, closer, err := scraper.GetSyncer(ctx, provider.AddrInfo)
		if err != nil {
			log.Errorf("Could not get syncer for provider %s: %v", provider.AddrInfo.ID, err)
			continue
		}
		defer closer()

		ads, err := scraper.GetAdvertisements(ctx, syncer, lsys, provider)
		if err != nil {
			log.Errorf("Could not get advertisements for provider %s: %v", provider.AddrInfo.ID, err)
			continue
		}

		for ad := range ads {
			if err := ad.Err; err != nil {
				log.Errorf("Failed to get ad: %v", err)
				continue
			}

			entries, err := scraper.GetEntries(ctx, syncer, lsys, ad.Advertisement)
			if err != nil {
				log.Errorf("Failed to get entries for ad %s: %v", ad.AdvertisementCid, err)
				continue
			}

			for entry := range entries {
				if err := entry.Err; err != nil {
					log.Errorf("Failed to get entry for ad: %v", err)
					continue
				}

				log.Debugf("Got entry %s for ad %s (rm: %v)", entry.Entry, ad.AdvertisementCid, entry.IsRm)
			}
		}
	}

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

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSelector := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}),
	).Node()

	queuedCid := headCid

	// Advertisements stored from most recent (reverse of required order)
	var adsReverse []finder_schema.Advertisement

	adsChan := make(chan GetAdvertisementsResult, 10)
	go func() {
		defer close(adsChan)

		// Goroutine error shorthand
		sendFail := func(err error) {
			adsChan <- GetAdvertisementsResult{
				Err: err,
			}
		}

		for queuedCid != cid.Undef {

			log.Debugf("Reading advertisement: %s", queuedCid)

			if err := ctx.Err(); err != nil {
				sendFail(err)
				break
			}

			if err := syncer.Sync(ctx, queuedCid, adSelector); err != nil {
				sendFail(fmt.Errorf("failed to sync: %w", err))
				break
			}

			adNode, err := lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: queuedCid}, basicnode.Prototype.Any)
			if err != nil {
				sendFail(fmt.Errorf("failed to load advertisement: %w", err))
				break
			}

			ad, err := finder_schema.UnwrapAdvertisement(adNode)
			if err != nil {
				sendFail(fmt.Errorf("failed to unwrap advertisement: %w", err))
				break
			}

			adsReverse = append(adsReverse, *ad)

			if ad.PreviousID == nil {
				break
			}

			previousIDLink := *ad.PreviousID
			if previousIDCidLink, ok := previousIDLink.(cidlink.Link); ok {
				queuedCid = previousIDCidLink.Cid
			} else {
				sendFail(fmt.Errorf("previous ID wasn't a CID link"))
				break
			}
		}

		// Send ads from end to beginning
		for i := len(adsReverse) - 1; i >= 0; i-- {
			adsChan <- GetAdvertisementsResult{
				Advertisement:    adsReverse[i],
				AdvertisementCid: queuedCid,
			}
		}
	}()

	return adsChan, nil
}

type EntriesResult struct {
	Entry multihash.Multihash
	IsRm  bool

	// If non-nil, other values are invalid
	Err error
}

func (scraper *Scraper) GetEntries(
	ctx context.Context,
	syncer legs.Syncer,
	lsys ipld.LinkSystem,
	ad finder_schema.Advertisement,
) (<-chan EntriesResult, error) {
	// Error shorthand
	fail := func(err error) (<-chan EntriesResult, error) {
		return nil, err
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	entriesSelector := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Entries", ssb.ExploreRecursiveEdge())
		}),
	)
	syncer.Sync(ctx, ad.Entries.(cidlink.Link).Cid, entriesSelector.Node())

	entriesNode, err := lsys.Load(ipld.LinkContext{}, ad.Entries, basicnode.Prototype.Any)
	if err != nil {
		return fail(fmt.Errorf("failed to load entries node: %w", err))
	}

	entryChunk, err := finder_schema.UnwrapEntryChunk(entriesNode)
	if err != nil {
		return fail(fmt.Errorf("failed to unwrap entry chunk: %w", err))
	}

	entriesChan := make(chan EntriesResult, 10)
	go func() {
		defer close(entriesChan)

		for _, entry := range entryChunk.Entries {
			entriesChan <- EntriesResult{
				Entry: entry,
				IsRm:  ad.IsRm,
			}
		}

		log.Warnf("TODO: read all entry chunks instead of just first")
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

		sync, err := dtsync.NewSync(scraper.Host, ds, lsys, func(i peer.ID, c cid.Cid) {})
		if err != nil {
			return fail(fmt.Errorf("could not create datastore: %w", err))
		}
		closer = func() {
			sync.Close()
		}

		if err := scraper.Host.Connect(ctx, addrInfo); err != nil {
			return fail(fmt.Errorf("failed to connect to host: %w", err))
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
	logging.SetLogLevel("test", "debug")

	datastore := safemapds.NewMapDatastore()

	host, err := libp2p.New()
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

func getProviderAdvertisements(ctx context.Context, provider finder_model.ProviderInfo) error {
	lsys := cidlink.DefaultLinkSystem()
	store := memstore.Store{}
	lsys.SetReadStorage(&store)
	lsys.SetWriteStorage(&store)
	lsys.TrustedStorage = true

	rl := rate.NewLimiter(rate.Inf, 0)
	var syncer legs.Syncer
	if isHTTP(provider.AddrInfo) {
		log.Infof("Using httpsync")

		sync := httpsync.NewSync(lsys, &http.Client{}, func(i peer.ID, c cid.Cid) {})
		defer sync.Close()
		_syncer, err := sync.NewSyncer(provider.AddrInfo.ID, provider.AddrInfo.Addrs[0], rl)
		if err != nil {
			return fmt.Errorf("could not create http syncer: %w", err)
		}
		syncer = _syncer
	} else {
		log.Infof("Using dtsync")

		host, err := libp2p.New()
		if err != nil {
			return fmt.Errorf("could not create p2p host: %w", err)
		}
		defer host.Close()
		host.Peerstore().AddAddrs(provider.AddrInfo.ID, provider.AddrInfo.Addrs, time.Hour*24*7)
		ds := safemapds.NewMapDatastore()
		sync, err := dtsync.NewSync(host, ds, lsys, func(i peer.ID, c cid.Cid) {})
		if err != nil {
			return fmt.Errorf("could not create datastore: %w", err)
		}
		defer sync.Close()
		if err := host.Connect(ctx, provider.AddrInfo); err != nil {
			return fmt.Errorf("failed to connect to host: %w", err)
		}
		protos, err := host.Peerstore().GetProtocols(provider.AddrInfo.ID)
		if err != nil {
			return fmt.Errorf("could not get protocols: %w", err)
		}
		topic := topicFromSupportedProtocols(protos)
		syncer = sync.NewSyncer(provider.AddrInfo.ID, topic, rl)
	}

	headCid, err := syncer.GetHead(ctx)
	if err != nil {
		return fmt.Errorf("syncer could not get head: %w", err)
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSelector := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}),
	).Node()

	queuedCid := headCid

	for queuedCid != cid.Undef {
		if err := syncer.Sync(ctx, queuedCid, adSelector); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}

		adNode, err := lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: queuedCid}, basicnode.Prototype.Any)
		if err != nil {
			return fmt.Errorf("failed to load advertisement: %w", err)
		}

		ad, err := finder_schema.UnwrapAdvertisement(adNode)
		if err != nil {
			return fmt.Errorf("failed to unwrap advertisement: %w", err)
		}

		entriesSelector := ssb.ExploreRecursive(
			selector.RecursionLimitNone(),
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Entries", ssb.ExploreRecursiveEdge())
			}),
		)
		syncer.Sync(ctx, ad.Entries.(cidlink.Link).Cid, entriesSelector.Node())

		entriesNode, err := lsys.Load(ipld.LinkContext{}, ad.Entries, basicnode.Prototype.Any)
		if err != nil {
			return fmt.Errorf("failed to load entries node: %w", err)
		}

		entries, err := finder_schema.UnwrapEntryChunk(entriesNode)
		if err != nil {
			return fmt.Errorf("failed to unwrap entry chunk: %w", err)
		}

		log.Infof("===> GOT %s from %s (first entry chunk length %d)", queuedCid, provider.AddrInfo.ID, len(entries.Entries))

		if ad.PreviousID == nil {
			break
		}

		previousIDLink := *ad.PreviousID
		if previousIDCidLink, ok := previousIDLink.(cidlink.Link); ok {
			queuedCid = previousIDCidLink.Cid
		} else {
			return fmt.Errorf("previous ID wasn't a CID link")
		}
	}

	log.Infof("===> DONE WITH %s", provider.AddrInfo.ID)

	return nil
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
