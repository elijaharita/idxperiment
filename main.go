package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/filecoin-project/index-provider/engine"
	finder_model "github.com/filecoin-project/storetheindex/api/v0/finder/model"
	finder_schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/willscott/index-observer/safemapds"
	"golang.org/x/time/rate"
)

var log = logging.Logger("test")

type Scraper struct {
	engine *engine.Engine
}

func NewScraper() (*Scraper, error) {
	engine, err := engine.New()
	if err != nil {
		return nil, fmt.Errorf("could not create engine: %w", err)
	}

	return &Scraper{
		engine: engine,
	}, nil
}

func (scraper *Scraper) Run(ctx context.Context) {

}

func main() {
	logging.SetLogLevel("test", "debug")

	if err := test(); err != nil {
		log.Fatalf("%v", err)
	}
}

func test() error {
	ctx := context.Background()

	// Test index provider

	log.Infof("Testing index provider")

	// endpoint := "http://localhost:3000/providers"
	endpoint := "https://cid.contact/providers"
	providersRes, err := http.DefaultClient.Get(endpoint)
	if err != nil {
		return err
	}
	defer providersRes.Body.Close()
	var providers []finder_model.ProviderInfo
	if err := json.NewDecoder(providersRes.Body).Decode(&providers); err != nil {
		return err
	}
	log.Infof("providers: %#v", providers)

	var wg sync.WaitGroup
	for _, provider := range providers {
		wg.Add(1)
		go func(provider finder_model.ProviderInfo) {
			log.Infof("--- GETTING ADVERTISEMENTS FOR %s", provider.AddrInfo.ID)

			if err := getProviderAdvertisements(ctx, provider); err != nil {
				log.Errorf("Failed to get provider: %v", err)
				return
			}

			wg.Done()
		}(provider)
	}
	wg.Wait()

	return nil
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
