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
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
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
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/willscott/index-observer/safemapds"
	"golang.org/x/time/rate"
)

var log = logging.Logger("test")

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

	providersRes, err := http.DefaultClient.Get("http://localhost:3000/providers")
	if err != nil {
		return err
	}
	defer providersRes.Body.Close()
	var providers []model.ProviderInfo
	if err := json.NewDecoder(providersRes.Body).Decode(&providers); err != nil {
		return err
	}
	log.Infof("providers: %#v", providers)

	var wg sync.WaitGroup
	for _, provider := range providers {
		wg.Add(1)
		go func(provider model.ProviderInfo) {
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

func getProviderAdvertisements(ctx context.Context, provider model.ProviderInfo) error {
	lsys := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: map[string][]byte{}}
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
			return err
		}
		syncer = _syncer
	} else {
		log.Infof("Using dtsync")

		host, err := libp2p.New()
		if err != nil {
			return err
		}
		defer host.Close()
		host.Peerstore().AddAddrs(provider.AddrInfo.ID, provider.AddrInfo.Addrs, time.Hour*24*7)
		ds := safemapds.NewMapDatastore()
		sync, err := dtsync.NewSync(host, ds, lsys, func(i peer.ID, c cid.Cid) {})
		if err != nil {
			return err
		}
		defer sync.Close()
		if err := host.Connect(ctx, provider.AddrInfo); err != nil {
			return err
		}
		protos, err := host.Peerstore().GetProtocols(provider.AddrInfo.ID)
		if err != nil {
			return err
		}
		topic := topicFromSupportedProtocols(protos)
		syncer = sync.NewSyncer(provider.AddrInfo.ID, topic, rl)
	}

	headCid, err := syncer.GetHead(ctx)
	if err != nil {
		return err
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSelector := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
	})

	identityMH, err := multihash.Encode(nil, multihash.IDENTITY)
	if err != nil {
		return err
	}

	queuedCid := headCid

	for queuedCid != cid.Undef {
		// selector := legs.ExploreRecursiveWithStop(selector.RecursionLimitDepth(5000), adSelector, cidlink.Link{Cid: provider.LastAdvertisement})
		selector := legs.ExploreRecursiveWithStop(selector.RecursionLimitDepth(500), adSelector, cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Raw), identityMH)})
		if err := syncer.Sync(ctx, queuedCid, selector); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}

		ad, err := lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: queuedCid}, basicnode.Prototype.Any)
		if err != nil {
			return fmt.Errorf("failed to load: %w", err)
		}

		previousID, err := ad.LookupByString("PreviousID")
		if err != nil {
			log.Infof("===> DONE WITH %s!", provider.AddrInfo.ID)
			break
		}

		previousIDLink, err := previousID.AsLink()
		if err != nil {
			return err
		}

		if previousIDCidLink, ok := previousIDLink.(cidlink.Link); ok {
			log.Infof("===> GOT %s from %s", queuedCid, provider.AddrInfo.ID)

			queuedCid = previousIDCidLink.Cid
		} else {
			return fmt.Errorf("previous ID wasn't a CID")
		}
	}

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
