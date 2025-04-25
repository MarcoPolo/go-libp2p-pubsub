package pubsub

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

// NewMessageCache creates a sliding window cache that remembers messages for as
// long as `history` slots.
//
// When queried for messages to advertise, the cache only returns messages in
// the last `gossip` slots.
//
// The `gossip` parameter must be smaller or equal to `history`, or this
// function will panic.
//
// The slack between `gossip` and `history` accounts for the reaction time
// between when a message is advertised via IHAVE gossip, and the peer pulls it
// via an IWANT command.
func NewMessageCache(gossip, history int) *MessageCache {
	if gossip > history {
		err := fmt.Errorf("invalid parameters for message cache; gossip slots (%d) cannot be larger than history slots (%d)",
			gossip, history)
		panic(err)
	}
	return &MessageCache{
		msgs:    make(map[string]*Message),
		peertx:  make(map[string]map[peer.ID]int),
		history: make([][]CacheEntry, history),
		gossip:  gossip,
		msgID: func(msg *Message) string {
			return DefaultMsgIdFn(msg.Message)
		},
		ihave: make(map[string]map[peer.ID]struct{}),
	}
}

type MessageCache struct {
	msgs    map[string]*Message
	peertx  map[string]map[peer.ID]int
	history [][]CacheEntry
	gossip  int
	msgID   func(*Message) string
	ihave   map[string]map[peer.ID]struct{}
}

func (mc *MessageCache) SetMsgIdFn(msgID func(*Message) string) {
	mc.msgID = msgID
}

type CacheEntry struct {
	mid   string
	topic string
}

func (mc *MessageCache) Put(msg *Message) {
	mid := mc.msgID(msg)
	mc.msgs[mid] = msg
	mc.history[0] = append(mc.history[0], CacheEntry{mid: mid, topic: msg.GetTopic()})
}

func (mc *MessageCache) Get(mid string) (*Message, bool) {
	m, ok := mc.msgs[mid]
	return m, ok
}

func (mc *MessageCache) GetForPeer(mid string, p peer.ID) (*Message, int, bool) {
	m, ok := mc.msgs[mid]
	if !ok {
		return nil, 0, false
	}

	tx, ok := mc.peertx[mid]
	if !ok {
		tx = make(map[peer.ID]int)
		mc.peertx[mid] = tx
	}
	tx[p]++

	return m, tx[p], true
}

func (mc *MessageCache) Shift() {
	last := mc.history[len(mc.history)-1]
	for _, entry := range last {
		delete(mc.msgs, entry.mid)
		delete(mc.peertx, entry.mid)
		delete(mc.ihave, entry.mid)
	}
	for i := len(mc.history) - 2; i >= 0; i-- {
		mc.history[i+1] = mc.history[i]
	}
	mc.history[0] = nil
}

// RecordGossipEmission records that we informed peer about message mid
func (mc *MessageCache) RecordGossipEmission(mid string, p peer.ID) {
	peers, ok := mc.ihave[mid]
	if !ok {
		peers = make(map[peer.ID]struct{})
		mc.ihave[mid] = peers
	}
	peers[p] = struct{}{}
}

// AppendGossipIDs returns message IDs for a topic that haven't been advertised to the peer
func (mc *MessageCache) AppendGossipIDs(msgIDSlice []string, topic string, p peer.ID) []string {
	for _, entries := range mc.history[:mc.gossip] {
		for _, entry := range entries {
			if entry.topic == topic {
				// Check if we haven't informed this peer about this message
				if peers, ok := mc.ihave[entry.mid]; !ok || !hasPeer(peers, p) {
					msgIDSlice = append(msgIDSlice, entry.mid)
				}
			}
		}
	}

	return msgIDSlice
}

func hasPeer(peers map[peer.ID]struct{}, p peer.ID) bool {
	_, ok := peers[p]
	return ok
}
