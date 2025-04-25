package pubsub

import (
	"encoding/binary"
	"fmt"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestMessageCache(t *testing.T) {
	mcache := NewMessageCache(3, 5)
	msgID := DefaultMsgIdFn

	msgs := make([]*pb.Message, 60)
	for i := range msgs {
		msgs[i] = makeTestMessage(i)
	}

	for i := 0; i < 10; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	remotePeer := peer.ID("foo")
	gids := mcache.AppendGossipIDs(nil, "test", remotePeer)
	if len(gids) != 10 {
		t.Fatalf("Expected 10 gossip IDs; got %d", len(gids))
	}

	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	mcache.Shift()
	for i := 10; i < 20; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	for i := 0; i < 20; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	gids = mcache.AppendGossipIDs(nil, "test", remotePeer)
	if len(gids) != 20 {
		t.Fatalf("Expected 20 gossip IDs; got %d", len(gids))
	}

	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		if mid != gids[10+i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		mid := msgID(msgs[i])
		if mid != gids[i-10] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	mcache.Shift()
	for i := 20; i < 30; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	mcache.Shift()
	for i := 30; i < 40; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	mcache.Shift()
	for i := 40; i < 50; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	mcache.Shift()
	for i := 50; i < 60; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	if len(mcache.msgs) != 50 {
		t.Fatalf("Expected 50 messages in the cache; got %d", len(mcache.msgs))
	}

	for i := 0; i < 10; i++ {
		mid := msgID(msgs[i])
		_, ok := mcache.Get(mid)
		if ok {
			t.Fatalf("Message %d still in cache", i)
		}
	}

	for i := 10; i < 60; i++ {
		mid := msgID(msgs[i])
		m, ok := mcache.Get(mid)
		if !ok {
			t.Fatalf("Message %d not in cache", i)
		}

		if m.Message != msgs[i] {
			t.Fatalf("Message %d does not match cache", i)
		}
	}

	gids = mcache.AppendGossipIDs(nil, "test", remotePeer)
	if len(gids) != 30 {
		t.Fatalf("Expected 30 gossip IDs; got %d", len(gids))
	}

	for i := 0; i < 10; i++ {
		mid := msgID(msgs[50+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 10; i < 20; i++ {
		mid := msgID(msgs[30+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}

	for i := 20; i < 30; i++ {
		mid := msgID(msgs[10+i])
		if mid != gids[i] {
			t.Fatalf("GossipID mismatch for message %d", i)
		}
	}
}

func TestMessageCacheGossipTracing(t *testing.T) {
	mcache := NewMessageCache(3, 5)
	msgID := DefaultMsgIdFn

	// Create test messages
	msgs := make([]*pb.Message, 10)
	for i := range msgs {
		msgs[i] = makeTestMessage(i)
	}

	// Put messages in cache
	for i := 0; i < 10; i++ {
		mcache.Put(&Message{Message: msgs[i]})
	}

	peer1 := peer.ID("peer1")
	peer2 := peer.ID("peer2")

	// First request should return all messages for both peers
	gids1 := mcache.AppendGossipIDs(nil, "test", peer1)
	if len(gids1) != 10 {
		t.Fatalf("Expected 10 gossip IDs for peer1; got %d", len(gids1))
	}

	gids2 := mcache.AppendGossipIDs(nil, "test", peer2)
	if len(gids2) != 10 {
		t.Fatalf("Expected 10 gossip IDs for peer2; got %d", len(gids2))
	}

	// Record that we sent IHAVEs to peer1 for first 5 messages
	for i := 0; i < 5; i++ {
		mid := msgID(msgs[i])
		mcache.RecordGossipEmission(mid, peer1)
	}

	// peer1 should only get remaining 5 messages
	gids1 = mcache.AppendGossipIDs(nil, "test", peer1)
	if len(gids1) != 5 {
		t.Fatalf("Expected 5 gossip IDs for peer1 after recording; got %d", len(gids1))
	}
	// Verify they're the right messages (last 5)
	for i := 5; i < 5; i++ {
		mid := msgID(msgs[i+5])
		if mid != gids1[i] {
			t.Fatalf("GossipID mismatch for message %d", i+5)
		}
	}

	// peer2 should still get all 10 messages
	gids2 = mcache.AppendGossipIDs(nil, "test", peer2)
	if len(gids2) != 10 {
		t.Fatalf("Expected 10 gossip IDs for peer2; got %d", len(gids2))
	}

	// Shift the window and add new messages
	mcache.Shift()
	mcache.Shift()
	mcache.Shift()
	for i := 10; i < 15; i++ {
		mcache.Put(&Message{Message: makeTestMessage(i)})
	}

	// Both peers should get only the new messages
	gids1 = mcache.AppendGossipIDs(nil, "test", peer1)
	if len(gids1) != 5 {
		t.Fatalf("Expected 5 new gossip IDs after shift; got %d", len(gids1))
	}
	gids2 = mcache.AppendGossipIDs(nil, "test", peer2)
	if len(gids2) != 5 {
		t.Fatalf("Expected 5 new gossip IDs for peer2 after shift; got %d", len(gids2))
	}

	// Verify they're the new messages
	for i := 0; i < 5; i++ {
		mid := msgID(makeTestMessage(i + 10))
		if mid != gids1[i] {
			t.Fatalf("GossipID mismatch for new message %d", i+10)
		}
	}

	// Record IHAVE for all current messages to peer1
	gids1 = mcache.AppendGossipIDs(nil, "test", peer1)
	for _, mid := range gids1 {
		mcache.RecordGossipEmission(mid, peer1)
	}

	// peer1 should now get no messages
	gids1 = mcache.AppendGossipIDs(nil, "test", peer1)
	if len(gids1) != 0 {
		t.Fatalf("Expected no gossip IDs after recording all; got %d", len(gids1))
	}

	// peer2 should still get the 5 messages
	gids2 = mcache.AppendGossipIDs(nil, "test", peer2)
	if len(gids2) != 5 {
		t.Fatalf("Expected 5 gossip IDs for peer2 after recording all; got %d", len(gids2))
	}

	// Shift enough times to clear the window
	for i := 0; i < 5; i++ {
		mcache.Shift()
	}

	// Add a new message
	newMsg := makeTestMessage(20)
	mcache.Put(&Message{Message: newMsg})

	// Both peers should get the new message
	gids1 = mcache.AppendGossipIDs(nil, "test", peer1)
	if len(gids1) != 1 {
		t.Fatalf("Expected 1 gossip ID after window clear; got %d", len(gids1))
	}
	if gids1[0] != msgID(newMsg) {
		t.Fatal("GossipID mismatch for new message after window clear")
	}
	gids2 = mcache.AppendGossipIDs(nil, "test", peer2)
	if len(gids2) != 1 {
		t.Fatalf("Expected 1 gossip ID for peer2 after window clear; got %d", len(gids2))
	}
	if gids2[0] != msgID(newMsg) {
		t.Fatal("GossipID mismatch for new message after window clear")
	}
}

func makeTestMessage(n int) *pb.Message {
	seqno := make([]byte, 8)
	binary.BigEndian.PutUint64(seqno, uint64(n))
	data := []byte(fmt.Sprintf("%d", n))
	topic := "test"
	return &pb.Message{
		Data:  data,
		Topic: &topic,
		From:  []byte("test"),
		Seqno: seqno,
	}
}
