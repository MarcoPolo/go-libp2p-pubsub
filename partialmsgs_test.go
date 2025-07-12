package pubsub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/big"
	"math/rand"
	"testing"

	"github.com/libp2p/go-libp2p-pubsub/internal/merkle"
	"github.com/libp2p/go-libp2p/core/peer"
)

type mockNetworkPartialMessages struct {
	t *testing.T
	m map[peer.ID][]*RPC

	allSentMsgs map[peer.ID][]*RPC

	handlers map[peer.ID]*PartialMessageExtension
}

func (m *mockNetworkPartialMessages) clearMsgs() {
	m.allSentMsgs = make(map[peer.ID][]*RPC)
}
func (m *mockNetworkPartialMessages) addPeers() {
	for a := range m.handlers {
		for b := range m.handlers {
			if a == b {
				continue
			}
			m.handlers[a].AddPeer(b)
			m.handlers[b].AddPeer(a)
		}
	}
}

func (m *mockNetworkPartialMessages) removePeers() {
	for a := range m.handlers {
		for b := range m.handlers {
			if a == b {
				continue
			}
			m.handlers[a].RemovePeer(b)
			m.handlers[b].RemovePeer(a)
		}
	}
}

func (m *mockNetworkPartialMessages) handleRPCs() bool {
	for id, h := range m.handlers {
		if len(m.m[id]) > 0 {
			var rpc *RPC
			rpc, m.m[id] = m.m[id][0], m.m[id][1:]
			h.HandleRPC(rpc)
		}
	}
	moreLeft := false
	for id := range m.handlers {
		if len(m.m[id]) > 0 {
			moreLeft = true
			break
		}
	}
	return moreLeft
}

func (m *mockNetworkPartialMessages) sendRPC(id peer.ID, rpc *RPC, _ bool) {
	if id == "" {
		panic("empty peer ID")
	}
	fmt.Printf("Sending RPC to %s: %+v\n", id, rpc)
	m.m[id] = append(m.m[id], rpc)
	m.allSentMsgs[id] = append(m.allSentMsgs[id], rpc)
}

const testPartialMessageLeaves = 8

// testPartialMessage represents a partial message where parts can be verified
// by a merkle tree commitment. By convention, there are
// `testPartialMessageLeaves` parts.
type testPartialMessage struct {
	Commitment []byte
	Parts      [testPartialMessageLeaves][]byte
	Proofs     [testPartialMessageLeaves][]merkle.ProofStep

	republish func(*testPartialMessage)
	onErr     func(error)
}

// AvailableParts returns a bitmap of available parts
func (pm *testPartialMessage) AvailableParts() ([]byte, error) {
	var temp big.Int
	for i, part := range pm.Parts {
		var b uint = 0
		if len(part) > 0 {
			b = 1
		}
		temp.SetBit(&temp, i, b)
	}
	return temp.Bytes(), nil
}

// ExtendFromEncodedPartialMessage implements PartialMessage.
func (pm *testPartialMessage) ExtendFromEncodedPartialMessage(data []byte) {
	var decoded testPartialMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		pm.onErr(err)
		return
	}

	// Verify
	if !bytes.Equal(pm.Commitment, decoded.Commitment) {
		pm.onErr(errors.New("commitment mismatch"))
		return
	}

	var added bool
	for i, part := range decoded.Parts {
		if len(pm.Parts[i]) > 0 {
			continue
		}
		if len(part) == 0 {
			continue
		}
		proof := decoded.Proofs[i]
		if len(proof) == 0 {
			continue
		}
		if !merkle.VerifyProof(part, pm.Commitment, proof) {
			pm.onErr(errors.New("proof verification failed"))
			return
		}

		pm.Parts[i] = part
		pm.Proofs[i] = proof
		added = true
	}

	nonEmptyParts := 0
	for i := range pm.Parts {
		if len(pm.Parts[i]) > 0 {
			nonEmptyParts++
		}
	}

	if added {
		pm.republish(pm)
	}

}

// GroupID implements PartialMessage.
func (pm *testPartialMessage) GroupID() []byte {
	return pm.Commitment
}

// MissingParts implements PartialMessage.
func (pm *testPartialMessage) MissingParts() ([]byte, error) {
	setAny := false
	var temp big.Int
	for i, part := range pm.Parts {
		var b uint = 1
		if len(part) > 0 {
			setAny = true
			b = 0
		}
		temp.SetBit(&temp, i, b)
	}
	if !setAny {
		return nil, nil
	}
	return temp.Bytes(), nil

}

// PartialMessageBytesFromMetadata implements PartialMessage.
func (pm *testPartialMessage) PartialMessageBytesFromMetadata(metadata []byte) ([]byte, []byte, error) {
	var temp big.Int
	temp.SetBytes(metadata)

	var tempMessage testPartialMessage
	tempMessage.Commitment = pm.Commitment
	for i := range temp.BitLen() {
		if temp.Bit(i) == 1 {
			if len(pm.Parts[i]) == 0 {
				// We can't fulfill this part
				continue
			}
			// Clear the bit, we provided the data
			temp.SetBit(&temp, i, 0)

			tempMessage.Parts[i] = pm.Parts[i]
			tempMessage.Proofs[i] = pm.Proofs[i]
		}
	}

	b, err := json.Marshal(tempMessage)
	if err != nil {
		return nil, nil, err
	}

	return b, temp.Bytes(), nil
}

func newFullTestMessage(topic string, r io.Reader) (*testPartialMessage, error) {
	out := &testPartialMessage{}
	for i := range out.Parts {
		out.Parts[i] = make([]byte, 8)
		if _, err := io.ReadFull(r, out.Parts[i]); err != nil {
			return nil, err
		}
	}
	out.Commitment = merkle.MerkleRoot(out.Parts[:])
	for i := range out.Parts {
		out.Proofs[i] = merkle.MerkleProof(out.Parts[:], i)
	}
	return out, nil
}

func TestPartialMessages(t *testing.T) {
	nw := &mockNetworkPartialMessages{
		t:           t,
		m:           make(map[peer.ID][]*RPC),
		allSentMsgs: make(map[peer.ID][]*RPC),
		handlers:    make(map[peer.ID]*PartialMessageExtension),
	}

	peer1 := peer.ID("1")
	peer2 := peer.ID("2")
	topic := "test-topic"

	republish := func(from PartialMessageExtension) func(pm *testPartialMessage) {
		return func(pm *testPartialMessage) {
			from.PublishPartial(topic, pm, PartialMessagePublishOptions{})
		}

	}

	var h1, h2 PartialMessageExtension
	h1 = PartialMessageExtension{
		NewPartialMessage: func(topic string, groupID []byte) (PartialMessage, error) {
			return &testPartialMessage{
				Commitment: groupID,
				republish:  republish(h1),
			}, nil
		},
		ValidateRequestMetadata: func(topic string, metadata []byte) error {
			if len(metadata) > 1024 {
				return errors.New("metadata too large")
			}
			return nil
		},
		GroupTTLByHeatbeat: 5,

		sendRPC: func(p peer.ID, r *RPC, urgent bool) {
			r.from = peer1
			nw.sendRPC(p, r, urgent)
		},
		topicsForPeer: func(p peer.ID) iter.Seq[string] {
			return func(yield func(string) bool) {
				yield(topic)
			}
		},
		meshPeers: func() iter.Seq[peer.ID] {
			return func(yield func(peer.ID) bool) {
				yield(peer2)
			}
		},
	}
	h2 = h1 // Copy most settings
	h2.meshPeers = func() iter.Seq[peer.ID] {
		return func(yield func(peer.ID) bool) {
			yield(peer1)
		}
	}
	h2.sendRPC = func(p peer.ID, r *RPC, urgent bool) {
		r.from = peer2
		nw.sendRPC(p, r, urgent)
	}
	h2.NewPartialMessage = func(topic string, groupID []byte) (PartialMessage, error) {
		return &testPartialMessage{
			Commitment: groupID,
			republish:  republish(h2),
		}, nil
	}

	h1.init()
	h2.init()

	nw.handlers[peer1] = &h1
	nw.handlers[peer2] = &h2

	rand := rand.New(rand.NewSource(0))

	assertNoEmptyRPCs := func() {
		for _, msgs := range nw.allSentMsgs {
			for _, msg := range msgs {
				if msg.Partial.Size() == 0 {
					msg.Partial = nil
				}
				if msg.Size() == 0 {
					t.Fatal("empty message")
				}
			}
		}

	}

	t.Run("h1 has all the data. h2 requests it", func(t *testing.T) {
		defer nw.clearMsgs()
		nw.addPeers()
		defer nw.removePeers()
		defer func() {
			// Assert no more state is left
			for range 10 {
				h1.Heartbeat()
				h2.Heartbeat()
			}
			if len(h1.statePerTopicPerGroup) != 0 || len(h2.statePerTopicPerGroup) != 0 {
				t.Fatal("h1 and h2 should have cleaned up all their state")
			}
		}()
		defer assertNoEmptyRPCs()

		h1Msg, err := newFullTestMessage(topic, rand)
		h1Msg.republish = republish(h1)
		if err != nil {
			t.Fatal(err)
		}

		// h1 knows the full message
		h1.PublishPartial(topic, h1Msg, PartialMessagePublishOptions{})

		// h2 only knows the group id
		h2Msg := &testPartialMessage{Commitment: h1Msg.Commitment, republish: republish(h2)}
		h2.PublishPartial(topic, h2Msg, PartialMessagePublishOptions{})

		// Handle all RPCs
		for nw.handleRPCs() {
		}

		// Assert h2 has the full message
		missing, err := h2Msg.MissingParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 has all the data. h2 doesn't know anything", func(t *testing.T) {
		defer nw.clearMsgs()
		nw.addPeers()
		defer nw.removePeers()
		defer func() {
			// Assert no more state is left
			for range 10 {
				h1.Heartbeat()
				h2.Heartbeat()
			}
			if len(h1.statePerTopicPerGroup) != 0 || len(h2.statePerTopicPerGroup) != 0 {
				t.Fatal("h1 and h2 should have cleaned up all their state")
			}
		}()
		defer assertNoEmptyRPCs()

		h1Msg, err := newFullTestMessage(topic, rand)
		h1Msg.republish = republish(h1)
		if err != nil {
			t.Fatal(err)
		}

		// h1 knows the full message
		h1.PublishPartial(topic, h1Msg, PartialMessagePublishOptions{})

		// h2 knows nothing
		var h2Msg *testPartialMessage
		backup := h2.NewPartialMessage
		defer func() {
			h2.NewPartialMessage = backup
		}()
		h2.NewPartialMessage = func(topic string, groupID []byte) (PartialMessage, error) {
			h2Msg = &testPartialMessage{
				Commitment: groupID,
				republish:  republish(h2),
			}
			return h2Msg, nil
		}

		// Handle all RPCs
		for nw.handleRPCs() {
		}

		// Assert h2 has the full message
		missing, err := h2Msg.MissingParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 has all the data. h2 has some of it", func(t *testing.T) {
		defer nw.clearMsgs()
		nw.addPeers()
		defer nw.removePeers()
		defer func() {
			// Assert no more state is left
			for range 10 {
				h1.Heartbeat()
				h2.Heartbeat()
			}
			if len(h1.statePerTopicPerGroup) != 0 || len(h2.statePerTopicPerGroup) != 0 {
				t.Fatal("h1 and h2 should have cleaned up all their state")
			}
		}()
		defer assertNoEmptyRPCs()

		h1Msg, err := newFullTestMessage(topic, rand)
		h1Msg.republish = republish(h1)
		if err != nil {
			t.Fatal(err)
		}

		// h1 knows the full message
		h1.PublishPartial(topic, h1Msg, PartialMessagePublishOptions{})

		// h2 only knows part of it
		h2Msg := &testPartialMessage{Commitment: h1Msg.Commitment, republish: republish(h2)}
		for i := range h2Msg.Parts {
			if i%2 == 0 {
				h2Msg.Parts[i] = h1Msg.Parts[i]
				h2Msg.Proofs[i] = h1Msg.Proofs[i]
			}
		}
		h2.PublishPartial(topic, h2Msg, PartialMessagePublishOptions{})

		emptyMsg := &testPartialMessage{}
		emptyMetadata, _ := emptyMsg.MissingParts()
		if bytes.Equal(nw.m[peer1][0].Partial.Iwant.Metadata, emptyMetadata) {
			t.Fatal("h2 should not be asking for the full message")
		}

		// Handle all RPCs
		for nw.handleRPCs() {
		}

		// Assert that h2 only sent a single Partial IWANT
		count := 0
		for _, rpc := range nw.allSentMsgs[peer1] {
			if rpc.Partial.Iwant != nil {
				count++
			}
		}
		if count != 1 {
			t.Fatal("h2 should only have sent a single Partial IWANT")
		}

		// Assert h2 has the full message
		missing, err := h2Msg.MissingParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatal("h2 should have the full message")
		}
	})

	t.Run("h1 has half the data. h2 has the other half of it", func(t *testing.T) {
		defer nw.clearMsgs()
		nw.addPeers()
		defer nw.removePeers()
		defer func() {
			// Assert no more state is left
			for range 10 {
				h1.Heartbeat()
				h2.Heartbeat()
			}
			if len(h1.statePerTopicPerGroup) != 0 || len(h2.statePerTopicPerGroup) != 0 {
				t.Fatal("h1 and h2 should have cleaned up all their state")
			}
		}()
		defer assertNoEmptyRPCs()

		fullMsg, err := newFullTestMessage(topic, rand)
		if err != nil {
			t.Fatal(err)
		}
		h1Msg := &testPartialMessage{Commitment: fullMsg.Commitment, republish: republish(h1)}
		for i := range fullMsg.Parts {
			if i%2 == 0 {
				h1Msg.Parts[i] = fullMsg.Parts[i]
				h1Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// h2 only knows part of it
		h2Msg := &testPartialMessage{Commitment: fullMsg.Commitment, republish: republish(h2)}
		for i := range h2Msg.Parts {
			if i%2 == 1 {
				h2Msg.Parts[i] = fullMsg.Parts[i]
				h2Msg.Proofs[i] = fullMsg.Proofs[i]
			}
		}

		// h1 knows half
		h1.PublishPartial(topic, h1Msg, PartialMessagePublishOptions{})
		// h2 knows the other half
		h2.PublishPartial(topic, h2Msg, PartialMessagePublishOptions{})

		emptyMsg := &testPartialMessage{}
		emptyMetadata, _ := emptyMsg.MissingParts()
		if bytes.Equal(nw.m[peer1][0].Partial.Iwant.Metadata, emptyMetadata) {
			t.Fatal("h2 should not be asking for the full message")
		}

		// Handle all RPCs
		for nw.handleRPCs() {
		}

		// Assert that h2 only sent a single Partial IWANT
		count := 0
		for _, rpc := range nw.allSentMsgs[peer1] {
			if rpc.Partial.Iwant != nil {
				count++
			}
		}
		if count != 1 {
			t.Fatal("h2 should only have sent a single Partial IWANT")
		}

		// Assert h2 has the full message
		missing, err := h2Msg.MissingParts()
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatal("h2 should have the full message")
		}
	})
}

func TestTestPartialMessage(t *testing.T) {
	randReader := rand.New(rand.NewSource(0))
	msg1, err := newFullTestMessage("test-topic", randReader)
	if err != nil {
		t.Fatal(err)
	}

	parts, err := msg1.MissingParts()
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 0 {
		t.Fatal("expected no missing parts")
	}
}
