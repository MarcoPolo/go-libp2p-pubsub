package pubsub

import (
	"bytes"
	"iter"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TODO: Handle PartialIDONTWANT
// TODO: Add gossip fallback
// TODO: Limit number of concurrent PartialIWANTs.
// TODO: Move this to a separate package?
// TODO: Validate topic.
// 	 - How do integrate this with existing mechanism in pubsub?
// TODO: Add invariant tester for PartialMessages interface
//   - Add check for requesting parts you don't have to result in a nil response
//   - Add check that PartialMessageBytesFromMetadata properly returns the "rest" of the request
// Question: How to configure scheduling RPCs?
// Question: Skip partial IHAVE for now?
// Question: I could have a user provided validation queue instead of requiring republishing
//   - But a user may need to republish anyways if they get parts out of band

const minGroupTTL = 3

type PartialMessage interface {
	GroupID() []byte

	// PartialMessageBytesFromMetadata takes in the opaque request metadata and
	// returns a encoded partial message that fulfills as much of the request as
	// possible. It also returns a opaque request metadata representing the
	// parts it could not fulfill. This MUST be empty if the implementation could
	// fulfill the whole request.
	//
	// An empty metadata should be treated the same as a request for all parts.
	//
	// If the Partial Message is empty, the implementation MUST return:
	// nil, metadata, nil.
	PartialMessageBytesFromMetadata(metadata []byte) ([]byte, []byte, error)

	// MissingParts returns the opaque request metadata representing the missing
	// parts. Used for PartialIWANT
	// Return an empty slice if you have all available parts.
	MissingParts() ([]byte, error)

	// ShouldRequest returns true if the given metadata has parts that this
	// partial message is missing. Used when handling PartialIHAVE.
	ShouldRequest(peerHasMetadata []byte) bool

	// AvailableParts returns the opaque metadata representing the parts this
	// message contains. Used for PartialIHAVE
	AvailableParts() ([]byte, error)

	// ExtendFromEncodedPartialMessage is how Gossipsub informs the application
	// of new parts from a peer. The implementation MUST be fast and
	// non-blocking as it is called from Gossipsub's goroutine. If some
	// expensive or blocking operation is required, the implementation should
	// spawn a goroutine or otherwise schedule the work to happen in a separate
	// goroutine.
	//
	// Republish the partial message to inform Gossipsub of any changes.
	ExtendFromEncodedPartialMessage(data []byte)
}

type partialMessagePeerState struct {
	wants    []byte
	has      []byte
	sentWant []byte
	// TODO: received IDONTWANTs
	recvdDontWant bool
}

func (ps *partialMessagePeerState) IsZero() bool {
	return len(ps.wants) == 0 && len(ps.has) == 0 && len(ps.sentWant) == 0 && !ps.recvdDontWant
}

type partialMessageStatePerTopicGroup struct {
	peerState      map[peer.ID]*partialMessagePeerState
	partialMessage PartialMessage
	lastHave       []byte
	groupTTL       int
}

func newPartialMessageStatePerTopicGroup(groupTTL int, pm PartialMessage) *partialMessageStatePerTopicGroup {
	return &partialMessageStatePerTopicGroup{
		peerState:      make(map[peer.ID]*partialMessagePeerState),
		partialMessage: pm,
		groupTTL:       max(groupTTL, minGroupTTL),
	}
}

func (s *partialMessageStatePerTopicGroup) clearPeerWants(peerID peer.ID) {
	if peerState, ok := s.peerState[peerID]; ok {
		if len(peerState.wants) > 0 {
			peerState.wants = nil
			if peerState.IsZero() {
				delete(s.peerState, peerID)
			}
		}
	}
}

func (s *partialMessageStatePerTopicGroup) setPeerWants(peerID peer.ID, wants []byte) {
	if len(wants) == 0 {
		s.clearPeerWants(peerID)
	}
	peerState, ok := s.peerState[peerID]
	if !ok {
		peerState = &partialMessagePeerState{}
		s.peerState[peerID] = peerState
	}
	peerState.wants = wants
}

func (s *partialMessageStatePerTopicGroup) setSentWants(peerID peer.ID, sentWant []byte) {
	peerState, ok := s.peerState[peerID]
	if !ok {
		peerState = &partialMessagePeerState{}
		s.peerState[peerID] = peerState
	}
	peerState.sentWant = sentWant
}

type PartialMessageExtension struct {
	// NewPartialMessage must return an empty PartialMessage. This message is if
	// we receive a partial message from a peer before we publish a partial
	// message of our own.
	NewPartialMessage func(topic string, groupID []byte) (PartialMessage, error)

	// ValidateRequestMetadata should be a fast function that performs some
	// basic sanity checks on the opaque request metadata. Checking that it is
	// within some size limit is recommended.
	ValidateRequestMetadata func(topic string, metadata []byte) error

	// GroupTTLByHeatbeat is how many heartbeats we store Group state for.
	GroupTTLByHeatbeat int

	statePerTopicPerGroup map[string]map[string]*partialMessageStatePerTopicGroup

	sendRPC       func(p peer.ID, r *RPC, urgent bool)
	meshPeers     func() iter.Seq[peer.ID]
	topicsForPeer func(p peer.ID) iter.Seq[string]
}

type PartialMessagePublishOptions struct {
	// EagerPush is data that will be eagerly pushed to peers in a PartialMessage
	EagerPush []byte
}

func (e *PartialMessageExtension) init() {
	e.statePerTopicPerGroup = make(map[string]map[string]*partialMessageStatePerTopicGroup)
}

func (e *PartialMessageExtension) groupState(topic string, groupID []byte) (*partialMessageStatePerTopicGroup, error) {
	statePerTopic, ok := e.statePerTopicPerGroup[topic]
	if !ok {
		statePerTopic = make(map[string]*partialMessageStatePerTopicGroup)
		e.statePerTopicPerGroup[topic] = statePerTopic
	}
	state, ok := statePerTopic[string(groupID)]
	if !ok {
		pm, err := e.NewPartialMessage(topic, groupID)
		if err != nil {
			return nil, err
		}
		state = newPartialMessageStatePerTopicGroup(max(e.GroupTTLByHeatbeat, minGroupTTL), pm)
		statePerTopic[string(groupID)] = state
	}
	return state, nil
}

func (e *PartialMessageExtension) PublishPartial(topic string, partial PartialMessage, opts PartialMessagePublishOptions) error {
	iwant, err := partial.MissingParts()
	if err != nil {
		return err
	}
	ihave, err := partial.AvailableParts()
	if err != nil {
		return err
	}

	statePerTopic := e.statePerTopicPerGroup[topic]
	if statePerTopic == nil {
		statePerTopic = make(map[string]*partialMessageStatePerTopicGroup)
		e.statePerTopicPerGroup[topic] = statePerTopic
	}
	groupID := partial.GroupID()
	state := statePerTopic[string(groupID)]
	if state == nil {
		state = newPartialMessageStatePerTopicGroup(e.GroupTTLByHeatbeat, partial)
		statePerTopic[string(groupID)] = state
	} else {
		state.partialMessage = partial
		state.groupTTL = max(e.GroupTTLByHeatbeat, minGroupTTL)
	}

	var eagerPush *pb.PartialMessage
	if len(opts.EagerPush) > 0 {
		eagerPush = &pb.PartialMessage{
			TopicID: &topic,
			GroupID: groupID,
			Data:    opts.EagerPush,
		}
	}

	var partialIHAVE *pb.PartialIHAVE
	if len(ihave) > 0 && !bytes.Equal(ihave, state.lastHave) {
		state.lastHave = ihave
		partialIHAVE = &pb.PartialIHAVE{
			TopicID:  &topic,
			GroupID:  groupID,
			Metadata: ihave,
		}
	}

	var partialIWANT *pb.PartialIWANT
	if len(iwant) > 0 {
		partialIWANT = &pb.PartialIWANT{
			TopicID:  &topic,
			GroupID:  groupID,
			Metadata: iwant,
		}
	}

	for p := range e.meshPeers() {
		// Try to fulfill any outstanding PartialIWants
		if peerState, ok := state.peerState[p]; ok && len(peerState.wants) > 0 {
			// This peer has previously asked for a certain part. We'll give
			// them what we can.
			pm, rest, err := partial.PartialMessageBytesFromMetadata(peerState.wants)
			if err != nil {
				log.Warn("partial message extension failed to get partial message bytes", "error", err)
				// Possibly a bad request, we'll delete the request as we will likely error next time we try to handle it
				state.clearPeerWants(p)
				continue
			}
			if len(pm) == 0 {
				// We don't have anything useful for them
				continue
			}
			if len(rest) == 0 {
				// We've sent all the parts they asked for
				state.clearPeerWants(p)
			} else {
				// Track what we haven't sent them yet
				peerState.wants = rest
			}
			// Send them the data
			e.sendRPC(p, &RPC{
				RPC: pb.RPC{
					Partial: &pb.PartialMessagesExtension{
						Message: &pb.PartialMessage{
							TopicID: &topic,
							GroupID: groupID,
							Data:    pm,
						},
						// Also send them what we want
						Iwant: partialIWANT,
					},
				},
			}, false)
			// We don't need to send them eager pushes or PartialIHAVE
			// as they already made an explicit request.
			continue
		}

		var rpc RPC
		var added bool

		if eagerPush != nil {
			added = true
			if rpc.Partial == nil {
				rpc.Partial = &pb.PartialMessagesExtension{}
			}
			rpc.Partial.Message = eagerPush
		}

		// Only send IHAVE if they don't have what we have and they haven't sent
		// us a a IDONTWANT
		if partialIHAVE != nil {
			if peerState, ok := state.peerState[p]; !ok || (!bytes.Equal(ihave, peerState.has) && !peerState.recvdDontWant) {
				added = true
				if rpc.Partial == nil {
					rpc.Partial = &pb.PartialMessagesExtension{}
				}
				rpc.Partial.Ihave = partialIHAVE
			}
		}

		// Only send an IWANT if it was different then before
		if partialIWANT != nil {
			if peerState, ok := state.peerState[p]; !ok || !bytes.Equal(iwant, peerState.sentWant) {
				added = true
				state.setSentWants(p, iwant)
				if rpc.Partial == nil {
					rpc.Partial = &pb.PartialMessagesExtension{}
				}
				rpc.Partial.Iwant = partialIWANT
			}
		}

		if added {
			e.sendRPC(p, &rpc, false)
		}
	}

	return nil
}

func (e *PartialMessageExtension) AddPeer(id peer.ID) {
	// Send this new peer PartialIWants
	// TODO: We might not need/want this.
	for topic := range e.topicsForPeer(id) {
		if statePerTopic := e.statePerTopicPerGroup[topic]; statePerTopic != nil {
			for _, state := range statePerTopic {
				iwant, err := state.partialMessage.MissingParts()
				if err != nil {
					log.Warn("partial message extension failed to get missing parts", "error", err)
					continue
				}
				if len(iwant) == 0 {
					continue
				}
				if peerState, ok := state.peerState[id]; !ok || !bytes.Equal(peerState.sentWant, iwant) {
					state.setSentWants(id, iwant)
					e.sendRPC(id, &RPC{
						RPC: pb.RPC{
							Partial: &pb.PartialMessagesExtension{
								Iwant: &pb.PartialIWANT{
									TopicID:  &topic,
									GroupID:  state.partialMessage.GroupID(),
									Metadata: iwant,
								},
							},
						},
					}, false)
				}
			}
		}
	}
}

func (e *PartialMessageExtension) RemovePeer(id peer.ID) {
	for _, statePerTopic := range e.statePerTopicPerGroup {
		for _, state := range statePerTopic {
			delete(state.peerState, id)
		}
	}
}

func (e *PartialMessageExtension) Heartbeat() {
	for topic, statePerTopic := range e.statePerTopicPerGroup {
		for group, s := range statePerTopic {
			if s.groupTTL == 0 {
				delete(statePerTopic, group)
				if len(statePerTopic) == 0 {
					delete(e.statePerTopicPerGroup, topic)
				}
			} else {
				s.groupTTL--
			}
		}
	}
}

func (e *PartialMessageExtension) HandleRPC(rpc *RPC) error {
	if rpc.Partial == nil {
		return nil
	}
	peerID := rpc.from

	if rpc.Partial.Message != nil {
		pbMsg := rpc.Partial.Message
		state, err := e.groupState(pbMsg.GetTopicID(), pbMsg.GroupID)
		if err != nil {
			return err
		}
		state.partialMessage.ExtendFromEncodedPartialMessage(pbMsg.Data)
	}

	if rpc.Partial.Iwant != nil {
		pbMsg := rpc.Partial.Iwant
		topic := pbMsg.GetTopicID()
		groupID := pbMsg.GroupID
		iwant := pbMsg.Metadata
		if err := e.ValidateRequestMetadata(topic, iwant); err != nil {
			return err
		}
		state, err := e.groupState(topic, groupID)
		if err != nil {
			return err
		}
		iwantResponse, iwant, err := state.partialMessage.PartialMessageBytesFromMetadata(pbMsg.Metadata)
		if err != nil {
			return err
		}

		if len(iwantResponse) > 0 {
			// We have something to offer
			e.sendRPC(peerID, &RPC{
				RPC: pb.RPC{
					Partial: &pb.PartialMessagesExtension{
						Message: &pb.PartialMessage{
							TopicID: &topic,
							GroupID: groupID,
							Data:    iwantResponse,
						},
					},
				},
			}, false)
		}

		state.setPeerWants(peerID, iwant)
	}

	if rpc.Partial.Ihave != nil {
		ihaveMsg := rpc.Partial.Ihave
		topic := ihaveMsg.GetTopicID()
		groupID := ihaveMsg.GroupID
		if err := e.ValidateRequestMetadata(topic, ihaveMsg.Metadata); err != nil {
			return err
		}
		state, err := e.groupState(topic, groupID)
		if err != nil {
			return err
		}
		peerState, ok := state.peerState[peerID]
		if !ok {
			peerState = &partialMessagePeerState{}
			state.peerState[peerID] = peerState
		}
		peerState.has = ihaveMsg.Metadata
		if state.partialMessage.ShouldRequest(peerState.has) {
			iwant, err := state.partialMessage.MissingParts()
			if err != nil {
				return err
			}

			if len(iwant) > 0 {
				if !bytes.Equal(peerState.sentWant, iwant) {
					state.setSentWants(peerID, iwant)
					e.sendRPC(peerID, &RPC{
						RPC: pb.RPC{
							Partial: &pb.PartialMessagesExtension{
								Iwant: &pb.PartialIWANT{
									TopicID:  &topic,
									GroupID:  groupID,
									Metadata: iwant,
								},
							},
						},
					}, false)
				}
			}
		}
	}

	if rpc.Partial.Idontwant != nil {
		topic := rpc.Partial.Idontwant.GetTopicID()
		groupID := rpc.Partial.Idontwant.GroupID
		state, err := e.groupState(topic, groupID)
		if err != nil {
			return err
		}
		peerState, ok := state.peerState[peerID]
		if !ok {
			peerState = &partialMessagePeerState{}
			state.peerState[peerID] = peerState
		}
		peerState.wants = nil
		peerState.recvdDontWant = true
	}

	return nil
}
