package pubsub

import (
	"errors"
	"fmt"
	"iter"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PartialMessageGroupID []byte

// Application defined
type PartialMessageExtensionSettings struct {
	Validator  PartialMessageValidator
	GetMessage func(groupID PartialMessageGroupID) (*Message, error)

	SplitMessage            func(message *Message) (PartialMessage, error)
	UnmarshalPartialMessage func(topic, data []byte) (PartialMessage, error)
}

type PartialMessage interface {
	Key() string
	Merge(PartialMessage) error
	Split(metadata []byte) iter.Seq[PartialMessage]
	GroupID() PartialMessageGroupID
	Marshal() ([]byte, error)
	IsComplete() bool
	AsComplete() (*Message, error)
}

type PartialMessageValidator interface {
	ValidatePartialMessage(PartialMessage) error
}

type pendingIWant struct {
	peer     peer.ID
	topic    []byte
	groupID  PartialMessageGroupID
	metadata []byte

	key string
}

func (p *pendingIWant) Key() string {
	if p.key == "" {
		key := append(p.topic, '|')
		key = append(key, p.groupID...)
		p.key = string(key)
	}
	return p.key
}

type partialMessageExtension struct {
	settings        PartialMessageExtensionSettings
	partialMessages map[string]PartialMessage
	// Key is topic+|+groupID
	pendingIWants map[string][]pendingIWant

	// TODO obviously needs to be GC'd and bounded.
	groupIDToMessageID map[string]string

	// Provided by gossipsub
	rpcSender             func(to peer.ID, rpc *RPC) error
	handleCompleteMessage func(message *Message) error
	handlePeerIDONTWANT   func(peer peer.ID, messageID string)
	alreadySentMessage    func(peer peer.ID, messageID string) bool
	getMessage            func(msgID string) *Message
}

func (p *partialMessageExtension) handleRPC(r *RPC) error {
	partial := r.GetPartial()
	if partial == nil {
		return nil
	}

	var errGroup error
	if partial.Iwant != nil {
		err := p.handlePartialIWANT(r.from, partial.Iwant)
		errGroup = errors.Join(errGroup, err)
	}

	if partial.Message != nil {
		err := p.handlePartialMessagePB(partial.Message)
		errGroup = errors.Join(errGroup, err)
	}
	return errGroup
}

func (p *partialMessageExtension) HandleValidatedMessage(msg *Message) error {
	partialMsg, err := p.settings.SplitMessage(msg)
	if err != nil {
		return err
	}

	if p.groupIDToMessageID == nil {
		p.groupIDToMessageID = make(map[string]string)
	}
	p.groupIDToMessageID[string(partialMsg.GroupID())] = msg.ID

	key := partialMsg.Key()
	if p.pendingIWants == nil {
		return nil
	}
	pendingIWants := p.pendingIWants[key]

	for _, pending := range pendingIWants {
		p.handlePeerIDONTWANT(pending.peer, msg.ID)
		p.sendPartialMessage(pending.peer, pending.topic, pending.groupID, partialMsg, pending.metadata)
	}
	p.pendingIWants[key] = nil
	return nil
}

func (p *partialMessageExtension) handlePartialIWANT(from peer.ID, partialIWANT *pubsub_pb.PartialIWANT) error {
	msgID, ok := p.groupIDToMessageID[string(partialIWANT.GroupID)]
	if !ok {
		// We don't have complete message to make partial messages from.
		// Register this as pending

		pending := pendingIWant{
			peer:     from,
			topic:    partialIWANT.TopicID,
			groupID:  partialIWANT.GroupID,
			metadata: partialIWANT.Metadata,
		}
		if p.pendingIWants == nil {
			p.pendingIWants = make(map[string][]pendingIWant)
		}

		pendingKey := pending.Key()
		p.pendingIWants[pendingKey] = append(p.pendingIWants[pendingKey], pending)

		return nil
	}

	if p.alreadySentMessage(from, msgID) {
		return nil
	}

	msg := p.getMessage(msgID)
	partialMessage, err := p.settings.SplitMessage(msg)
	if err != nil {
		return err
	}

	// We have the partial message. We can handle this RPC
	return p.sendPartialMessage(from, partialIWANT.TopicID, partialIWANT.GroupID, partialMessage, partialIWANT.Metadata)
}

func (p *partialMessageExtension) sendPartialMessage(to peer.ID, topicID, groupID []byte, msg PartialMessage, metadata []byte) error {
	for part := range msg.Split(metadata) {
		b, err := part.Marshal()
		if err != nil {
			return err
		}
		err = p.rpcSender(
			to,
			&RPC{
				RPC: pubsub_pb.RPC{
					Partial: &pubsub_pb.PartialMessagesExtension{
						Message: &pubsub_pb.PartialMessage{
							TopicID: topicID,
							Data:    b,
						},
					},
				},
			},
		)
		if err != nil {
			return err
		}
	}
	return nil

}

func (p *partialMessageExtension) handlePartialMessagePB(partialMessagePB *pubsub_pb.PartialMessage) error {
	partial, err := p.settings.UnmarshalPartialMessage(partialMessagePB.TopicID, partialMessagePB.Data)
	if err != nil {
		return err
	}
	return p.handlePartialMessage(string(partialMessagePB.TopicID), partial)
}

func (p *partialMessageExtension) handlePartialMessage(topic string, partialMessage PartialMessage) error {
	if p.partialMessages == nil {
		p.partialMessages = make(map[string]PartialMessage)
	}

	if p.settings.Validator != nil {
		err := p.settings.Validator.ValidatePartialMessage(partialMessage)
		if err != nil {
			return err
		}
	}

	k := partialMessage.Key()
	pmsg, ok := p.partialMessages[k]
	if ok {
		pmsg.Merge(partialMessage)
	} else {
		p.partialMessages[k] = partialMessage
		pmsg = partialMessage
	}

	if pmsg.IsComplete() {
		delete(p.partialMessages, k)
		if len(p.partialMessages) == 0 {
			p.partialMessages = nil
		}

		complete, err := pmsg.AsComplete()
		if err != nil {
			return err
		}
		err = p.handleCompleteMessage(complete)
		if err != nil {
			return err
		}
	}

	return nil
}

func TrackPartialMessage(pubsub *PubSub, topic string, partialMsg PartialMessage) error {
	rt, ok := pubsub.rt.(*GossipSubRouter)
	if !ok {
		return fmt.Errorf("not a gossipsub router")
	}
	pubsub.eval <- func() {
		if rt.partialMessages != nil {
			rt.partialMessages.handlePartialMessage(topic, partialMsg)
		}
	}

	return nil
}

func SendPartialIWANT(pubsub *PubSub, topic string, groupID PartialMessageGroupID, metadata []byte) error {
	rt, ok := pubsub.rt.(*GossipSubRouter)
	if !ok {
		return fmt.Errorf("not a gossipsub router")
	}
	pubsub.eval <- func() {
		for peer := range rt.mesh[topic] {
			rt.sendRPC(peer, &RPC{
				RPC: pubsub_pb.RPC{
					Partial: &pubsub_pb.PartialMessagesExtension{
						Iwant: &pubsub_pb.PartialIWANT{
							TopicID:  []byte(topic),
							GroupID:  groupID,
							Metadata: metadata,
						},
					},
				},
			}, true)
		}
	}

	return nil
}

func WithPartialMessagesExtension(settings PartialMessageExtensionSettings) Option {
	return func(p *PubSub) error {
		rt, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("not a gossipsub router")
		}

		rt.partialMessages = &partialMessageExtension{
			settings: settings,
			rpcSender: func(to peer.ID, rpc *RPC) error {
				rt.sendRPC(to, rpc, false)
				return nil
			},
			handleCompleteMessage: func(message *Message) error {
				if message.Topic == nil {
					return errors.New("invalid message; missing topic")
				}

				// Has to be in a goroutine to avoid a deadlock
				go func() {
					err := p.Publish(*message.Topic, message.Data)
					if err != nil {
						log.Warn("Error publishing complete message from partial messages:", err)
					}
				}()
				return nil
			},
			handlePeerIDONTWANT: func(peer peer.ID, messageID string) {
				rt.unwanted[peer][computeChecksum(messageID)] = rt.params.IDontWantMessageTTL
			},
			getMessage: func(msgID string) *Message {
				msg, ok := rt.mcache.Get(msgID)
				if !ok {
					return nil
				}
				return msg
			},
			alreadySentMessage: func(peer peer.ID, messageID string) bool {
				msg, ok := rt.mcache.Get(messageID)
				if !ok {
					return false
				}
				if msg.Topic == nil {
					return false
				}

				// If we have this message and the peer is in our mesh, we have
				// already sent the message.
				_, ok = rt.mesh[*msg.Topic][peer]
				return ok
			},
		}
		return nil
	}
}
