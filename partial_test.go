package pubsub

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type largeMockMessage struct {
	Group  uint32
	Chunks [][]byte

	// Cache
	groupID []byte
}

func (m *largeMockMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *largeMockMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *largeMockMessage) GroupID() []byte {
	if m.groupID != nil {
		return m.groupID
	}
	groupID := make([]byte, 4)
	binary.BigEndian.PutUint32(groupID, uint32(m.Group))
	m.groupID = groupID
	return groupID
}

func (m *largeMockMessage) ID(idFn func(data []byte) string) (string, error) {
	b, err := m.Marshal()
	if err != nil {
		return "", err
	}
	return idFn(b), nil
}

type mockPartialMessage struct {
	GroupIDString string
	topic         string
	Chunks        [][]byte
}

type mockMetadata struct {
	ToInclude []int
}

func (md *mockMetadata) Marshal() ([]byte, error) {
	return json.Marshal(md)
}

func (md *mockMetadata) Unmarshal(data []byte) error {
	return json.Unmarshal(data, md)
}

func (p *mockPartialMessage) GroupID() PartialMessageGroupID {
	return []byte(p.GroupIDString)
}

// AsComplete implements PartialMessage.
func (p *mockPartialMessage) AsComplete() (*Message, error) {
	msg := largeMockMessage{
		Group:  binary.BigEndian.Uint32(p.GroupID()),
		Chunks: p.Chunks,
	}
	b, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	return &Message{
		Message: &pb.Message{
			Data:  b,
			Topic: &p.topic,
		},
	}, nil
}

// IsComplete implements PartialMessage.
func (p *mockPartialMessage) IsComplete() bool {
	for _, c := range p.Chunks {
		if c == nil {
			return false
		}
	}
	return true
}

// Key implements PartialMessage.
func (p *mockPartialMessage) Key() string {
	return fmt.Sprintf("%s|%s", p.topic, p.GroupIDString)
}

// Marshal implements PartialMessage.
func (p *mockPartialMessage) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

func (p *mockPartialMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, p)
}

// Merge implements PartialMessage.
func (p *mockPartialMessage) Merge(otherMsg PartialMessage) error {
	other, ok := otherMsg.(*mockPartialMessage)
	if !ok {
		return errors.New("invalid message type")
	}
	for i := range p.Chunks {
		if p.Chunks[i] == nil {
			p.Chunks[i] = other.Chunks[i]
		}
	}
	return nil
}

// Split implements PartialMessage.
func (p *mockPartialMessage) Split(applicationDefinedFilter []byte) iter.Seq[PartialMessage] {
	toSend := &mockPartialMessage{
		GroupIDString: p.GroupIDString,
		topic:         p.topic,
		Chunks:        make([][]byte, len(p.Chunks)),
	}
	var mockMetadata mockMetadata
	if err := mockMetadata.Unmarshal(applicationDefinedFilter); err != nil {
		return nil
	}
	for _, i := range mockMetadata.ToInclude {
		toSend.Chunks[i] = p.Chunks[i]
	}

	return func(yield func(PartialMessage) bool) {
		yield(toSend)
	}
}

var _ PartialMessage = (*mockPartialMessage)(nil)

func TestPartialIWANT(t *testing.T) {
	ctx, timeoutCncl := context.WithCancel(context.Background())
	defer timeoutCncl()

	msgIDFromData := func(data []byte) string {
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:])
	}

	msgID := func(pmsg *pb.Message) string {
		return msgIDFromData(pmsg.Data)
	}

	msg1 := largeMockMessage{
		Group:  1,
		Chunks: [][]byte{[]byte("chunk1"), []byte("chunk2")},
	}
	msg2 := largeMockMessage{
		Group:  2,
		Chunks: [][]byte{[]byte("chunk3"), []byte("chunk4")},
	}

	msgs := []largeMockMessage{msg1, msg2}
	msgMapByGroup := make(map[string]largeMockMessage)
	msgMapByID := make(map[string]largeMockMessage)
	for _, msg := range msgs {
		id, err := msg.ID(msgIDFromData)
		if err != nil {
			t.Fatal(err)
		}
		msgMapByGroup[string(msg.GroupID())] = msg
		msgMapByID[id] = msg
	}

	topicString := "foobar"

	partialMessageSettings := PartialMessageExtensionSettings{
		SplitMessage: func(message *Message) (PartialMessage, error) {
			var msg largeMockMessage
			if err := msg.Unmarshal(message.Data); err != nil {
				return nil, err
			}
			groupID := msg.GroupID()
			return &mockPartialMessage{
				GroupIDString: string(groupID),
				topic:         *message.Topic,
				Chunks:        msg.Chunks,
			}, nil
		},

		UnmarshalPartialMessage: func(topic []byte, data []byte) (PartialMessage, error) {
			var partialMsg mockPartialMessage
			err := partialMsg.Unmarshal(data)
			if err != nil {
				return nil, err
			}
			partialMsg.topic = string(topic)
			return &partialMsg, nil
		},
	}

	hosts := getDefaultHosts(t, 3)
	psubs := make([]*PubSub, 3)
	psubs[0] = getGossipsub(ctx, hosts[0],
		WithPartialMessagesExtension(partialMessageSettings),
		WithMessageSignaturePolicy(StrictNoSign),
		WithNoAuthor(),
		WithMessageIdFn(msgID))
	psubs[1] = getGossipsub(ctx, hosts[1],
		WithPartialMessagesExtension(partialMessageSettings),
		WithMessageSignaturePolicy(StrictNoSign),
		WithNoAuthor(),
		WithMessageIdFn(msgID))
	psubs[2] = getGossipsub(ctx, hosts[2],
		WithMessageSignaturePolicy(StrictNoSign),
		WithNoAuthor(),
		WithMessageIdFn(msgID))

	var topics []*Topic
	for _, ps := range psubs {
		topic, err := ps.Join(topicString)
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, topic)

		_, err = ps.Subscribe(topicString)
		if err != nil {
			t.Fatal(err)
		}
	}

	sub0, err := topics[0].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub0.Cancel()

	connect(t, hosts[2], hosts[1])
	connect(t, hosts[1], hosts[0])
	time.Sleep(1 * time.Second)

	md := mockMetadata{
		ToInclude: []int{1},
	}
	mdBytes, err := md.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	groupID := msg1.GroupID()

	partialChunks := make([][]byte, len(msg1.Chunks))
	// We already have the first chunk
	partialChunks[0] = msg1.Chunks[0]
	err = TrackPartialMessage(psubs[0], topicString, &mockPartialMessage{
		GroupIDString: string(groupID),
		topic:         topicString,
		Chunks:        partialChunks,
	})

	err = SendPartialIWANT(psubs[0], topicString, groupID, mdBytes)
	if err != nil {
		t.Fatal(err)
	}

	msg1Bytes, err := msg1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	topics[2].Publish(ctx, msg1Bytes)

	time.Sleep(time.Second)

	ctxTimeout, timeoutCncl := context.WithTimeout(ctx, 2*time.Second)
	defer timeoutCncl()
	msg, err := sub0.Next(ctxTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Got full message", msg)

}
