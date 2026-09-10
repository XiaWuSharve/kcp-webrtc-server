package datas

import (
	"github.com/bwmarrin/snowflake"
	"google.golang.org/protobuf/proto"
)

type MMessage struct {
	Message
	Payload []byte
}

// GetHeaderLen implements [Encodable].
func (p *MMessage) GetHeaderLen() int {
	return 0
}

var _ Encodable = (*MMessage)(nil)

func (p *MMessage) ToByte() []byte {
	p.Payload, _ = proto.Marshal(p)
	return p.Payload
}

type MessageDecoder struct {
	message MMessage
}

// TODO 改为 Decodable，Parse(ToByte?)时指定底层数组偏移量
var _ Decoder[*MMessage] = (*MessageDecoder)(nil)

func (mp *MessageDecoder) Parse(data []byte) (*MMessage, error) {
	if err := proto.Unmarshal(data, &mp.message); err != nil {
		return nil, err
	}
	mp.message.Payload = data
	return &mp.message, nil
}

type MMessage2Send struct {
	frame Send
}

var _ Converter[*MMessage, *Send] = (*MMessage2Send)(nil)

func (m2f *MMessage2Send) Convert(mess *MMessage) (*Send, error) {
	m2f.frame.Type = mess.Type
	m2f.frame.ReceiverId = mess.ReceiverId
	m2f.frame.MessageId = mess.MessageId
	m2f.frame.ConnId = mess.ConnId
	if mess.Type == MessageType_ACK {
		m2f.frame.AckStatus = mess.GetAckStatus()
	}
	m2f.frame.Payload = mess.ToByte()
	return &m2f.frame, nil
}

func GenId() int64 {
	return Ids.Generate().Int64()
}

var Ids *snowflake.Node
