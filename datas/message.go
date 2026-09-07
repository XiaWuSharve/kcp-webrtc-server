package datas

import (
	"github.com/bwmarrin/snowflake"
	"google.golang.org/protobuf/proto"
)

type MMessage struct {
	Message
	bytes []byte
}

// GetRequiredBufLen implements [Encodable].
func (p *MMessage) GetRequiredBufLen() int {
	panic("unimplemented")
}

var _ Encodable = (*MMessage)(nil)

func (p *MMessage) ToByte() []byte {
	p.bytes, _ = proto.Marshal(p)
	return p.bytes
}

type MessageDecoder struct {
	message MMessage
}

var _ Decoder[*MMessage] = (*MessageDecoder)(nil)

func (mp *MessageDecoder) Parse(data []byte) (*MMessage, error) {
	if err := proto.Unmarshal(data, &mp.message); err != nil {
		return nil, err
	}
	return &mp.message, nil
}

// type Message2SendFrame struct {
// 	frame SendFrame
// 	bytes []byte
// 	Err   error
// }

// var _ Converter[*Message, *SendFrame] = (*Message2SendFrame)(nil)

// func (m2f *Message2SendFrame) Convert(mess *Message) (*SendFrame, error) {
// 	m2f.frame.AckStatus = mess.GetAck().Status
// 	m2f.frame.ConnId = mess.ConnId
// 	m2f.bytes, m2f.Err = proto.Marshal(mess)
// 	if m2f.Err != nil {
// 		return nil, m2f.Err
// 	}
// 	m2f.frame.Payload = m2f.bytes
// 	return &m2f.frame, nil
// }

func GenId() int64 {
	return Ids.Generate().Int64()
}

var Ids *snowflake.Node
