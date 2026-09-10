package datas

type Send struct {
	Type       MessageType
	ReceiverId string
	AckStatus  AckStatus
	MessageId  int64
	ConnId     int64
	Payload    []byte
}

// GetHeaderLen implements [Encodable].
func (s *Send) GetHeaderLen() int {
	panic("unimplemented")
}

// ToByte implements [Encodable].
func (s *Send) ToByte() []byte {
	panic("unimplemented")
}

var _ Encodable = (*Send)(nil)
