package datas

type Send struct {
	ReceiverId string
	AckStatus  AckStatus
	MessageId  int64
	ConnId     int64
	Payload    []byte
}

// GetRequiredBufLen implements [Encodable].
func (s *Send) GetRequiredBufLen() int {
	panic("unimplemented")
}

// ToByte implements [Encodable].
func (s *Send) ToByte() []byte {
	panic("unimplemented")
}

var _ Encodable = (*Send)(nil)
