package datas

type Store struct {
	Type        MessageType
	Sequence    int64
	ReceiverId  string
	AckSequence int64
	PullCount   int32
	Payload     []byte
}

// GetRequiredBufLen implements [Encodable].
func (s *Store) GetRequiredBufLen() int {
	panic("unimplemented")
}

// ToByte implements [Encodable].
func (s *Store) ToByte() []byte {
	panic("unimplemented")
}

var _ Encodable = (*Store)(nil)
