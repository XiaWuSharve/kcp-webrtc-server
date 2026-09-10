package datas

type Encodable interface {
	ToByte() []byte
	GetHeaderLen() int
}

type Converter[S, D any] interface {
	Convert(source S) (D, error)
}

type Decoder[MessType any] interface {
	Parse(data []byte) (MessType, error)
}
