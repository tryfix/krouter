package krouter

type Encoder interface {
	Encode(v interface{}) ([]byte, error)
	Decode([]byte) (interface{}, error)
}
