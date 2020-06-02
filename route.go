package krouter

import "encoding/json"

type Route struct {
	Params  map[string]string `json:"params"`
	Headers map[string]string `json:"headers"`
	Payload string            `json:"payload"`
	Name    string            `json:"name"`
}

func (r Route) Encode() ([]byte, error) {
	return json.Marshal(r)
}

func (r Route) Decode(data []byte) error {
	return json.Unmarshal(data, &r)
}

