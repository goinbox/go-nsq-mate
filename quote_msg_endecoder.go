package nsqmate

import (
	"strconv"
)

type QuoteMessageEndecoder struct {
}

func (this *QuoteMessageEndecoder) Encode(msg []byte) ([]byte, error) {
	msgs := []byte(strconv.Quote(string(msg)))
	return msgs, nil
}

func (this *QuoteMessageEndecoder) Decode(msg []byte) ([]byte, error) {
	msgs, err := strconv.Unquote(string(msg))
	if err != nil {
		return nil, err
	}

	return []byte(msgs), nil
}
