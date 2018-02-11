package nsqmate

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

type gzipMessageProcessor struct {
	processor IMessageProcessor
}

func NewGzipMessageProcessor(processor IMessageProcessor) *gzipMessageProcessor {
	if processor == nil {
		processor = new(NoopMsgProcessor)
	}

	return &gzipMessageProcessor{processor}
}

func (g *gzipMessageProcessor) Process(msg []byte) []byte {
	msg = g.processor.Process(msg)

	b := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(b)
	w.Write(msg)
	w.Flush()
	w.Close()

	return b.Bytes()
}

func (g *gzipMessageProcessor) MultiProcess(msgs [][]byte) [][]byte {
	msgs = g.processor.MultiProcess(msgs)

	bs := make([][]byte, len(msgs))
	for i, msg := range msgs {
		bs[i] = g.Process(msg)
	}

	return bs
}

func (g *gzipMessageProcessor) Restore(msg []byte) ([]byte, error) {
	var err error
	msg, err = g.processor.Restore(msg)
	if err != nil {
		return nil, err
	}

	b := bytes.NewReader(msg)
	r, err := gzip.NewReader(b)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(r)
	if err != nil && len(body) == 0 {
		return nil, err
	}

	return body, nil
}
