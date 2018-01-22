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

func (this *gzipMessageProcessor) Process(msg []byte) []byte {
	msg = this.processor.Process(msg)

	b := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(b)
	w.Write(msg)
	w.Flush()
	w.Close()

	return b.Bytes()
}

func (this *gzipMessageProcessor) MultiProcess(msgs [][]byte) [][]byte {
	msgs = this.processor.MultiProcess(msgs)

	bs := make([][]byte, len(msgs))
	for i, msg := range msgs {
		bs[i] = this.Process(msg)
	}

	return bs
}

func (this *gzipMessageProcessor) Restore(msg []byte) ([]byte, error) {
	var err error
	msg, err = this.processor.Restore(msg)
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
