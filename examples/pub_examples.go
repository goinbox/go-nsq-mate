package main

import (
	"github.com/nsqio/go-nsq"

	"bytes"
	"compress/gzip"
	"io/ioutil"
)

func main() {
	config := nsq.NewConfig()
	producer, _ := nsq.NewProducer("127.0.0.1:4150", config)

	for i := 0; i < 10; i++ {
		line := "abc\nbcd\ncde\n"
		cline := compress([]byte(line))
		producer.Publish("go-nsq-mate-dev", cline)
		//lb, _ := decompress(cline)
		//fmt.Println("unzip",string(lb))
	}
}

func compress(msg []byte) []byte {
	b := bytes.NewBuffer([]byte{})
	w := gzip.NewWriter(b)
	w.Write(msg)
	w.Flush()
	w.Close()

	return b.Bytes()
}

func decompress(msg []byte) ([]byte, error) {
	var err error

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
