package nsqmate

import "github.com/nsqio/go-nsq"

type Message struct {
	*nsq.Message
}

type Config struct {
	*nsq.Config
}

func NewConfig() *Config {
	return &Config{nsq.NewConfig()}
}

type HandlerFunc func(message *Message) error
