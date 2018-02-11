package nsqmate

import (
	"github.com/nsqio/go-nsq"
)

type Consumer struct {
	topic   string
	channel string

	config   *Config
	consumer *nsq.Consumer

	hf           HandlerFunc
	msgProcessor IMessageProcessor
	lookupdList  []string

	stopCh chan bool
}

func NewConsumer(topic, channel string, config *Config) (*Consumer, error) {
	nc, err := nsq.NewConsumer(topic, channel, config.Config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		topic:   topic,
		channel: channel,

		config:   config,
		consumer: nc,

		stopCh: make(chan bool),
	}, nil
}

func (c *Consumer) SetHandleFunc(hf HandlerFunc) *Consumer {
	c.hf = hf
	c.consumer.AddHandler(c)

	return c
}

func (c *Consumer) SetMsgProcessor(msgProcessor IMessageProcessor) *Consumer {
	c.msgProcessor = msgProcessor

	return c
}

func (c *Consumer) AddLookupd(addr string) *Consumer {
	c.lookupdList = append(c.lookupdList, addr)

	return c
}

func (c *Consumer) HandleMessage(message *nsq.Message) error {
	body, err := c.msgProcessor.Restore(message.Body)
	if err != nil {
		return err
	}

	message.Body = body
	return c.hf(&Message{message})
}

func (c *Consumer) Run() error {
	err := c.consumer.ConnectToNSQLookupds(c.lookupdList)
	if err != nil {
		return err
	}

	<-c.stopCh
	c.consumer.Stop()
	c.stopCh <- true

	return nil
}

func (c *Consumer) Stop() {
	c.stopCh <- true
	<-c.stopCh
}
