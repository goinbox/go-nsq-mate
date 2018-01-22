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
	c, err := nsq.NewConsumer(topic, channel, config.Config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		topic:   topic,
		channel: channel,

		config:   config,
		consumer: c,

		stopCh: make(chan bool),
	}, nil
}

func (this *Consumer) SetHandleFunc(hf HandlerFunc) *Consumer {
	this.hf = hf
	this.consumer.AddHandler(this)

	return this
}

func (this *Consumer) SetMsgProcessor(msgProcessor IMessageProcessor) *Consumer {
	this.msgProcessor = msgProcessor

	return this
}

func (this *Consumer) AddLookupd(addr string) *Consumer {
	this.lookupdList = append(this.lookupdList, addr)

	return this
}

func (this *Consumer) HandleMessage(message *nsq.Message) error {
	body, err := this.msgProcessor.Restore(message.Body)
	if err != nil {
		return err
	}

	message.Body = body
	return this.hf(&Message{message})
}

func (this *Consumer) Run() error {
	err := this.consumer.ConnectToNSQLookupds(this.lookupdList)
	if err != nil {
		return err
	}

	<-this.stopCh
	this.consumer.Stop()
	this.stopCh <- true

	return nil
}

func (this *Consumer) Stop() {
	this.stopCh <- true
	<-this.stopCh
}
