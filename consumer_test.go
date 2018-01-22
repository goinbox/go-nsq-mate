package nsqmate

import (
	"github.com/goinbox/golog"

	"fmt"
	"testing"
	"time"
)

func newTestConsumer() *Consumer {
	config := NewConfig()
	config.MaxInFlight = 100
	config.LookupdPollInterval = time.Second * 3

	consumer, _ := NewConsumer("go-nsq-mate-dev", "c1", config)
	consumer.AddLookupd("127.0.0.1:4161").
		SetMsgProcessor(NewGzipMessageProcessor(nil))

	return consumer
}

func TestConsumer(t *testing.T) {
	consumer := newTestConsumer()
	consumer.SetHandleFunc(func(message *Message) error {
		fmt.Println("Recieve", string(message.Body))
		return nil
	})

	consumer.Run()
}

type ConsumerTaskDemo struct {
	*BaseConsumerTask
}

func TestConsumerTask(t *testing.T) {
	fw, _ := golog.NewFileWriter("/tmp/demo_consumer_task.log")
	logger, _ := golog.NewSimpleLogger(fw, golog.LEVEL_DEBUG, golog.NewSimpleFormater())
	ct := &ConsumerTaskDemo{NewBaseConsumerTask("DemoConsumerTask")}
	ct.SetLogger(logger).
		SetConsumer(newTestConsumer()).
		SetLineWorker(10, demoLineProcessFunc)

	ct.Start()
}

func demoLineProcessFunc(line []byte, workerId int) {
	fmt.Println("WorkerId:", workerId, "line:", string(line))
}
