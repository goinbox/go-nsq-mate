package nsqmate

import (
	"github.com/goinbox/golog"

	"bytes"
	"sync"
)

type IConsumerTask interface {
	Start()
	Stop()
}

type LineProcessFunc func(line []byte, workerId int)

type BaseConsumerTask struct {
	Name string

	lineWorkerNum int
	lineCh        chan []byte
	lpf           LineProcessFunc

	wg     *sync.WaitGroup
	stopCh chan bool

	logger golog.ILogger

	consumer *Consumer
}

func NewBaseConsumerTask(name string) *BaseConsumerTask {
	return &BaseConsumerTask{
		Name: name,

		logger: new(golog.NoopLogger),
	}
}

func (b *BaseConsumerTask) SetLogger(logger golog.ILogger) *BaseConsumerTask {
	b.logger = logger

	return b
}

func (b *BaseConsumerTask) SetConsumer(consumer *Consumer) *BaseConsumerTask {
	b.consumer = consumer
	b.consumer.SetHandleFunc(b.consumerHandleFunc)

	return b
}

func (b *BaseConsumerTask) SetLineWorker(lineWorkerNum int, lpf LineProcessFunc) *BaseConsumerTask {
	b.lineWorkerNum = lineWorkerNum
	b.lineCh = make(chan []byte, lineWorkerNum)
	b.lpf = lpf

	b.wg = new(sync.WaitGroup)
	b.stopCh = make(chan bool, lineWorkerNum)

	return b
}

func (b *BaseConsumerTask) DebugLog(msg string) {
	b.logger.Debug([]byte(msg))
}

func (b *BaseConsumerTask) InfoLog(msg string) {
	b.logger.Info([]byte(msg))
}

func (b *BaseConsumerTask) NoticeLog(msg string) {
	b.logger.Notice([]byte(msg))
}

func (b *BaseConsumerTask) ErrorLog(msg string) {
	b.logger.Error([]byte(msg))
}

func (b *BaseConsumerTask) WarningLog(msg string) {
	b.logger.Warning([]byte(msg))
}

func (b *BaseConsumerTask) Start() {
	b.startLineWorker()
	b.consumer.Run()
}

func (b *BaseConsumerTask) Stop() {
	b.stopLineWorker()
}

func (b *BaseConsumerTask) consumerHandleFunc(message *Message) error {
	for _, line := range bytes.Split(message.Body, []byte{'\n'}) {
		if len(line) != 0 {
			b.lineCh <- line
		}
	}

	return nil
}

func (b *BaseConsumerTask) startLineWorker() {
	for i := 0; i < b.lineWorkerNum; i++ {
		go b.lineWorker(i + 1)
		b.wg.Add(1)
	}
}

func (b *BaseConsumerTask) stopLineWorker() {
	b.consumer.Stop()
	for i := 0; i < b.lineWorkerNum; i++ {
		b.stopCh <- true
	}

	b.wg.Wait()
	for len(b.lineCh) != 0 {
		line := <-b.lineCh
		b.lpf(line, 0)
	}
}

func (b *BaseConsumerTask) lineWorker(workerId int) {
	defer b.wg.Done()

	for {
		select {
		case line := <-b.lineCh:
			b.lpf(line, workerId)
		case <-b.stopCh:
			return
		}
	}
}
