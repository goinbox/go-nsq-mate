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

func (this *BaseConsumerTask) SetLogger(logger golog.ILogger) *BaseConsumerTask {
	this.logger = logger

	return this
}

func (this *BaseConsumerTask) SetConsumer(consumer *Consumer) *BaseConsumerTask {
	this.consumer = consumer
	this.consumer.SetHandleFunc(this.consumerHandleFunc)

	return this
}

func (this *BaseConsumerTask) SetLineWorker(lineWorkerNum int, lpf LineProcessFunc) *BaseConsumerTask {
	this.lineWorkerNum = lineWorkerNum
	this.lineCh = make(chan []byte, lineWorkerNum)
	this.lpf = lpf

	this.wg = new(sync.WaitGroup)
	this.stopCh = make(chan bool, lineWorkerNum)

	return this
}

func (this *BaseConsumerTask) DebugLog(msg string) {
	this.logger.Debug([]byte(msg))
}

func (this *BaseConsumerTask) InfoLog(msg string) {
	this.logger.Info([]byte(msg))
}

func (this *BaseConsumerTask) NoticeLog(msg string) {
	this.logger.Notice([]byte(msg))
}

func (this *BaseConsumerTask) ErrorLog(msg string) {
	this.logger.Error([]byte(msg))
}

func (this *BaseConsumerTask) WarningLog(msg string) {
	this.logger.Warning([]byte(msg))
}

func (this *BaseConsumerTask) Start() {
	this.startLineWorker()
	this.consumer.Run()
}

func (this *BaseConsumerTask) Stop() {
	this.stopLineWorker()
}

func (this *BaseConsumerTask) consumerHandleFunc(message *Message) error {
	for _, line := range bytes.Split(message.Body, []byte{'\n'}) {
		if len(line) != 0 {
			this.lineCh <- line
		}
	}

	return nil
}

func (this *BaseConsumerTask) startLineWorker() {
	for i := 0; i < this.lineWorkerNum; i++ {
		go this.lineWorker(i + 1)
		this.wg.Add(1)
	}
}

func (this *BaseConsumerTask) stopLineWorker() {
	this.consumer.Stop()
	for i := 0; i < this.lineWorkerNum; i++ {
		this.stopCh <- true
	}

	this.wg.Wait()
	for len(this.lineCh) != 0 {
		line := <-this.lineCh
		this.lpf(line, 0)
	}
}

func (this *BaseConsumerTask) lineWorker(workerId int) {
	defer this.wg.Done()

	for {
		select {
		case line := <-this.lineCh:
			this.lpf(line, workerId)
		case <-this.stopCh:
			return
		}
	}
}
