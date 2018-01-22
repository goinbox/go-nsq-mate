package nsqmate

type NoopMsgProcessor struct {
}

func (this *NoopMsgProcessor) Process(msg []byte) []byte {
	return msg
}

func (this *NoopMsgProcessor) MultiProcess(msgs [][]byte) [][]byte {
	return msgs
}

func (this *NoopMsgProcessor) Restore(msg []byte) ([]byte, error) {
	return msg, nil
}
