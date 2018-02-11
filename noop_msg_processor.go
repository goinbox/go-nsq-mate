package nsqmate

type NoopMsgProcessor struct {
}

func (n *NoopMsgProcessor) Process(msg []byte) []byte {
	return msg
}

func (n *NoopMsgProcessor) MultiProcess(msgs [][]byte) [][]byte {
	return msgs
}

func (n *NoopMsgProcessor) Restore(msg []byte) ([]byte, error) {
	return msg, nil
}
