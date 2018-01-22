package nsqmate

type IMessageProcessor interface {
	Process(msg []byte) []byte
	MultiProcess(msgs [][]byte) [][]byte

	Restore(msg []byte) ([]byte, error)
}

type IMessageEndecoder interface {
	Encode(msg []byte) ([]byte, error)
	Decode(msg []byte) ([]byte, error)
}
