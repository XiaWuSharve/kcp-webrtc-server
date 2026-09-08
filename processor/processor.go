package processor

import (
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type Processor[ConnType any] struct {
	consumer *mq.ReceiveConsumer
	sender   *mq.Producer
	// registry  *registry.UserRepo
	converter datas.Converter[*datas.Receive, *datas.Message]
}

var _ mq.Handler[*datas.Receive] = (*Processor[struct{}])(nil)

func New[ConnType any](consumer *mq.ReceiveConsumer, producer *mq.Producer) *Processor[ConnType] {
	return &Processor[ConnType]{consumer, producer, &datas.ReceiveFrame2Message{}}
}

// TODO 抽出发送入队逻辑
func (p *Processor[C]) Handle(frame *datas.Receive) error {
	slog.Debug("received raw message", "payload", frame.Payload)
	m, err := p.converter.Convert(frame)
	slog.Debug("message", "mess", m.String())
	if err != nil {
		// FAIL message
		m.Type = datas.MessageType_ACK
		m.Data = &datas.Message_Ack{Ack: &datas.Ack{
			MessageId: m.MessageId,
			Status:    datas.AckStatus_FAIL,
			Reason:    fmt.Sprintf("请求格式错误: %s", err),
		}}
		m.SenderId = m.ReceiverId
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
		return nil
	}
	switch m.GetData().(type) {
	case *datas.Message_Candidate:
		// TODO check candiMess info
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	case *datas.Message_Chat:
		chatMess := m.GetChat()
		for _, v := range chatMess.MessageChain {
			if v.Type != datas.MessageUnitType_TEXT &&
				v.Type != datas.MessageUnitType_CALL &&
				v.Type != datas.MessageUnitType_ANSWER &&
				v.Type != datas.MessageUnitType_ESTABLISH {
				return fmt.Errorf("ilegal message unit type: %s", v.Type.String())
			}
		}
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	case *datas.Message_Call:
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	case *datas.Message_Answer:
		_, err := p.sender.Enqueue(m)
		if err != nil {
			return fmt.Errorf("cannot enqueue sender: %w", err)
		}
	}
	m.Data = &datas.Message_Ack{Ack: &datas.Ack{
		MessageId: m.MessageId,
		Status:    datas.AckStatus_SENT,
	}}
	m.SenderId = m.ReceiverId
	_, err = p.sender.Enqueue(m)
	if err != nil {
		return fmt.Errorf("cannot enqueue sender: %w", err)
	}
	return nil
}

// func ToWriteChan() {
// 	rc.WriteChan <- s.TransformResponse(createdTime, m)
// 	slog.Debug("sent chat message", "id", c.Id, "raw json", m)
// }
