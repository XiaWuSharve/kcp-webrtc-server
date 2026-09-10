package conn

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/utils"
)

type Pool = utils.ShardMap[string, *SendHandler]

type Client struct {
	Conn
	StoreProducer  *mq.Producer
	sendHandler    *SendHandler
	receiveHandler *ReceiveHandler
	Pool           *Pool
}

type SendHandler struct {
	Client
	Err          error
	SendChan     chan *datas.Send
	sendData     *datas.Send
	storeData    *datas.Store
	SendConsumer mq.Consumer[*datas.Send]
	routed2store datas.Converter[*datas.Send, *datas.Store]
	id           *string
}

// Handle implements [mq.Handler].
func (s *SendHandler) Handle(message *datas.Send) error {
	s.SendChan <- message
	return nil
}

var _ mq.Handler[*datas.Send] = (*SendHandler)(nil)

func (s *SendHandler) close() {
	close(s.SendChan)
	var err error
	for s.sendData = range s.SendChan {
		s.storeData, err = s.routed2store.Convert(s.sendData)
		if err != nil {
			slog.Error("failed to conver routed send to cache: %w", "err", err)
		}
		_, err = s.StoreProducer.Enqueue(s.storeData)
		if err != nil {
			slog.Error("failed to enqueue store mq", "err", err)
		}
	}
}

func (s *SendHandler) Start() error {
	defer s.close()
	if err := s.SendConsumer.Start(s); err != nil {
		return fmt.Errorf("cannot start send consumer: %w", err)
	}
	defer s.SendConsumer.Close()
	for s.sendData = range s.SendChan {
		if s.Err = s.Send(s.sendData.ToByte()); s.Err != nil {
			if errors.Is(s.Err, net.ErrClosed) {
				return s.Err
			}
			return fmt.Errorf("cannot send: %w", s.Err)
		}
	}
	return nil
}

type ReceiveHandler struct {
	Client
	ProcessProducer *mq.Producer
	receiveData     *datas.Receive
	streamDecoder   datas.ReceiveStreamDecoder
	receive2store   datas.Converter[*datas.Receive, *datas.Store]
	storeData       *datas.Store
	// to self sendHandler channel
	ok       bool
	sendData datas.Send
	Err      error
}

var ErrIdNotFound = errors.New("id not found, try adding first")

func (r *ReceiveHandler) Start() error {
	defer close(r.sendHandler.SendChan)
	// 只处理与业务无关的连接相关的逻辑
	reader := bufio.NewReader(r.GetReader())
	for {
		r.receiveData, r.Err = r.streamDecoder.Parse(reader)
		if r.Err != nil {
			if errors.Is(r.Err, net.ErrClosed) {
				return r.Err
			} else if errors.Is(r.Err, datas.ErrTimeLargeOffset) {
				// send fail ACK
				r.ackFail()
				slog.Error(r.Err.Error())
				continue
			} else {
				return fmt.Errorf("failed to handle receive: %w", r.Err)
			}
		}
		if r.sendHandler.id == nil {
			r.sendHandler.id = &r.receiveData.SenderId
			r.Pool.Set(*r.sendHandler.id, r.sendHandler)
		} else if *r.sendHandler.id != r.receiveData.SenderId {
			handler, ok := r.Pool.Get(r.receiveData.SenderId)
			if !ok {
				return ErrIdNotFound
			}
			r.Pool.Set(r.receiveData.SenderId, handler)
			r.Pool.Delete(*r.sendHandler.id)
		}
		// 消息类型：normal/pull(ack sequence+pull count)
		switch r.receiveData.Type {
		case datas.MessageType_NORMAL:
			// offline store
			_, r.ok = r.Pool.Get(r.receiveData.ReceiverId)
			if !r.ok {
				r.toStore()
			} else {
				// normal ACK SENDING
				r.ProcessProducer.Enqueue(r.receiveData)
				// TODO transaction chan
			}
		case datas.MessageType_PULL:
			r.toStore()
		}
		r.ackSending()
	}
}

func (r *ReceiveHandler) ackFail() {
	r.sendData.AckStatus = datas.AckStatus_FAIL
	r.sendData.MessageId = r.receiveData.MessageId
	// TODO reason bit
	r.sendHandler.SendChan <- &r.sendData
}

func (r *ReceiveHandler) ackSending() {
	r.sendData.AckStatus = datas.AckStatus_SENDING
	r.sendData.MessageId = r.receiveData.MessageId
	// TODO reason bit
	r.sendHandler.SendChan <- &r.sendData
}

func (r *ReceiveHandler) toStore() {
	r.storeData, r.Err = r.receive2store.Convert(r.receiveData)
	if r.Err != nil {
		r.ackFail()
		return
	}
	_, r.Err = r.StoreProducer.Enqueue(r.storeData)
	if r.Err != nil {
		r.ackFail()
		return
	}
}
