package router

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/network/conn"
)

type Router struct {
	consumer      mq.Consumer[*datas.Send]
	pool          *conn.Pool
	Err           error
	storeProducer mq.Producer
	send2store    datas.Converter[*datas.Send, *datas.Store]
}

var _ mq.Handler[*datas.Send] = (*Router)(nil)

var ErrConnNotFound = errors.New("conn not exist")

func (r *Router) Start() error {
	return r.consumer.Start(r)
}

var ErrWaitingRetry = errors.New("send channel is full")

func (r *Router) Handle(d *datas.Send) error {
	handler, ok := r.pool.Get(d.ReceiverId)
	if !ok {
		storeData, err := r.send2store.Convert(d)
		if err != nil {
			return fmt.Errorf("failed to convert send to store data: %w", err)
		}
		if _, err := r.storeProducer.Enqueue(storeData); err != nil {
			return fmt.Errorf("cannot enqueue store producer: %w", err)
		}
		return nil
	}
	// r.HeaderBytes[0] = byte(frame.AckStatus)
	// binary.BigEndian.PutUint64(r.HeaderBytes[1:9], uint64(frame.ConnId))
	// binary.BigEndian.PutUint32(r.HeaderBytes[9:13], uint32(len(frame.Payload)))
	slog.Debug("client sending frame")

	select {
	case handler.SendChan <- d:
	default:
		return ErrWaitingRetry
	}

	return nil
}
