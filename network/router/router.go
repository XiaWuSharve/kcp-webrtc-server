package router

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
)

type Router struct {
	consumer      mq.Consumer[*datas.Send]
	pool          *client.ConnPool
	Err           error
	storeProducer mq.Producer
	send2cache    datas.Converter[*datas.Send, *datas.Cache]
}

var _ mq.Handler[*datas.Send] = (*Router)(nil)

var ErrConnNotFound = errors.New("conn not exist")

func (r *Router) Start() error {
	return r.consumer.Start(r)
}

var ErrWaitingRetry = errors.New("send channel is full")

func (r *Router) Handle(frame *datas.Send) error {
	conn, ok := r.pool.Conns.Get(frame.ConnId)
	if !ok {
		cacheData, err := r.send2cache.Convert(frame)
		if err != nil {
			slog.Error("failed to convert send to cache data", "err", err)
			return nil
		}
		if _, err := r.storeProducer.Enqueue(cacheData); err != nil {
			return fmt.Errorf("cannot enqueue store producer: %w", err)
		}
	}

	r.HeaderBytes[0] = byte(frame.AckStatus)
	binary.BigEndian.PutUint64(r.HeaderBytes[1:9], uint64(frame.ConnId))
	binary.BigEndian.PutUint32(r.HeaderBytes[9:13], uint32(len(frame.Payload)))
	slog.Debug("client sending kcp frame")
	select {
	case conn.GetSendChan() <- append(r.HeaderBytes[0:13], frame.Payload...):
	default:
		return ErrWaitingRetry
	}

	return nil
}
