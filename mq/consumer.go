package mq

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/nsqio/go-nsq"
)

type Consumer[MessType any] struct {
	// 交给子类初始化
	Decoder           datas.Decoder[MessType]
	consumer          *nsq.Consumer
	NsqLookupdAddress string
}

type Handler[MessType any] interface {
	Handle(message MessType) error
}

var ErrRequeue = errors.New("require requeue")

func (c *Consumer[M]) Start(handler Handler[M]) error {
	h := func(message *nsq.Message) error {
		data, err := c.Decoder.Parse(message.Body)
		if err != nil {
			slog.Error("failed to parse during handling", "err", err)
			return nil
		}
		if err := handler.Handle(data); err != nil {
			if errors.Is(err, ErrRequeue) {
				return err
			}
			slog.Error("failed to handle", "err", err)
		}
		return nil
	}
	c.consumer.AddHandler(nsq.HandlerFunc(h))

	// Use nsqlookupd to discover nsqd instances.
	// See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err := c.consumer.ConnectToNSQLookupd(c.NsqLookupdAddress)
	if err != nil {
		return fmt.Errorf("consumer failed to connect to nsq lookup damon: %w", err)
	}
	return nil
}

func (c *Consumer[MessType]) Stop() chan int {
	c.consumer.Stop()
	return c.consumer.StopChan
}

type ReceiveConsumer = Consumer[*datas.Receive]
type SendConsumer = Consumer[*datas.Send]
type StoreConsumer = Consumer[*datas.Cache]
