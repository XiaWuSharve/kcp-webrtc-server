package client

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/network/conn"
)

type Client struct {
	conn.Conn
	wg              sync.WaitGroup
	ProcessProducer mq.Producer
	StoreProducer   mq.Producer
	SendConsumer    mq.Consumer[*datas.Send]
	ReceiveFrame    *datas.Receive
	HeaderBytes     [13]byte
	ACKSend         datas.RoutedSend
	routedSend      *datas.RoutedSend
	streamDecoder   datas.ReceiveFrameStreamDecoder
	ReceiveErr      error
	SendErr         error
	pool            *conn.ConnPool
	ok              bool
	routed2cache    datas.Converter[*datas.RoutedSend, *datas.Cache]
	cache           *datas.Cache
}

func NewClient() *Client {
	return &Client{
		ACKSend: datas.RoutedSend{
			Payload: make([]byte, 1),
		},
	}
}

func (c *Client) Start(conn conn.Conn) int64 {
	c.wg.Go(func() {
		if err := c.HandleSend(); !errors.Is(err, net.ErrClosed) {
			slog.Error("failed to handle send", "err", err)
		}
	})

	c.wg.Go(func() {
		if err := c.HandleReceive(); !errors.Is(err, net.ErrClosed) {
			slog.Error("failed to handle receive", "err", err)
		}
	})
	return c.pool.AddConn(c.Conn)
}

func (c *Client) HandleReceive() error {
	// 只处理与业务无关的连接相关的逻辑
	reader := bufio.NewReader(c.GetReader())
	for {
		c.ReceiveFrame, c.ReceiveErr = c.streamDecoder.Parse(reader)
		if c.ReceiveErr != nil {
			if errors.Is(c.ReceiveErr, net.ErrClosed) {
				return c.ReceiveErr
			} else if errors.Is(c.ReceiveErr, datas.ErrTimeLargeOffset) {
				slog.Error(c.ReceiveErr.Error())
			} else {
				return fmt.Errorf("failed to handle receive: %w", c.ReceiveErr)
			}
		}
		_, c.ReceiveErr = c.processProducer.Enqueue(c.ReceiveFrame)
		if c.ReceiveErr != nil {
			return fmt.Errorf("failed to enqueue: %w", c.ReceiveErr)
		}
		c.ACKSend.Type = datas.MessageType_ACK
		c.ACKSend.AckStatus = datas.AckStatus_SENDING
		c.ReceiveErr = c.Send(c.ACKSend.ToByte())
		if c.ReceiveErr != nil {
			if errors.Is(c.ReceiveErr, net.ErrClosed) {
				return c.ReceiveErr
			}
			return fmt.Errorf("cannot ack sending: %w", c.ReceiveErr)
		}
	}
}

func (c *Client) HandleSend() error {
	defer func() {
		if c.ok {
			c.cache, c.SendErr = c.routed2cache.Convert(c.routedSend)
			if c.SendErr != nil {
				slog.Error("failed to conver routed send to cache: %w", "err", c.SendErr)
			}
			_, c.SendErr = c.storeProducer.Enqueue(c.cache)
			if c.SendErr != nil {
				slog.Error("failed to enqueue store mq", "err", c.SendErr)
			}
		}
	}()
	// 只处理与业务无关的连接相关的逻辑
	for {
		c.routedSend, c.ok = <-c.GetSendChan()
		if !c.ok {
			return nil
		}
		if c.SendErr = c.Send(c.routedSend.ToByte()); c.SendErr != nil {
			if errors.Is(c.SendErr, net.ErrClosed) {
				return c.SendErr
			}
			return fmt.Errorf("cannot send: %w", c.SendErr)
		}
	}
}
