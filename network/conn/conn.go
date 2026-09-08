package conn

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/gorilla/websocket"
)

type Conn interface {
	GetId() int64
	GetReader() io.Reader
	GetSendChan() chan *datas.RoutedSend
	Send(data []byte) error
}

type KcpConn struct {
	net.Conn
	Err      error
	SendChan chan *datas.RoutedSend
	Id       int64
}

// GetSendChan implements [Conn].
func (c *KcpConn) GetSendChan() chan *datas.RoutedSend {
	return c.SendChan
}

var _ Conn = (*KcpConn)(nil)

func (c *KcpConn) GetReader() io.Reader {
	return c
}
func (c *KcpConn) Send(data []byte) error {
	_, c.Err = c.Write(data)
	return c.Err
}
func (c *KcpConn) GetId() int64 {
	return c.Id
}

type WsConn struct {
	*websocket.Conn
	Err      error
	reader   io.Reader
	n        int
	Id       int64
	SendChan chan *datas.RoutedSend
}

// GetSendChan implements [Conn].
func (c *WsConn) GetSendChan() chan *datas.RoutedSend {
	return c.SendChan
}

var _ Conn = (*WsConn)(nil)
var _ io.Reader = (*WsConn)(nil)

func (c *WsConn) Read(p []byte) (int, error) {
	c.n, c.Err = c.reader.Read(p)
	if c.Err != nil {
		if errors.Is(c.Err, io.EOF) {
			_, c.reader, c.Err = c.Conn.NextReader()
			if c.Err != nil {
				if websocket.IsCloseError(c.Err) {
					return 0, net.ErrClosed
				}
				return 0, fmt.Errorf("failed to read message: %w", c.Err)
			}
			return c.Read(p)
		}
	}
	return c.n, c.Err
}

func (c *WsConn) GetReader() io.Reader {
	return c
}

func (c *WsConn) Send(data []byte) error {
	return c.Conn.WriteMessage(websocket.BinaryMessage, data)
}
func (c *WsConn) GetId() int64 {
	return c.Id
}
