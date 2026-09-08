package listener

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/network/conn"
	"github.com/gorilla/websocket"
)

type Listener interface {
	Listen(connChan chan conn.Conn) error
	Close() error
	Addr() net.Addr
}

type KcpListener struct {
	net.Listener
}

var _ Listener = (*KcpListener)(nil)

func (kcp *KcpListener) Listen(connChan chan conn.Conn) error {
	for {
		session, err := kcp.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			return fmt.Errorf("listener cannot accept kcp: %w", err)
		}
		defer session.Close()
		connChan <- &conn.KcpConn{Conn: session, Id: datas.GenId()}
	}
}

type WsListener struct {
	net.Listener
	Upgrader *websocket.Upgrader
	ConnChan chan conn.Conn
}

var _ Listener = (*WsListener)(nil)

func (ws *WsListener) Listen(connChan chan conn.Conn) error {
	ws.ConnChan = connChan
	http.HandleFunc("/ws", ws.Handle)
	return http.Serve(ws, nil)
}

func (ws *WsListener) Handle(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("unable to upgrade the HTTP server connection to the WebSocket protoco", "err", err)
		return
	}
	ws.ConnChan <- &conn.WsConn{Conn: conn, Id: datas.GenId()}
}

func StartListen(listener Listener, connBufSize int) (chan conn.Conn, chan error) {
	connChan := make(chan conn.Conn, connBufSize)
	errChan := make(chan error)
	go func() {
		if err := listener.Listen(connChan); err != nil {
			if errors.Is(err, net.ErrClosed) {
				errChan <- err
			} else {
				errChan <- fmt.Errorf("failed to listen: %w", err)
			}
		}
	}()
	slog.Info("server started", "addr", listener.Addr().String())
	return connChan, errChan
}
