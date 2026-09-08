package listener

import (
	"errors"
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
		con, err := kcp.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			slog.Error("listener cannot accept kcp", "err", err)
			continue
		}
		defer con.Close()
		connChan <- &conn.KcpConn{Conn: con, Id: datas.GenId()}
	}
}

type WsListener struct {
	net.Listener
	Upgrader *websocket.Upgrader
	connChan chan conn.Conn
}

var _ Listener = (*WsListener)(nil)

func (ws *WsListener) Listen(connChan chan conn.Conn) error {
	ws.connChan = connChan
	http.HandleFunc("/ws", ws.Handle)
	return http.Serve(ws, nil)
}

func (ws *WsListener) Handle(w http.ResponseWriter, r *http.Request) {
	con, err := ws.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("unable to upgrade the HTTP server connection to the WebSocket protoco", "err", err)
		return
	}
	ws.connChan <- &conn.WsConn{Conn: con, Id: datas.GenId()}
}
