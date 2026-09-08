package conn

import (
	"context"

	"github.com/XiaWuSharve/whisperly/network/client"
	"github.com/XiaWuSharve/whisperly/utils"
)

type ConnPool struct {
	// only shared memory structure are allowed to be coroutine safe (to avoid lock acquiring)
	Conns         *utils.ShardMap[int64, Conn]
	Clients       *utils.ShardMap[string, *client.Client]
	UserId2connId *utils.ShardMap[string, int64]
	ConnChan      chan Conn
	closeSig      chan any
}

func (p *ConnPool) Close() {
	close(p.closeSig)
}

func (p *ConnPool) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case conn := <-p.ConnChan:
				c := client.NewClient()
			case <-p.closeSig:
				cancel()
				return
			}

		}
	}()
}

// coroutine safe
func (p *ConnPool) AddConn(c Conn) int64 {
	p.Conns.Set(c.GetId(), c)
	return c.GetId()
}

func (p *ConnPool) AddClient(c *client.Client) {
	p.Clients.Set(c.Conn.GetId(), c)
}
