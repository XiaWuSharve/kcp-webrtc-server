package conn

import (
	"sync"

	"github.com/XiaWuSharve/whisperly/utils"
)

type Pool struct {
	// only shared memory structure are allowed to be coroutine safe (to avoid lock acquiring)
	SendHandlersByConnId *utils.ShardMap[int64, *SendHandler]
	SendHandlersByUserId *utils.ShardMap[string, *SendHandler]
	UserIdByConnId       *utils.ShardMap[int64, string]
	mu                   sync.Mutex
}

// coroutine safe
func (p *Pool) AddSendHandler(c *SendHandler) {
	p.SendHandlersByConnId.Set(c.GetId(), c)
}

func (p *Pool) RemoveSendHandler(id int64) {
	p.mu.Lock()
	con, ok := p.SendHandlersByConnId.Get(id)
	if ok {
		con.Close()
		p.SendHandlersByConnId.Delete(id)
	}
	userId, ok := p.UserIdByConnId.Get(id)
	if ok {
		p.UserIdByConnId.Delete(id)
		p.SendHandlersByUserId.Delete(userId)
	}
	p.mu.Unlock()
}

func (p *Pool) FindByUserId(id string) (*SendHandler, bool) {
	return p.SendHandlersByUserId.Get(id)
}

func (p *Pool) UpdateUserId(connId int64, userId string) {
	p.mu.Lock()
	uid, ok := p.UserIdByConnId.Get(connId)
	if ok {
		sendHandler, ok2 := p.SendHandlersByConnId.Get(connId)
		if !ok2 {
			p.RemoveSendHandler(connId)
			return
		}
		p.SendHandlersByUserId.Delete(uid)
		p.SendHandlersByUserId.Set(userId, sendHandler)
	}
	p.UserIdByConnId.Set(connId, userId)
	p.mu.Unlock()
}
