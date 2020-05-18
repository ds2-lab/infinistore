package server

import (
	"fmt"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	infinicache "github.com/mason-leap-lab/infinicache/client"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
)

type RedisAdapter struct {
	server       *redeo.Server
	proxy        *Proxy
	d            int
	p            int
	addresses    []string
	local        int
	log          logger.ILogger
}

var (
	ECMaxGoroutine = 32
)

func NewRedisAdapter(srv *redeo.Server, proxy *Proxy, d int, p int) *RedisAdapter {
	protocol.InitShortcut()

	addresses := config.ProxyList
	localhost := fmt.Sprintf("%s:%d", global.ServerIp, global.BasePort)
	included := -1
	for i, address := range addresses {
		if address == localhost {
			included = i
			break
		}
	}
	if included < 0 && len(addresses) > 0 {
		included = len(addresses)
		addresses = append(addresses, "address template")
	}

	adapter := &RedisAdapter{
		server: srv,
		proxy: proxy,
		d: d,
		p: p,
		addresses: addresses,
		local: included,
		log: &logger.ColorLogger{
			Prefix: "RedisAdapter ",
			Level:  global.Log.GetLevel(),
			Color:  !global.Options.NoColor,
		},
	}

	srv.HandleFunc(protocol.CMD_SET, adapter.handleSet)
	srv.HandleFunc(protocol.CMD_GET, adapter.handleGet)

	return adapter
}

// from client
func (a *RedisAdapter) handleSet(w resp.ResponseWriter, c *resp.Command) {
	client := a.getClient(redeo.GetClient(c.Context()))

	key := c.Arg(0).String()
	body := c.Arg(1).Bytes()

	t := time.Now()
	_, ok := client.EcSet(key, body)
	dt := time.Since(t)
	if !ok {
		w.AppendErrorf("failed to set %s", key)
		w.Flush()
	} else {
		w.AppendInlineString("OK")
		w.Flush()
	}
	collector.Collect(collector.LogEndtoEnd, protocol.CMD_GET, util.Ifelse(ok, "200", "500"),
		int64(len(body)), t.UnixNano(), int64(dt))
}

func (a *RedisAdapter) handleGet(w resp.ResponseWriter, c *resp.Command) {
	client := a.getClient(redeo.GetClient(c.Context()))

	key := c.Arg(0).String()

	meta, ok := a.proxy.placer.Get(key, 0)
	if !ok || meta.Deleted {
		w.AppendNil()
		w.Flush()
		a.log.Warn("KEY %s not found, please set first.", key)
		collector.Collect(collector.LogEndtoEnd, protocol.CMD_GET, "404",
			int64(0), time.Now().UnixNano(), int64(0))
		return
	}

	t := time.Now()
	_, reader, ok := client.EcGet(key, int(meta.Size))
	dt := time.Since(t)
	if !ok {
		w.AppendErrorf("failed to get %s", key)
		w.Flush()
	} else {
		if err := w.CopyBulk(reader, meta.Size); err != nil {
			ok = false
			a.log.Warn("Error on sending %s: %v", key, err)
		}
		reader.Close()
	}
	collector.Collect(collector.LogEndtoEnd, protocol.CMD_GET, util.Ifelse(ok, "200", "500"),
		meta.Size, t.UnixNano(), int64(dt))
}

func (a *RedisAdapter) getClient(redeoClient *redeo.Client) *infinicache.Client {
	shortcut := protocol.Shortcut.Prepare(int(redeoClient.ID()), a.d + a.p)
	if shortcut.Client == nil {
		var addresses []string
		if len(a.addresses) == 0 {
			addresses = []string{ shortcut.Address }
		} else {
			addresses = make([]string, len(a.addresses))
			copy(addresses, a.addresses)
			addresses[a.local] = shortcut.Address
		}

		client := infinicache.NewClient(a.d, a.p, ECMaxGoroutine)
		client.Dial(addresses)
		shortcut.Client = client
		for _, conn := range shortcut.Conns {
			go a.server.ServeForeignClient(conn.Server, false)
		}
		go func() {
			redeoClient.WaitClose()
			client.Close()
			shortcut.Client = nil
			protocol.Shortcut.Invalidate(shortcut)
		}()
	}
	return shortcut.Client.(*infinicache.Client)
}

func (a *RedisAdapter) Close() {
	// Nothing
}
