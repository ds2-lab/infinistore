package server

import (
	"fmt"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/client"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
)

type RedisAdapter struct {
	shortcut protocol.ShortcutConnection
	client   *client.Client
	proxy    *Proxy
	log      logger.ILogger
}

var (
	ECMaxGoroutine = 32
)

func NewRedisAdapter(srv *redeo.Server, proxy *Proxy, d int, p int) *RedisAdapter {
	shortcut := protocol.InitShortcut(d + p)
	cli := client.NewClient(d, p, 32)
	addresses := config.ProxyList
	localhost := fmt.Sprintf("%s:%d", global.ServerIp, global.BasePort)
	included := false
	for i, address := range addresses {
		if address == localhost {
			addresses[i] = client.BUILDIN_PROXY    // Use mock connection.
			included = true
		}
	}
	if !included {
		addresses = append(addresses, client.BUILDIN_PROXY)
	}
	cli.Dial(addresses)

	adapter := &RedisAdapter{
		shortcut: shortcut,
		client: cli,
		proxy: proxy,
		log: &logger.ColorLogger{
			Prefix: "RedisAdapter ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
	}

	srv.HandleFunc(protocol.CMD_SET, adapter.handleSet)
	srv.HandleFunc(protocol.CMD_GET, adapter.handleGet)
	for i := 0; i < len(adapter.shortcut); i++ {
		go srv.ServeForeignClient(adapter.shortcut[i].Server)
	}

	return adapter
}

// from client
func (a *RedisAdapter) handleSet(w resp.ResponseWriter, c *resp.Command) {
	key := c.Arg(0).String()
	body := c.Arg(1).Bytes()

	if _, ok := a.client.EcSet(key, body); !ok {
		w.AppendErrorf("Failed to set %s.", key)
		w.Flush()
		return
	}

	w.AppendInlineString("OK")
	w.Flush()
}

func (a *RedisAdapter) handleGet(w resp.ResponseWriter, c *resp.Command) {
	key := c.Arg(0).String()

	meta, ok := a.proxy.metaStore.Get(key, 0)
	if !ok || meta.Deleted {
		a.log.Warn("KEY %s not found, please set first.", key)
		w.AppendNil()
		w.Flush()
		return
	}

	if _, reader, ok := a.client.EcGet(key, int(meta.Size)); !ok {
		w.AppendNil()
		w.Flush()
	} else {
		if err := w.CopyBulk(reader, meta.Size); err != nil {
			a.log.Warn("Error on sending %s: %v", key, err)
		}
		reader.Close()
	}
}

func (a *RedisAdapter) Close() {
	a.shortcut.Close()
}
