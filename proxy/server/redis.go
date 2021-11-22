package server

import (
	"fmt"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/net"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"

	infinicache "github.com/mason-leap-lab/infinicache/client"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
)

type RedisAdapter struct {
	server    *redeo.Server
	proxy     *Proxy
	d         int
	p         int
	addresses []string
	localAddr string
	localIdx  int
	log       logger.ILogger
}

var (
	ECMaxGoroutine = 32
)

func NewRedisAdapter(srv *redeo.Server, proxy *Proxy, d int, p int) *RedisAdapter {
	net.InitShortcut()

	addresses := config.ProxyList
	localAddr := fmt.Sprintf("%s:%d", global.ServerIp, global.BasePort)
	included := -1
	for i, address := range addresses {
		if address == localAddr {
			included = i
			break
		}
	}
	// Add place holder only if there are other proxies
	if included < 0 && len(addresses) > 0 {
		included = len(addresses)
		addresses = append(addresses, "place holder")
	}

	adapter := &RedisAdapter{
		server:    srv,
		proxy:     proxy,
		d:         d,
		p:         p,
		addresses: addresses,
		localAddr: localAddr,
		localIdx:  included,
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
	collector.Collect(collector.LogEndtoEnd, protocol.CMD_SET, util.Ifelse(ok, "200", "500"),
		int64(len(body)), t.UnixNano(), int64(dt))
}

func (a *RedisAdapter) handleGet(w resp.ResponseWriter, c *resp.Command) {
	client := a.getClient(redeo.GetClient(c.Context()))

	key := c.Arg(0).String()

	t := time.Now()
	_, reader, ok := client.EcGet(key)
	dt := time.Since(t)
	code := "500"
	size := 0
	if !ok {
		w.AppendErrorf("failed to get %s", key)
		w.Flush()
	} else if reader == nil {
		w.AppendNil()
		w.Flush()
		code = "404"
	} else {
		size = reader.Len()
		if err := w.CopyBulk(reader, int64(size)); err != nil {
			ok = false
			a.log.Warn("Error on sending %s: %v", key, err)
		}
		reader.Close()
		code = "200"
	}
	collector.Collect(collector.LogEndtoEnd, protocol.CMD_GET, code, int64(size), t.UnixNano(), int64(dt))
}

func (a *RedisAdapter) getClient(redeoClient *redeo.Client) *infinicache.Client {
	shortcut := net.Shortcut.Prepare(a.localAddr, int(redeoClient.ID()), a.d+a.p)
	if shortcut.Client == nil {
		var addresses []string
		if len(a.addresses) == 0 {
			addresses = []string{shortcut.Address}
		} else {
			addresses = make([]string, len(a.addresses))
			copy(addresses, a.addresses)
			addresses[a.localIdx] = shortcut.Address
		}

		client := infinicache.NewClient(a.d, a.p, ECMaxGoroutine)
		shortcut.Client = client
		shortcut.OnValidate = func(mock *net.MockConn) {
			go a.server.ServeForeignClient(mock.Server, false)
		}
		// Dial after shortcut set up
		client.Dial(addresses)

		go func() {
			redeoClient.WaitClose()
			client.Close()
			shortcut.Client = nil
			net.Shortcut.Invalidate(shortcut)
		}()
	}
	return shortcut.Client.(*infinicache.Client)
}

func (a *RedisAdapter) Close() {
	// Nothing
}
