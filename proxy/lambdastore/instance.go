package lambdastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/cespare/xxhash"
	"github.com/cornelk/hashmap"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

const (
	INSTANCE_MASK_STATUS_START = 0x000F
	INSTANCE_MASK_STATUS_CONNECTION = 0x00F0
	INSTANCE_MASK_STATUS_BACKING = 0x0F00
	INSTANCE_MASK_STATUS_LIFECYCLE = 0xF000

	INSTANCE_UNSTARTED = 0
	INSTANCE_STARTED = 1
	INSTANCE_SLEEP = 0
	INSTANCE_AWAKE = 1
	INSTANCE_MAYBE = 2
	MAX_RETRY      = 3
	TEMP_MAP_SIZE  = 10
	BACKING_DISABLED = 0
	BACKING_RESERVED = 1
	BACKING_ENABLED = 2
)

var (
	Registry       InstanceRegistry
	WarmTimout     = config.InstanceWarmTimout
	DefaultConnectTimeout = 20 * time.Millisecond // Just above average triggering cost.
	MaxConnectTimeout = 1 * time.Second
	RequestTimeout = 1 * time.Second
	BackoffFactor  = 2
	timeouts       = sync.Pool{
		New: func() interface{} {
			return time.NewTimer(0)
		},
	}
	DefaultPingPayload = []byte{}
	AwsSession     = awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
		SharedConfigState: awsSession.SharedConfigEnable,
	}))
)

type InstanceRegistry interface {
	Instance(uint64) (*Instance, bool)
}

type ValidateOption struct {
	WarmUp      bool
	Command     types.Command
}

type Instance struct {
	*Deployment
	Meta

	started       uint32
	cn            *Connection
	chanCmd       chan types.Command
	chanPriorCmd  chan types.Command // Channel for priority commands: control and forwarded backing requests.
	awake        uint32
	chanValidated chan struct{}
	lastValidated *Connection
	mu            sync.Mutex
	closed        chan struct{}
	coolTimer     *time.Timer

	// Connection management
	sessions      *hashmap.HashMap

	// Backup fields
	candidates    []*Instance       // Must be initialized before invoke lambda. Stores pointers instead of ids for query, so legacy instances may be used.
	backups       []*Instance       // Actual backups in use.
	recovering    uint32            // # of backups in use, also if the recovering > 0, the instance is recovering.
	writtens      *hashmap.HashMap  // Whitelist, write opertions will be added to it during parallel recovery.
	backing       uint32            // backing status, 0 for non backing, 1 for reserved, 2 for backing.
	backingIns    *Instance
	backingId     int               // Identifier for backup, ranging from [0, # of backups)
	backingTotal  int               // Total # of backups ready for backing instance.
}

func NewInstanceFromDeployment(dp *Deployment) *Instance {
	dp.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("%s ", dp.name),
		Level:  global.Log.GetLevel(),
		Color:  !global.Options.NoColor,
	}

	chanValidated := make(chan struct{})
	close(chanValidated)

	return &Instance{
		Deployment:    dp,
		Meta:          Meta{ Term: 1 }, // Term start with 1 to avoid uninitialized term ambigulous.
		awake:         INSTANCE_SLEEP,
		chanCmd:       make(chan types.Command, 1),
		chanPriorCmd:  make(chan types.Command, 1),
		chanValidated: chanValidated, // Initialize with a closed channel.
		closed:        make(chan struct{}),
		coolTimer:     time.NewTimer(WarmTimout),
		sessions:      hashmap.New(TEMP_MAP_SIZE),
		writtens:      hashmap.New(TEMP_MAP_SIZE),
	}
}

// create new lambda instance
func NewInstance(name string, id uint64, replica bool) *Instance {
	return NewInstanceFromDeployment(NewDeployment(name, id, replica))
}

func (ins *Instance) Status() uint64 {
	// 0x000F  started
	// 0x00F0  connection
	// 0x0F00  backing
	var backing uint64
	if ins.IsRecovering() {
		backing += 1
	}
	if ins.IsBacking() {
		backing += 2
	}
	// 0xF000  lifecycle
	return uint64(atomic.LoadUint32(&ins.started)) +
		(uint64(ins.awake) << 4) +
		(backing << 8)
}

func (ins *Instance) AssignBackups(numBak int, candidates []*Instance) {
	ins.candidates = candidates
	ins.backups = make([]*Instance, 0, numBak)
}

func (ins *Instance) C() chan types.Command {
	return ins.chanCmd
}

func (ins *Instance) WarmUp() {
	ins.validate(&ValidateOption{ WarmUp: true })
	// Force reset
	ins.flagWarmed()
}

func (ins *Instance) Validate(opts ...*ValidateOption) *Connection {
	var opt *ValidateOption
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt == nil {
		opt = &ValidateOption{}
	}
	return ins.validate(opt)
}

func (ins *Instance) IsValidating() bool {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	select {
	case <-ins.chanValidated:
		return false
	default:
		return true
	}
}

// Handle incoming client requests
// lambda facing goroutine
func (ins *Instance) HandleRequests() {
	for {
		select {
		case <-ins.closed:
			return
		case cmd := <-ins.chanPriorCmd: // Priority queue get
			ins.handleRequest(cmd)
		case cmd := <-ins.chanCmd: /*blocking on lambda facing channel*/
			// Drain priority channel first.
			for len(ins.chanPriorCmd) > 0 {
				ins.handleRequest(<-ins.chanPriorCmd)
				// Check closure.
				select {
				case <-ins.closed:
					return
				default:
				}
			}
			ins.handleRequest(cmd)
		case <-ins.coolTimer.C:
			// Warmup will not work until first call.
			// Double check, for it could timeout before a previous request got handled.
			if len(ins.chanPriorCmd) > 0 || len(ins.chanCmd) > 0 || atomic.LoadUint32(&ins.started) == INSTANCE_UNSTARTED {
				ins.resetCoolTimer()
			} else {
				// Force warm up.
				ins.warmUp()
			}
		}
	}
}

// Start parallel recovery mode.
// Return # of ready backups
func (ins *Instance) StartRecovery() int {
	recovering := atomic.LoadUint32(&ins.recovering)
	if recovering > 0 {
		ins.log.Warn("Instance is recovering")
		return int(recovering)
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	return ins.startRecoveryLocked()
}

func (ins *Instance) startRecoveryLocked() int {
	if recovering := atomic.LoadUint32(&ins.recovering); recovering > 0 {
		ins.log.Warn("Instance is recovering")
		return int(recovering)
	}

	// Reset backups
	lastnum := len(ins.backups)
	changes := 0
	ins.backups = ins.backups[:0]
	// Reserve backups so we can know how many backups are available
	alters := cap(ins.backups)    // If failed to reserve a backup, select one start from alters.
	offset := 0                   // Offset based on alters.
	tested := make([]bool, len(ins.candidates) - alters) // Count start from alters
	for i := 0; i < cap(ins.backups); i++ {
		if ins.candidates[i].ReserveBacking() {
			changes += ins.promoteCandidate(i, i)
			continue
		}
		// Try alter + i to keep backingID stable.
		if alters + i < len(ins.candidates) && !tested[i] && ins.candidates[alters + i].ReserveBacking() {
			// exchange candidates
			changes += ins.promoteCandidate(i, alters + i)
			tested[i] = true
			continue
		}
		// Try find whatever possible
		for ; offset < len(tested); offset++ {
			if !tested[offset] && ins.candidates[alters + offset].ReserveBacking() {
				changes += ins.promoteCandidate(i, alters + offset)
				tested[offset] = true
				break
			} else {
				tested[offset] = true
			}
		}
		// TODO: hole here if not avaiable
	}
	if len(ins.backups) != lastnum {
		// The difference of total changes everything.
		changes = len(ins.backups)
	}

	// Start backups.
	var msg strings.Builder
	for i, candid := range ins.backups {
		candid.StartBacking(ins, i, len(ins.backups))
		msg.WriteString(" ")
		msg.WriteString(strconv.FormatUint(candid.Id(), 10))
	}

	atomic.StoreUint32(&ins.recovering, uint32(len(ins.backups)))
	if len(ins.backups) > 0 {
		ins.log.Debug("Parallel recovery started with %d backup instances: %s, changes: %d", len(ins.backups), msg.String(), changes)
	} else {
		ins.log.Warn("Unable to start parallel recovery due to no backup instances available")
	}

	return len(ins.backups)
}

// Resume serving
func (ins *Instance) ResumeServing() {
	ins.mu.Lock()
	atomic.StoreUint32(&ins.recovering, 0)
	for _, backup := range ins.backups {
		backup.StopBacking(ins)
	}
	// Clear whitelist during fast recovery.
	if ins.writtens.Len() > 0 {
		ins.writtens = hashmap.New(TEMP_MAP_SIZE)
	}
	ins.mu.Unlock()
	ins.log.Debug("Recovered and service resumed")
}

func (ins *Instance) IsRecovering() bool {
	return atomic.LoadUint32(&ins.recovering) > 0
}

// Check if the instance is available for serving as a backup for specified instance.
// Return false if the instance is backing another instance.
func (ins *Instance) ReserveBacking() bool {
	return atomic.LoadUint32(&ins.recovering) == 0 &&
		atomic.CompareAndSwapUint32(&ins.backing, BACKING_DISABLED, BACKING_RESERVED)
}

// Start serving as the backup for specified instance.
// Return false if the instance is backing another instance.
func (ins *Instance) StartBacking(bakIns *Instance, bakId int, total int) bool {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if atomic.LoadUint32(&ins.backing) != BACKING_RESERVED {
		ins.log.Error("Please call ReserveBacking before StartBacking")
		return false
	}

	ins.backingIns = bakIns
	ins.backingId = bakId
	ins.backingTotal = total
	atomic.StoreUint32(&ins.backing, BACKING_ENABLED)

	// Manually trigger ping with payload to initiate parallel recovery
	payload, err := ins.backingIns.Meta.ToCmdPayload(ins.backingIns.Id(), bakId, total)
	if err != nil {
		ins.log.Warn("Failed to prepare payload to trigger recovery: %v", err)
	} else {
		ins.chanPriorCmd <- &types.Control{
			Cmd: protocol.CMD_PING,
			Payload: payload,
		}
	}
	return true
}

// Stop serving as a backup
func (ins *Instance) StopBacking(bakIns *Instance) {
	if ins.backingIns != bakIns {
		return
	}
	ins.mu.Lock()
	atomic.StoreUint32(&ins.backing, BACKING_DISABLED)
	ins.mu.Unlock()
}

func (ins *Instance) IsBacking() bool {
	return atomic.LoadUint32(&ins.backing) == BACKING_ENABLED
}

func (ins *Instance) Switch(to types.LambdaDeployment) *Instance {
	temp := &Deployment{}
	ins.Reset(to, temp)
	to.Reset(temp, nil)
	return ins
}

// TODO: Add sid support, proxy now need sid to connect.
func (ins *Instance) Migrate() error {
	// func launch Mproxy
	// get addr if Mproxy
	dply, err := global.Migrator.GetDestination(ins.Id())
	if err != nil {
		ins.log.Error("Failed to find a migration destination: %v", err)
		return err
	}

	addr, err := global.Migrator.StartMigrator(ins.Id())
	if err != nil {
		ins.log.Error("Failed to start a migrator: %v", err)
		return err
	}
	// expand local address
	if addr[0] == ':' {
		addr = global.ServerIp + addr
	}

	ins.log.Info("Initiating migration to %s...", dply.Name())
	ins.chanCmd <- &types.Control{
		Cmd:        "migrate",
		Addr:       addr,
		Deployment: dply.Name(),
		Id:         dply.Id(),
	}
	return nil
}

func (ins *Instance) Close() {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.isClosedLocked() {
		return
	}

	ins.log.Debug("Closing...")
	close(ins.closed)
	if !ins.coolTimer.Stop() {
		select {
		case <-ins.coolTimer.C:
		default:
		}
	}
	if ins.cn != nil {
		ins.cn.Close()
		ins.cn = nil
	}
	atomic.StoreUint32(&ins.awake, INSTANCE_SLEEP)
	ins.flagValidatedLocked(nil)
	ins.log.Info("Closed")
}

func (ins *Instance) IsClosed() bool {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	return ins.isClosedLocked()
}

func (ins *Instance) getSid() string {
	return uuid.New().String()
}

func (ins *Instance) initSession() string {
	sid := ins.getSid()
	ins.sessions.Set(sid, false)
	return sid
}

func (ins *Instance) startSession(sid string) bool {
	return ins.sessions.Cas(sid, false, true)
}

func (ins *Instance) endSession(sid string) {
	ins.sessions.Del(sid)
}

func (ins *Instance) validate(opt *ValidateOption) *Connection {
	ins.mu.Lock()

	select {
	case <-ins.chanValidated:
		// Not validating. Validate...
		ins.chanValidated = make(chan struct{})
		ins.lastValidated = nil
		ins.mu.Unlock()

		connectTimeout := DefaultConnectTimeout
		for {
			ins.log.Debug("Validating...")
			triggered := atomic.LoadUint32(&ins.awake) == INSTANCE_SLEEP && ins.tryTriggerLambda(opt)
			if triggered {
				return ins.validated()
			} else if opt.WarmUp && !global.IsWarmupWithFixedInterval() {
				return ins.flagValidated(ins.cn, "", false) // No new session involved.
			}

			// Ping is issued to ensure awake
			if opt.Command != nil && opt.Command.String() == protocol.CMD_PING {
				ins.log.Debug("Ping with payload")
				ins.cn.Ping(opt.Command.(*types.Control).Payload)
			} else {
				ins.cn.Ping(DefaultPingPayload)
			}

			// Start timeout, ping may get stucked anytime.
			timeout := timeouts.Get().(*time.Timer)
			if !timeout.Stop() {
				select {
				case <-timeout.C:
				default:
				}
			}
			timeout.Reset(connectTimeout)

			select {
			case <-timeout.C:
				// Set status to dead and revalidate.
				timeouts.Put(timeout)
				// If instance is not invoked by proxy, it may be slept
				if atomic.CompareAndSwapUint32(&ins.awake, INSTANCE_MAYBE, INSTANCE_SLEEP) {
					ins.log.Warn("Timeout on validating, assuming instance dead and reinvoke...")
					// Close or not? Maybe we can wait until a connection comes.
					// ins.cn.Close()
					// ins.cn = nil
				} else {
					// Exponential backoff
					connectTimeout *= time.Duration(BackoffFactor)
					if connectTimeout > MaxConnectTimeout {
						connectTimeout = MaxConnectTimeout
					}
					ins.log.Warn("Timeout on validating, re-ping...")
				}
			case <-ins.chanValidated:
				timeouts.Put(timeout)
				return ins.validated()
			}
		}
	default:
		// Validating... Wait and return false
		ins.mu.Unlock()
		return ins.validated()
	}
}

func (ins *Instance) tryTriggerLambda(opt *ValidateOption) bool {
	if atomic.LoadUint32(&ins.awake) == INSTANCE_AWAKE {
		return false
	}

	if opt.WarmUp {
		ins.log.Info("[Lambda store is not awake, warming up...]")
	} else {
		ins.log.Info("[Lambda store is not awake, activating...]")
	}
	go ins.triggerLambda(opt)

	return true
}

func (ins *Instance) triggerLambda(opt *ValidateOption) {
	ins.triggerLambdaLocked(opt)
	for {
		if !ins.IsValidating() {
			// Don't overwrite the MAYBE status.
			atomic.CompareAndSwapUint32(&ins.awake, INSTANCE_AWAKE, INSTANCE_SLEEP)
			return
		}

		// Validating, retrigger.
		ins.log.Info("[Validating lambda store,  reactivateing...]")
		ins.triggerLambdaLocked(opt)
	}
}

func (ins *Instance) triggerLambdaLocked(opt *ValidateOption) {
	if ins.Meta.Stale {
		// TODO: Check stale status
		ins.log.Warn("Detected stale meta: %d", ins.Meta.Term)
	}
	client := lambda.New(AwsSession, &aws.Config{Region: aws.String(config.AWSRegion)})

	tips := &url.Values{}
	if opt.Command != nil && opt.Command.String() == protocol.CMD_GET {
		tips.Set(protocol.TIP_SERVING_KEY, opt.Command.GetRequest().Key)
	}

	var status protocol.Status
	if !ins.IsBacking() {
		// Main store only
		status = protocol.Status{ *ins.Meta.ToProtocolMeta(ins.Id()) }
		status[0].Tip = tips.Encode()
	} else {
		// Main store + backing store
		status = protocol.Status{
			*ins.Meta.ToProtocolMeta(ins.Id()),
			*ins.backingIns.Meta.ToProtocolMeta(ins.backingIns.Id()),
		}
		if opt.Command != nil && opt.Command.String() == protocol.CMD_GET && opt.Command.GetRequest().InsId == ins.Id() {
			// Request is for main store, reset tips. Or tips will accumulatively used for backing store.
			status[0].Tip = tips.Encode()
			tips = &url.Values{}
		}
		// Add backing infos to tips
		tips.Set(protocol.TIP_BACKUP_KEY, strconv.Itoa(ins.backingId))
		tips.Set(protocol.TIP_BACKUP_TOTAL, strconv.Itoa(ins.backingTotal))
		status[1].Tip = tips.Encode()
	}
	event := &protocol.InputEvent{
		Sid:    ins.initSession(),
		Cmd:    protocol.CMD_PING,
		Id:     ins.Id(),
		Proxy:  fmt.Sprintf("%s:%d", global.ServerIp, global.BasePort+1),
		Prefix: global.Options.Prefix,
		Log:    global.Log.GetLevel(),
		Flags:  global.Flags,
		Backups:len(ins.candidates),
		Status: status,
	}
	if opt.WarmUp {
		event.Cmd = protocol.CMD_WARMUP
	}
	payload, err := json.Marshal(event)
	if err != nil {
		ins.log.Error("Failed to marshal payload of lambda input: %v", err)
	}
	input := &lambda.InvokeInput{
		FunctionName: aws.String(ins.Name()),
		Payload:      payload,
	}

	ins.Meta.Stale = true
	output, err := client.Invoke(input)
	ins.endSession(event.Sid)
	// Don't reset connection here, fronzen but not dead.
	// ins.cn = nil
	if err != nil {
		ins.log.Error("Error on activating lambda store: %v", err)
	} else {
		ins.log.Debug("[Lambda store is deactivated]")
	}
	if output != nil && len(output.Payload) > 0 {
		var outputStatus protocol.Status
		var outputError protocol.OutputError
		if err := json.Unmarshal(output.Payload, &outputError); err == nil {
			ins.log.Error("[Lambda deactivated with error]: %v", outputError)
		} else if err := json.Unmarshal(output.Payload, &outputStatus); err != nil {
			ins.log.Error("Failed to unmarshal payload of lambda output: %v, payload", err, string(output.Payload))
		} else if len(outputStatus) > 0 {
			uptodate := ins.Meta.FromProtocolMeta(&outputStatus[0])  // Ignore backing store
			if uptodate {
				// If the node was invoked by other than the proxy, it could be stale.
				if atomic.LoadUint32(&ins.awake) != INSTANCE_MAYBE {
					ins.Meta.Stale = false
				} else {
					uptodate = false
				}
			}

			if uptodate {
				ins.log.Debug("Got updated instance lineage: %v", &outputStatus)
			} else {
				ins.log.Debug("Got staled instance lineage: %v", &outputStatus)
			}
		}
	} else if event.IsPersistencyEnabled() {
		ins.log.Error("No instance lineage returned, output: %v", output)
	}
}

func (ins *Instance) flagValidated(conn *Connection, sid string, recoveryRequired bool) *Connection {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.isClosedLocked() {
		// Simple return, the conn will close itself.
		return conn
	}

	ins.flagWarmed()
	if ins.cn != conn {
		// Check possible duplicated session
		if !ins.startSession(sid) {
			// Deny session
			return conn
		}
		ins.log.Debug("Session %s started.", sid)

		oldConn := ins.cn

		// Set instance, order matters here.
		conn.instance = ins
		conn.log = ins.log
		ins.cn = conn

		if oldConn != nil {
			oldConn.Close()

			if oldConn.instance == ins {
				// There are two possibilities for connectio switch:
				// 1. Old node get reclaimed
				atomic.StoreUint32(&ins.awake, INSTANCE_AWAKE)
				// 2. Migration, which we have no way to know its status.
				// TODO: Distinguish with case 1 using sid
				// atomic.StoreUint32(&ins.awake, INSTANCE_MAYBE)
			} else {
				ins.log.Warn("I can't believe this, you find a misplaced instance: %d", oldConn.instance.Id())
			}
		} else {
			atomic.StoreUint32(&ins.awake, INSTANCE_AWAKE)
		}
	} else {
		ins.startSession(sid)

		// For instance not invoked by proxy (INSTANCE_MAYBE), keep status.
		atomic.CompareAndSwapUint32(&ins.awake, INSTANCE_SLEEP, INSTANCE_AWAKE)
	}

	if recoveryRequired {
		ins.log.Debug("Parallel recovery requested.")
		ins.startRecoveryLocked()
	}
	return ins.flagValidatedLocked(conn)
}

func (ins *Instance) bye(conn *Connection) {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.cn != conn {
		return
	}

	if !atomic.CompareAndSwapUint32(&ins.awake, INSTANCE_MAYBE, INSTANCE_SLEEP) {
		ins.log.Debug("Bye ignored, waiting for the return of synchronous invocation.")
	}
}

func (ins *Instance) flagValidatedLocked(conn *Connection) *Connection {
	select {
	case <-ins.chanValidated:
		// Validated
	default:
		if conn != nil {
			ins.log.Debug("Validated")
			ins.lastValidated = conn
		}
		close(ins.chanValidated)
	}
	return ins.lastValidated
}

func (ins *Instance) validated() *Connection {
	<-ins.chanValidated
	return ins.lastValidated
}

func (ins *Instance) flagClosed(conn *Connection) {
	if ins.cn != conn {
		return
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.cn != conn {
		return
	}

	ins.cn = nil
	atomic.StoreUint32(&ins.awake, INSTANCE_SLEEP)
}

func (ins *Instance) handleRequest(cmd types.Command) {
	// On parallel recovering, we will try reroute get requests.
	if ins.IsRecovering() && cmd.String() == protocol.CMD_GET && ins.rerouteGetRequest(cmd.GetRequest()) {
		return
	}

	var err error
	var retries = MAX_RETRY
	if !cmd.Retriable() {
		retries = 1
	}

	for i := 0; i < retries; i++ {
		if i > 0 {
			ins.log.Debug("Attempt %d", i)
		}
		// Check lambda status first
		validateStart := time.Now()
		// Once active connection is confirmed, keep awake on serving.
		conn := ins.Validate(&ValidateOption{ Command: cmd })
		validateDuration := time.Since(validateStart)

		if conn == nil {
			// Check if conn is valid, nil if ins get closed
			return
		}
		err = ins.request(conn, cmd, validateDuration)
		if err == nil {
			break
		}
	}
	if err != nil {
		if cmd.Retriable() {
			ins.log.Error("Max retry reaches, give up")
		} else {
			ins.log.Error("Can not retry a streaming request, give up")
		}
		if request, ok := cmd.(*types.Request); ok {
			request.SetResponse(err)
		}
	}

	// Reset timer
	ins.flagWarmed()
}

func (ins *Instance) rerouteGetRequest(req *types.Request) bool {
	// Rerouted keys will not be rerouted.
	// During parallel recovery, the instance can be backing another instance. (Backing before recovery triggered)
	if req.InsId != ins.Id() {
		return false
	}

	// Written keys during recovery will not be rerouted.
	if _, ok := ins.writtens.Get(req.Key); ok {
		return false
	}

	bakId := xxhash.Sum64([]byte(req.Key)) % uint64(len(ins.backups))
	ins.backups[bakId].chanPriorCmd <- req	 // Rerouted request should not be queued again.
	ins.log.Debug("Rerouted %s to node %d as backup %d.", req.Key, ins.backups[bakId].Id(), bakId)
	return true
}


func (ins *Instance) request(conn *Connection, cmd types.Command, validateDuration time.Duration) error {
	switch cmd.(type) {
	case *types.Request:
		req := cmd.(*types.Request)

		cmd := strings.ToLower(req.Cmd)
		if req.EnableCollector {
			err := collector.Collect(collector.LogValidate, cmd, req.Id.ReqId, req.Id.ChunkId, int64(validateDuration));
			if err != nil {
				ins.log.Warn("Fail to record validate duration: %v", err)
			}
		}

		switch cmd {
		case protocol.CMD_SET: /*set or two argument cmd*/
			req.PrepareForSet(conn)
			// If parallel recovery is triggered, record keys set during recovery.
			if ins.IsRecovering() {
				ins.writtens.Set(req.Key, &struct{}{})
			}
		case protocol.CMD_GET: /*get or one argument cmd*/
			req.PrepareForGet(conn)
			// If parallel recovery is triggered, there is no need to forward the serving key.
		case protocol.CMD_DEL:
			req.PrepareForDel(conn)
			if ins.IsRecovering() {
				ins.writtens.Set(req.Key, &struct{}{})
			}
		default:
			req.SetResponse(errors.New(fmt.Sprintf("Unexpected request command: %s", cmd)))
			// Unrecoverable
			return nil
		}

		// In case there is a request already, wait to be consumed (for response).
		conn.chanWait <- req
		if err := req.Flush(RequestTimeout); err != nil {
			ins.log.Warn("Flush request error: %v", err)
			// Remove request.
			select {
			case <-conn.chanWait:
			default:
			}
			return err
		}

	case *types.Control:
		ctrl := cmd.(*types.Control)
		cmd := strings.ToLower(ctrl.Cmd)
		isDataRequest := false

		switch cmd {
		case protocol.CMD_PING:
			// Simply ignore.
			return nil
		case protocol.CMD_DATA:
			ctrl.PrepareForData(conn)
			isDataRequest = true
		case protocol.CMD_MIGRATE:
			ctrl.PrepareForMigrate(conn)
		case protocol.CMD_DEL:
			ctrl.PrepareForDel(conn)
			if ins.IsRecovering() {
				ins.writtens.Set(ctrl.Request.Key, &struct{}{})
			}
		default:
			ins.log.Error("Unexpected control command: %s", cmd)
			// Unrecoverable
			return nil
		}

		if err := ctrl.Flush(RequestTimeout); err != nil {
			ins.log.Error("Flush control error: %v", err)
			if isDataRequest {
				global.DataCollected.Done()
			}
			// Control commands are valid to connection only.
			return nil
		}

	default:
		ins.log.Error("Unexpected request type: %v", reflect.TypeOf(cmd))
		// Unrecoverable
		return nil
	}

	return nil
}

func (ins *Instance) isClosedLocked() bool {
	select {
	case <-ins.closed:
		// already closed
		return true
	default:
		return false
	}
}

func (ins *Instance) warmUp() {
	ins.validate(&ValidateOption{ WarmUp: true })
	// Force reset
	ins.resetCoolTimer()
}

func (ins *Instance) flagWarmed() {
	atomic.StoreUint32(&ins.started, INSTANCE_STARTED)
	if global.IsWarmupWithFixedInterval() {
		return
	}

	ins.resetCoolTimer()
}

func (ins *Instance) resetCoolTimer() {
	if !ins.coolTimer.Stop() {
		select {
		case <-ins.coolTimer.C:
		default:
		}
	}
	ins.coolTimer.Reset(WarmTimout)
}

func (ins *Instance) promoteCandidate(dest int, src int) int {
	if dest != src {
		// Exchange candidates to keep the next elections get a stable result
		ins.candidates[dest], ins.candidates[src] = ins.candidates[src], ins.candidates[dest]
	}
	ins.backups = ins.backups[:dest + 1]
	change := ins.backups[dest] != nil && ins.backups[dest].Id() != ins.candidates[dest].Id()
	ins.backups[dest] = ins.candidates[dest]
	if change {
		return 1
	} else {
		return 0
	}
}

func (ins *Instance) CollectData() {
	if atomic.LoadUint32(&ins.started) == INSTANCE_UNSTARTED {
		return
	}

	global.DataCollected.Add(1)
	ins.C() <- &types.Control{Cmd: "data"}
}

func (ins *Instance) FlagDataCollected(ok string) {
	if atomic.LoadUint32(&ins.started) == INSTANCE_UNSTARTED {
		return
	}

	ins.log.Debug("Data collected: %s", ok)
	global.DataCollected.Done()
}
