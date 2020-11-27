package lambdastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util/promise"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/zhangjyr/hashmap"
)

const (
	INSTANCE_MASK_STATUS_START      = 0x000F
	INSTANCE_MASK_STATUS_CONNECTION = 0x00F0
	INSTANCE_MASK_STATUS_BACKING    = 0x0F00
	INSTANCE_MASK_STATUS_LIFECYCLE  = 0xF000

	// Start status
	INSTANCE_UNSTARTED = 0
	INSTANCE_RUNNING   = 1
	INSTANCE_CLOSED    = 2
	INSTANCE_SHADOW    = 15

	// Connection status
	// Activate: Sleeping -> Activating -> Active
	// Retry:    Activating/Active -> Activate (validating)
	// Abandon:  Activating/Active -> Sleeping (validating, warmup)
	// Sleep:    Active -> Sleeping
	// Switch:   Active -> Maybe (Unmanaged)
	// Sleep:    Maybe -> Sleeping
	INSTANCE_SLEEPING   = 0
	INSTANCE_ACTIVATING = 1
	INSTANCE_ACTIVE     = 2
	INSTANCE_MAYBE      = 3

	// Backing status
	INSTANCE_RECOVERING = 1
	INSTANCE_BACKING    = 2

	// Lifecycle status
	PHASE_ACTIVE       = 0 // Instance is actively serving main repository and backup
	PHASE_BACKING_ONLY = 1 // Instance is expiring and serving backup only, warmup should be degraded.
	PHASE_RECLAIMED    = 2 // Instance has been reclaimed.
	PHASE_EXPIRED      = 3 // Instance is expired, no invocation will be made, and it is safe to recycle.

	MAX_ATTEMPTS     = 3
	TEMP_MAP_SIZE    = 10
	BACKING_DISABLED = 0
	BACKING_RESERVED = 1
	BACKING_ENABLED  = 2

	DESCRIPTION_UNSTARTED  = "unstarted"
	DESCRIPTION_CLOSED     = "closed"
	DESCRIPTION_SLEEPING   = "sleeping"
	DESCRIPTION_ACTIVATING = "activating"
	DESCRIPTION_ACTIVE     = "active"
	DESCRIPTION_MAYBE      = "unmanaged"
	DESCRIPTION_UNDEFINED  = "undefined"
)

var (
	CM                    ClusterManager
	WarmTimeout           = config.InstanceWarmTimeout
	DefaultConnectTimeout = 20 * time.Millisecond // Just above average triggering cost.
	MaxConnectTimeout     = 1 * time.Second
	RequestTimeout        = 1 * time.Second
	ValidationTimeout     = 80 * time.Millisecond // The minimum interval between validations.
	BackoffFactor         = 2
	MaxControlRequestSize = int64(200000) // 200KB, which can be transmitted in 20ms.
	DefaultPingPayload    = []byte{}
	AwsSession            = awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
		SharedConfigState: awsSession.SharedConfigEnable,
	}))

	// Errors
	ErrInstanceClosed    = errors.New("instance closed")
	ErrInstanceReclaimed = errors.New("instance reclaimed")
	ErrInstanceSleeping  = errors.New("instance is sleeping")
	ErrDuplicatedSession = errors.New("session has started")
	ErrNotCtrlLink       = errors.New("not control link")
	ErrInstanceValidated = errors.New("instance has been validated by another connection")
	ErrInstanceBusy      = errors.New("instance busy")
	ErrUnexpectedReturn  = errors.New("unexpected return from validation")
	ErrUnknown           = errors.New("unknown error")
)

type InstanceManager interface {
	Instance(uint64) *Instance
	Recycle(types.LambdaDeployment) error
}

type Relocator interface {
	// Relocate relocate the chunk specified by the meta(interface{}) and chunkId(int).
	// Return the instance the chunk is relocated to.
	Relocate(interface{}, int, types.Command) (*Instance, error)

	// TryRelocate Test and relocate the chunk specified by the meta(interface{}) and chunkId(int).
	// Return the instance, trigggered or not(bool), and error if the chunk is triggered.
	TryRelocate(interface{}, int, types.Command) (*Instance, bool, error)
}

type ClusterManager interface {
	InstanceManager
	Relocator
}

type ValidateOption struct {
	Notifier  chan struct{}
	Validated *Connection
	Error     error

	// Options
	WarmUp  bool
	Command types.Command
}

type Instance struct {
	*Deployment
	Meta

	chanCmd      chan types.Command
	chanPriorCmd chan types.Command // Channel for priority commands: control and forwarded backing requests.
	status       uint32             // Status of proxy side instance which can be one of unstarted, running, and closed.
	awakeness    uint32             // Status of lambda node which can be one of sleeping, activating, active, and maybe.
	phase        uint32             // Status of serving mode which can be one of active, backing only, reclaimed, and expired.
	validated    promise.Promise
	mu           sync.Mutex
	closed       chan struct{}
	coolTimer    *time.Timer
	coolTimeout  time.Duration

	// Connection management
	ctrlLink *Connection
	dataLink *Connection
	sessions *hashmap.HashMap

	// Backup fields
	backups      Backups
	recovering   uint32           // # of backups in use, also if the recovering > 0, the instance is recovering.
	writtens     *hashmap.HashMap // Whitelist, write opertions will be added to it during parallel recovery.
	backing      uint32           // backing status, 0 for non backing, 1 for reserved, 2 for backing.
	backingIns   *Instance
	backingId    int // Identifier for backup, ranging from [0, # of backups)
	backingTotal int // Total # of backups ready for backing instance.
}

func NewInstanceFromDeployment(dp *Deployment, id uint64) *Instance {
	dp.id = id
	dp.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("%s-%d ", dp.name, dp.id),
		Level:  global.Log.GetLevel(),
		Color:  !global.Options.NoColor,
	}

	return &Instance{
		Deployment: dp,
		Meta: Meta{
			Term:     1,
			Capacity: global.Options.GetInstanceCapacity(),
			size:     config.InstanceOverhead,
		}, // Term start with 1 to avoid uninitialized term ambigulous.
		awakeness:    INSTANCE_SLEEPING,
		chanCmd:      make(chan types.Command, 1),
		chanPriorCmd: make(chan types.Command, 1),
		validated:    promise.Resolved(), // Initialize with a resolved promise.
		closed:       make(chan struct{}),
		coolTimer:    time.NewTimer(time.Duration(rand.Int63n(int64(WarmTimeout)) + int64(WarmTimeout)/2)), // Differentiate the time to start warming up.
		coolTimeout:  WarmTimeout,
		sessions:     hashmap.New(TEMP_MAP_SIZE),
		writtens:     hashmap.New(TEMP_MAP_SIZE),
	}
}

// create new lambda instance
func NewInstance(name string, id uint64) *Instance {
	return NewInstanceFromDeployment(NewDeployment(name, id), id)
}

func (ins *Instance) GetShadowInstance() *Instance {
	return &Instance{status: INSTANCE_SHADOW}
}

func (ins *Instance) String() string {
	return ins.Description()
}

func (ins *Instance) Status() uint64 {
	// 0x000F  status
	// 0x00F0  connection
	// 0x0F00  backing
	var backing uint64
	if ins.IsRecovering() {
		backing += INSTANCE_RECOVERING
	}
	if ins.IsBacking(true) {
		backing += INSTANCE_BACKING
	}
	// 0xF000  lifecycle
	return uint64(atomic.LoadUint32(&ins.status)) +
		(uint64(atomic.LoadUint32(&ins.awakeness)) << 4) +
		(backing << 8) +
		(uint64(atomic.LoadUint32(&ins.phase)) << 12)
}

func (ins *Instance) Description() string {
	return ins.StatusDescription()
}

func (ins *Instance) StatusDescription() string {
	switch atomic.LoadUint32(&ins.status) {
	case INSTANCE_UNSTARTED:
		return DESCRIPTION_UNSTARTED
	case INSTANCE_CLOSED:
		return DESCRIPTION_CLOSED
	}

	switch atomic.LoadUint32(&ins.awakeness) {
	case INSTANCE_SLEEPING:
		return DESCRIPTION_SLEEPING
	case INSTANCE_ACTIVATING:
		return DESCRIPTION_ACTIVATING
	case INSTANCE_ACTIVE:
		return DESCRIPTION_ACTIVE
	case INSTANCE_MAYBE:
		return DESCRIPTION_MAYBE
	default:
		return DESCRIPTION_UNDEFINED
	}
}

func (ins *Instance) AssignBackups(numBak int, candidates []*Instance) {
	ins.backups.Reset(numBak, candidates)
}

func (ins *Instance) Dispatch(cmd types.Command) error {
	return ins.DispatchWithOptions(cmd, false)
}

func (ins *Instance) DispatchWithOptions(cmd types.Command, errorOnBusy bool) error {
	if ins.IsClosed() {
		return ErrInstanceClosed
	}

	select {
	case ins.chanCmd <- cmd:
		// continue after select
	default:
		if errorOnBusy {
			return ErrInstanceBusy
		}

		// wait to be inserted and continue after select
		ins.chanCmd <- cmd
	}

	// This check is thread safe for if it is not closed now, HandleRequests() will do the cleaning up.
	if ins.IsClosed() {
		ins.cleanCmdChannel(ins.chanCmd)
	}

	// Once the cmd is sent to chanCmd, HandleRequests() or cleanCmdChannel() will handle the possible error.
	return nil
}

func (ins *Instance) IsBusy() bool {
	return len(ins.chanCmd) > 0
}

func (ins *Instance) WarmUp() {
	ins.validate(&ValidateOption{WarmUp: true})
	// Force reset
	ins.flagWarmed()
}

func (ins *Instance) Validate(opts ...*ValidateOption) (*Connection, error) {
	var opt *ValidateOption
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt == nil {
		opt = &ValidateOption{}
	}
	return ins.validate(opt)
}

// Handle incoming client requests
// lambda facing goroutine
func (ins *Instance) HandleRequests() {
	for {
		select {
		case <-ins.closed:
			// Handle rest commands in channels
			ins.cleanCmdChannel(ins.chanPriorCmd)
			ins.cleanCmdChannel(ins.chanCmd)
			return
		case cmd := <-ins.chanPriorCmd: // Priority queue get
			ins.handleRequest(cmd)
		case cmd := <-ins.chanCmd: /*blocking on lambda facing channel*/
			// Drain priority channel first.
			// The implementation is not thread safe. As long as this is the only place to read chanPriorCmd, it is ok.
			for len(ins.chanPriorCmd) > 0 {
				ins.handleRequest(<-ins.chanPriorCmd)
			}
			ins.handleRequest(cmd)
		case <-ins.coolTimer.C:
			// Warmup will not work until first call.
			// Double check, for it could timeout before a previous request got handled.
			if ins.IsReclaimed() || len(ins.chanPriorCmd) > 0 || len(ins.chanCmd) > 0 || atomic.LoadUint32(&ins.status) == INSTANCE_UNSTARTED {
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

	// Reserve available backups
	changes := ins.backups.Reserve(nil)

	// Prepare log printer for debugging
	logFunc := logger.NewFunc(func() string {
		var msg strings.Builder
		for i := 0; i < ins.backups.Len(); i++ {
			msg.WriteString(" ")
			backup, ok := ins.backups.StartByIndex(i, ins)
			if ok {
				msg.WriteString(strconv.FormatUint(backup.Id(), 10))
			} else {
				msg.WriteString("N/A")
			}
		}
		return msg.String()
	})

	// Start backups.
	available := ins.backups.Availables()
	atomic.StoreUint32(&ins.recovering, uint32(available))
	if available > ins.backups.Len()/2 {
		ins.log.Debug("Parallel recovery started with %d backup instances:%v, changes: %d", available, logFunc, changes)
	} else if available == 0 {
		ins.log.Warn("Unable to start parallel recovery due to no backup instance available")
	} else {
		ins.log.Warn("Parallel recovery started with insufficient %d backup instances:%v, changes: %d", available, logFunc, changes)
	}

	return available
}

// Resume serving
func (ins *Instance) ResumeServing() {
	ins.mu.Lock()
	atomic.StoreUint32(&ins.recovering, 0)
	ins.backups.Stop(ins)
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
	if ins.IsClosed() {
		return false
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	// We don't check phase because backing and closed due to reclaiming/expiring are exclusive.
	// If instance is backing, reclaiming and expiring will not close instance.
	// If instance is closed due to reclaiming/expiring, it is not backing.
	if !ins.IsClosed() && !ins.IsRecovering() && atomic.CompareAndSwapUint32(&ins.backing, BACKING_DISABLED, BACKING_RESERVED) {
		return true
	}

	return false
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
			Cmd:     protocol.CMD_PING,
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
	defer ins.mu.Unlock()

	atomic.StoreUint32(&ins.backing, BACKING_DISABLED)

	// Including expired and reclaimed
	if ins.IsReclaimed() {
		ins.closeLocked()
	}
}

func (ins *Instance) IsBacking(includingPrepare bool) bool {
	if includingPrepare {
		return atomic.LoadUint32(&ins.backing) != BACKING_DISABLED
	} else {
		return atomic.LoadUint32(&ins.backing) == BACKING_ENABLED
	}
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
	ins.Dispatch(&types.Control{
		Cmd:        "migrate",
		Addr:       addr,
		Deployment: dply.Name(),
		Id:         dply.Id(),
	})
	return nil
}

// TODO: if instance in reclaimed | no backing state -> no warmup perform

func (ins *Instance) Degrade() {
	if atomic.CompareAndSwapUint32(&ins.phase, PHASE_ACTIVE, PHASE_BACKING_ONLY) {
		ins.coolTimeout = config.InstanceDegradeWarmTimeout
	}
}

func (ins *Instance) Expire() {
	if !atomic.CompareAndSwapUint32(&ins.phase, PHASE_BACKING_ONLY, PHASE_EXPIRED) {
		return
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	if !ins.IsBacking(true) {
		ins.closeLocked()
	}
}

func (ins *Instance) Phase() uint32 {
	return atomic.LoadUint32(&ins.phase)
}

func (ins *Instance) IsReclaimed() bool {
	return atomic.LoadUint32(&ins.phase) >= PHASE_RECLAIMED
}

func (ins *Instance) MatchDataLink(ctrl *Connection) {
	if ins.dataLink != nil && ctrl.AddDataLink(ins.dataLink) {
		ins.dataLink = nil
	}
}

func (ins *Instance) CacheDataLink(link *Connection) {
	ins.dataLink = link
}

func (ins *Instance) Close() {
	if ins.IsClosed() {
		return
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	ins.closeLocked()
}

func (ins *Instance) closeLocked() {
	if ins.IsClosed() {
		return
	}

	ins.log.Debug("[%v]Closing...", ins)
	select {
	case <-ins.closed:
		return
	default:
		close(ins.closed)
	}
	atomic.StoreUint32(&ins.status, INSTANCE_CLOSED)
	if !ins.coolTimer.Stop() {
		// For parallel access, use select.
		select {
		case <-ins.coolTimer.C:
		default:
		}
	}
	// Close all links
	// TODO: Due to reconnection from lambda side, we may just leave links to be closed by the lambda.
	if ins.ctrlLink != nil {
		ins.ctrlLink.Close()
		ins.ctrlLink = nil
	}
	if ins.dataLink != nil {
		ins.dataLink.Close()
		ins.dataLink = nil
	}
	atomic.StoreUint32(&ins.awakeness, INSTANCE_SLEEPING)
	ins.flagValidatedLocked(nil, ErrInstanceClosed)
	ins.log.Info("[%v]", ins)

	// Recycle instance
	CM.Recycle(ins)
}

// Support concurrent cleaning up.
func (ins *Instance) cleanCmdChannel(ch chan types.Command) {
	for {
		select {
		case cmd := <-ch:
			ins.handleRequest(cmd)
		default:
			return
		}
	}
}

func (ins *Instance) IsClosed() bool {
	return atomic.LoadUint32(&ins.status) == INSTANCE_CLOSED
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

func castValidatedConnection(validated promise.Promise) (*Connection, error) {
	cn, err := validated.Result()
	if cn == nil {
		return nil, err
	} else {
		return cn.(*Connection), err
	}
}

func (ins *Instance) validate(opt *ValidateOption) (*Connection, error) {
	ins.mu.Lock()

	// Closed safe: The only closed check that in the mutex.
	// See comments started with "Closed safe: " below for more detail.
	if ins.IsClosed() {
		ins.mu.Unlock()
		return nil, ErrInstanceClosed
	}

	lastResolved := ins.validated.ResolvedAt()
	// 1. resolved
	// 2. last validation succeeded and time since lastResolved >= ValidationTimeout
	// 3. or something wrong on last validating (must be ignorable)
	if (lastResolved != time.Time{} && (ins.validated.Error() != nil || time.Since(lastResolved) >= ValidationTimeout)) {
		// For reclaimed instance, simply return the result of last validation.
		// if ins.IsReclaimed() {
		// 	return castValidatedConnection(ins.validated)
		// }

		// Not validating. Validate...
		ins.validated.ResetWithOptions(opt)
		ins.mu.Unlock()

		connectTimeout := DefaultConnectTimeout
		// for::attemps
		for {
			ins.log.Debug("Validating...")
			// Try invoking the lambda node.
			// Closed safe: It is ok to invoke lambda, closed status will be checked in TryFlagValidated on processing the PONG.
			triggered := ins.tryTriggerLambda(ins.validated.Options().(*ValidateOption))
			if triggered {
				// Waiting to be validated
				return castValidatedConnection(ins.validated)
			} else if opt.WarmUp && !global.IsWarmupWithFixedInterval() {
				// Instance is warm, skip unnecssary warming up.
				return ins.flagValidatedLocked(ins.ctrlLink) // Skip any check in the "FlagValidated".
			}

			// If instance is active, PING is issued to ensure active
			// Closed safe: On closing, ctrlLink will be nil. By keep a local copy and check it is not nil, it is safe to make PING request.
			//   Like invoking, closed status will be checked in TryFlagValidated on processing the PONG.
			ctrl := ins.ctrlLink
			if ctrl != nil {
				if opt.Command != nil && opt.Command.String() == protocol.CMD_PING {
					ins.log.Debug("Ping with payload")
					ctrl.Ping(opt.Command.(*types.Control).Payload)
				} else {
					ctrl.Ping(DefaultPingPayload)
				}
			} // Ctrl can be nil if disconnected, simply wait for timeout and retry

			// Start timeout, ping may get stucked anytime.
			// TODO: In this version, no switching and unmanaged instance is handled. So ping will re-ping forever until being activated or proved to be sleeping.
			ins.validated.SetTimeout(connectTimeout)

			// Wait for timeout or validation get concluded
			// Closed safe: On closing, validation will be concluded.
			if err := ins.validated.Timeout(); err == promise.ErrTimeout {
				// Exponential backoff
				connectTimeout *= time.Duration(BackoffFactor)
				if connectTimeout > MaxConnectTimeout {
					connectTimeout = MaxConnectTimeout
				}
				ins.log.Warn("Timeout on validating, re-ping...")
			} else {
				// Validated.
				return castValidatedConnection(ins.validated)
			}
		} // End of for::attemps
	} else {
		// Validating... Wait.
		ins.mu.Unlock()
		return castValidatedConnection(ins.validated)
	}
}

func (ins *Instance) tryTriggerLambda(opt *ValidateOption) bool {
	ins.log.Debug("[%v]Try activate lambda.", ins)
	switch atomic.LoadUint32(&ins.awakeness) {
	case INSTANCE_ACTIVATING:
		// Cases:
		// On validating, lambda deactivated and reactivating. Then validating timeouts and retries.
		return true
	case INSTANCE_SLEEPING:
		if !atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_SLEEPING, INSTANCE_ACTIVATING) {
			return false
		} // Continue to activate
	default:
		return false
	}

	if opt.WarmUp {
		ins.log.Info("[%v]Warming up...", ins)
	} else {
		ins.log.Info("[%v]Activating...", ins)
	}
	go ins.triggerLambda(opt)

	return true
}

func (ins *Instance) triggerLambda(opt *ValidateOption) {
	err := ins.triggerLambdaLocked(opt)
	for {
		if atomic.LoadUint32(&ins.awakeness) == INSTANCE_MAYBE {
			return
		}

		if err != nil && err == ErrInstanceReclaimed {
			atomic.StoreUint32(&ins.phase, PHASE_RECLAIMED)
			ins.log.Info("Reclaimed")

			// We can close the instance if it is not backing any instance.
			if !ins.IsBacking(true) {
				ins.Close()
				return
			}

			// Continue to waiting for being validated
		}

		if ins.validated.IsResolved() {
			// Don't overwrite the MAYBE status.
			if atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_ACTIVE, INSTANCE_SLEEPING) {
				ins.log.Debug("[%v]Status updated.", ins)
			} else {
				ins.log.Error("[%v]Unexpected status.", ins)
			}
			return
		} else if ins.validated.Options().(*ValidateOption).WarmUp { // Use ValidationOption of most recent validation to reflect up to date status.
			// No retry for warming up.
			// Possible reasons
			// 1. Pong is delayed and lambda is returned without requesting for recovery, or the lambda will wait for the ending of the recovery.
			ins.mu.Lock()
			// Does not affect the MAYBE status.
			atomic.StoreUint32(&ins.awakeness, INSTANCE_SLEEPING)
			// No need to return a validated connection. If someone do require the connection, it is an unexpected error.
			ins.flagValidatedLocked(nil, ErrUnexpectedReturn)
			ins.mu.Unlock()

			ins.log.Debug("[%v]Detected unvalidated warmup, ignored.", ins)
			return
		} else {
			// Validating, retrigger.
			atomic.StoreUint32(&ins.awakeness, INSTANCE_ACTIVATING)

			ins.log.Info("[%v]Reactivateing...", ins)
			err = ins.triggerLambdaLocked(opt)
		}
	}
}

func (ins *Instance) triggerLambdaLocked(opt *ValidateOption) error {
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
	if !ins.IsBacking(false) {
		// Main store only
		status = protocol.Status{*ins.Meta.ToProtocolMeta(ins.Id())}
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
	var localFlags uint64
	if atomic.LoadUint32(&ins.phase) != PHASE_ACTIVE {
		localFlags |= protocol.FLAG_BACKING_ONLY
	}
	event := &protocol.InputEvent{
		Sid:     ins.initSession(),
		Cmd:     protocol.CMD_PING,
		Id:      ins.Id(),
		Proxy:   fmt.Sprintf("%s:%d", global.ServerIp, global.BasePort+1),
		Prefix:  global.Options.Prefix,
		Log:     global.Log.GetLevel(),
		Flags:   global.Flags | localFlags,
		Backups: config.BackupsPerInstance,
		Status:  status,
	}
	// CMD_PING is used for preflight requests. CMD_WARMUP is used for keeping node warmed, which involves no further action.
	// While requests do not use CMD_PING, CMD_PING request is used to piggy-back messages in the cases like starting backing.
	// In such case, no further action is required. So we use CMD_WARMUP to invoke node when requests command is CMD_PING.
	if opt.WarmUp || opt.Command.String() == protocol.CMD_PING {
		event.Cmd = protocol.CMD_WARMUP
		opt.WarmUp = true
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

	// Don't reset links here, fronzen but not dead.

	if err != nil {
		ins.log.Error("[%v]Error on activating lambda store: %v", ins, err)
	} else {
		ins.log.Debug("[%v]Lambda instance deactivated.", ins)
	}
	if output != nil && len(output.Payload) > 0 {
		var outputStatus protocol.Status
		var outputError protocol.OutputError
		if err := json.Unmarshal(output.Payload, &outputError); err == nil {
			ins.log.Error("[Lambda deactivated with error]: %v", outputError)
		} else if err := json.Unmarshal(output.Payload, &outputStatus); err != nil {
			ins.log.Error("Failed to unmarshal payload of lambda output: %v, payload", err, string(output.Payload))
		} else if len(outputStatus) > 0 {
			uptodate, err := ins.Meta.FromProtocolMeta(&outputStatus[0]) // Ignore backing store
			if err != nil && err == ErrInstanceReclaimed && ins.Phase() == PHASE_BACKING_ONLY {
				// Reclaimed
				ins.log.Debug("Detected instance reclaimed from lineage: %v", &outputStatus)
				return err
			} else if uptodate {
				// If the node was invoked by other than the proxy, it could be stale.
				if atomic.LoadUint32(&ins.awakeness) == INSTANCE_MAYBE {
					uptodate = false
				} else {
					ins.Meta.Stale = false
				}
			}

			// Show log
			if uptodate {
				ins.log.Debug("Got updated instance lineage: %v", &outputStatus)
			} else {
				ins.log.Debug("Got staled instance lineage: %v", &outputStatus)
			}
		}
	} else if event.IsPersistencyEnabled() {
		ins.log.Error("No instance lineage returned, output: %v", output)
	}
	return nil
}

// Flag the instance as validated by specified connection.
// This also validate the connection belonging to the instance by setting instance field of the connection.
func (ins *Instance) TryFlagValidated(conn *Connection, sid string, flags int64) (*Connection, error) {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.IsClosed() {
		// Validation is done by Close(), so we simply return here.
		return conn, ErrInstanceClosed
	}
	// Identified unhandled cases:
	// 1. Lambda is sleeping
	// Explain: Pong is likely delayed due quick exection of the lambda, possibly for "warmup" requests only.
	// Solution: Continue to save connection with no further changes to the "awakeness" and the "validating" status.
	//           Noted this function will not update status of sleeping lambda.

	// Acknowledge data links
	if !conn.control {
		// Datalinks can come earlier than ctrl link.
		// Keeping using the newly connectioned datalinks, and we are safe, or just wait it timeout and try to get a new one.
		conn.BindInstance(ins)
		// Connect to corresponding ctrl link
		if ins.ctrlLink == nil || !ins.ctrlLink.AddDataLink(conn) {
			ins.CacheDataLink(conn)
		}
		return conn, ErrNotCtrlLink
	}

	ins.log.Debug("[%v]Confirming validation...", ins)
	ins.flagWarmed()
	if ins.ctrlLink != conn {
		// Check possible duplicated session
		newSession := ins.startSession(sid)

		// If session is duplicated and no anomaly in flags, it is safe to switch regardless potential lambda change.
		if !newSession && flags != protocol.PONG_FOR_CTRL {
			// Deny session
			return conn, ErrDuplicatedSession
		} else if newSession {
			ins.log.Debug("Session %s started.", sid)
		} else {
			ins.log.Debug("Session %s resumed.", sid)
		}

		oldConn := ins.ctrlLink

		// Set instance, order matters here.
		conn.BindInstance(ins)
		ins.ctrlLink = conn

		if oldConn != nil {
			// Inherit the data link so it will not be closed because of ctrl link reconnected.
			dl := oldConn.GetDataLink()
			if dl != nil && conn.AddDataLink(dl) {
				// Worker matches and added
				oldConn.RemoveDataLink(dl)
			}
			oldConn.Close()
		}

		// There are three cases for link switch:
		// 1. Old node get reclaimed
		// 2. Reconnection
		atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_ACTIVATING, INSTANCE_ACTIVE)
		// 3. Migration, which we have no way to know its status. (Forget it now)
		// TODO: Fix migration handling later.
		// atomic.StoreUint32(&ins.awakeness, INSTANCE_MAYBE)
		ins.log.Debug("[%v]Control link updated.", ins)
	} else {
		// New session for saved connection, simply call startSession and update status.
		ins.startSession(sid)
		atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_ACTIVATING, INSTANCE_ACTIVE)
	}
	// Connect to possible data link.
	ins.MatchDataLink(conn)

	// These two flags are exclusive because backing only mode will enable reclaimation claim and disable fast recovery.
	if flags&protocol.PONG_RECOVERY > 0 {
		ins.log.Debug("Parallel recovery requested.")
		ins.startRecoveryLocked()
	} else if flags&protocol.PONG_RECLAIMED > 0 {
		// PONG_RECLAIMED will be issued for instances in PHASE_BACKING_ONLY or PHASE_EXPIRED.
		atomic.StoreUint32(&ins.phase, PHASE_RECLAIMED)
		// We can close the instance if it is not backing any instance.
		if !ins.IsBacking(true) {
			ins.log.Info("Reclaimed")
			ins.closeLocked()
			return conn, nil
		} else {
			ins.log.Info("Reclaimed, keep running because the instance is backing another instance.")
		}
	}

	validConn, _ := ins.flagValidatedLocked(conn)
	if validConn != conn {
		ins.log.Debug("[%v]Already validated", ins)
		return conn, ErrInstanceValidated
	} else {
		return conn, nil
	}
}

func (ins *Instance) flagValidatedLocked(conn *Connection, errs ...error) (*Connection, error) {
	var err error
	if len(errs) > 0 {
		err = errs[0]
	}
	if _, err := ins.validated.Resolve(conn, err); err == nil {
		ins.log.Debug("[%v]Validated", ins)
	}
	return castValidatedConnection(ins.validated)
}

func (ins *Instance) bye(conn *Connection) {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.ctrlLink != conn {
		// We don't care any rogue bye
		return
	}

	if atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_MAYBE, INSTANCE_SLEEPING) {
		ins.log.Info("[%v]Bye from unmanaged instance, flag as sleeping.", ins)
	} else {
		ins.log.Debug("[%v]Bye ignored, waiting for return of synchronous invocation.", ins)
	}
}

// FlagClosed Notify instance that a connection is closed.
func (ins *Instance) FlagClosed(conn *Connection) {
	// We care control link only.
	if ins.ctrlLink != conn {
		return
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	// Double check in the mutex block.
	if ins.ctrlLink != conn {
		return
	}

	// For now, all we can do is setting control link to nil to avoid it being used.
	// If node is active, it will be reconnected later.
	ins.ctrlLink = nil
}

func (ins *Instance) handleRequest(cmd types.Command) {
	// On parallel recovering, we will try reroute get requests.
	if cmd.String() == protocol.CMD_GET && ins.IsRecovering() && ins.rerouteGetRequest(cmd.GetRequest()) {
		return
	}

	var err error
	var attemps = MAX_ATTEMPTS
	if !cmd.Retriable() {
		attemps = 1
	}

	var i int
	for i = 0; i < attemps; i++ {
		if i > 0 {
			ins.log.Debug("Attempt %d", i)
		}
		// Check lambda status first
		validateStart := time.Now()
		// Once active connection is confirmed, keep awake on serving.
		ctrlLink, err := ins.Validate(&ValidateOption{Command: cmd})
		validateDuration := time.Since(validateStart)

		// Only after validated, we know whether the instance is reclaimed.
		// If instance is expiring and reclaimed, we will relocate objects in main repository while backing objects are not affected.
		// Two options available to handle reclaiming event:
		// 1. Recover to prevail node and reroute to the node. (current implementation)
		// 2. Report not found
		if cmd.String() == protocol.CMD_GET && ins.IsReclaimed() && ins.relocateGetRequest(cmd.GetRequest()) {
			return
		} else if err != nil {
			// Handle errors
			if req, ok := cmd.(*types.Request); ok {
				req.SetResponse(err)
			}
			return
		} else if ctrlLink == nil {
			// Unexpected error
			if req, ok := cmd.(*types.Request); ok {
				req.SetResponse(ErrUnknown)
			}
			ins.log.Warn("Unexpected nil control link with no error returned.")
			return
		}

		// Make request
		err = ins.request(ctrlLink, cmd, validateDuration)
		if err == nil || !cmd.Retriable() { // Request can become streaming, so we need to test again everytime.
			break
		}
	}
	if err != nil {
		if attemps > 1 && i == attemps {
			ins.log.Error("Max retry reaches, give up: %v", err)
		} else if cmd.Retriable() {
			ins.log.Error("Abandon retrial: %v", err)
		} else {
			ins.log.Error("Can not retry a streaming request, give up: %v", err)
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
		ins.log.Debug("Detected reroute override for key %s", req.Key)
		return false
	}

	// Thread safe
	backup, ok := ins.backups.GetByKey(req.Key)
	if !ok {
		// Backup is not available, can be recovered or simply not available.
		return false
	}

	backup.chanPriorCmd <- req // Rerouted request should not be queued again.
	ins.log.Debug("Rerouted %s to node %d as backup %d of %d.", req.Key, backup.Id(), backup.backingId, backup.backingTotal)
	return true
}

func (ins *Instance) relocateGetRequest(req *types.Request) bool {
	// Backing keys will not be relocated.
	// If the instance is expiring or reclaimed, main repository only is affected.
	if req.InsId != ins.Id() {
		return false
	}

	target, err := CM.Relocate(req.Info, req.Id.Chunk(), req.ToRecover())
	if err != nil {
		ins.log.Warn("Instance reclaimed, tried relocated %v but failed: %v", req.Key, err)
		return false
	}

	ins.log.Debug("Instance reclaimed, relocated %v to %d", req.Key, target.Id())
	return true
}

func (ins *Instance) request(ctrlLink *Connection, cmd types.Command, validateDuration time.Duration) error {
	cmdName := strings.ToLower(cmd.String())
	switch req := cmd.(type) {
	case *types.Request:
		collector.CollectRequest(collector.LogValidate, req.CollectorEntry.(*collector.DataEntry), cmdName, req.Id.ReqId, req.Id.ChunkId, int64(validateDuration))

		// Select link
		link := ctrlLink
		if req.Size() > MaxControlRequestSize {
			link = ctrlLink.GetDataLink()
			// Fallback to ctrlLink if dataLink is not ready.
			if link == nil {
				link = ctrlLink
			}
		}

		switch cmdName {
		case protocol.CMD_SET: /*set or two argument cmd*/
			req.PrepareForSet(link)
			// If parallel recovery is triggered, record keys set during recovery.
			if ins.IsRecovering() {
				ins.log.Debug("Override rerouting for key %s due to set", req.Key)
				ins.writtens.Set(req.Key, &struct{}{})
			}
		case protocol.CMD_GET: /*get or one argument cmd*/
			// If parallel recovery is triggered, there is no need to forward the serving key.
			req.PrepareForGet(link)
		case protocol.CMD_DEL:
			req.PrepareForDel(link)
			if ins.IsRecovering() {
				ins.log.Debug("Override rerouting for key %s due to del", req.Key)
				ins.writtens.Set(req.Key, &struct{}{})
			}
		case protocol.CMD_RECOVER:
			req.PrepareForRecover(link)
		default:
			req.SetResponse(fmt.Errorf("unexpected request command: %s", cmd))
			// Unrecoverable
			return nil
		}

		// In case there is a request already, wait to be consumed (for response).
		link.chanWait <- req
		if err := req.Flush(RequestTimeout); err != nil {
			ins.log.Warn("Flush request error: %v", err)
			// Remove request.
			select {
			case <-link.chanWait:
			default:
			}
			// link failed, close so it can be reconnected.
			if link != ctrlLink {
				ctrlLink.RemoveDataLink(link)
			}
			link.Close()
			return err
		}

	case *types.Control:
		isDataRequest := false

		switch cmdName {
		case protocol.CMD_PING:
			// Simply ignore.
			return nil
		case protocol.CMD_DATA:
			req.PrepareForData(ctrlLink)
			isDataRequest = true
		case protocol.CMD_MIGRATE:
			req.PrepareForMigrate(ctrlLink)
		case protocol.CMD_DEL:
			req.PrepareForDel(ctrlLink)
			if ins.IsRecovering() {
				ins.log.Debug("Override rerouting for key %s due to del", req.Request.Key)
				ins.writtens.Set(req.Request.Key, &struct{}{})
			}
		case protocol.CMD_RECOVER:
			req.PrepareForRecover(ctrlLink)
		default:
			ins.log.Error("Unexpected control command: %s", cmd)
			// Unrecoverable
			return nil
		}

		if err := req.Flush(RequestTimeout); err != nil {
			ins.log.Error("Flush control error: %v", err)
			if isDataRequest {
				global.DataCollected.Done()
			}
			ctrlLink.Close()
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

func (ins *Instance) warmUp() {
	ins.validate(&ValidateOption{WarmUp: true})
	// Force reset
	ins.resetCoolTimer()
}

func (ins *Instance) flagWarmed() {
	// Only switch if instance unstarted.
	atomic.CompareAndSwapUint32(&ins.status, INSTANCE_UNSTARTED, INSTANCE_RUNNING)
	if global.IsWarmupWithFixedInterval() || ins.IsReclaimed() {
		return
	}

	ins.resetCoolTimer()
}

func (ins *Instance) resetCoolTimer() {
	if !ins.coolTimer.Stop() {
		// For parallel access, use select.
		select {
		case <-ins.coolTimer.C:
		default:
		}
	}
	if !ins.IsClosed() && !ins.IsReclaimed() {
		ins.coolTimer.Reset(ins.coolTimeout)
	}
}

func (ins *Instance) CollectData() {
	if atomic.LoadUint32(&ins.status) == INSTANCE_UNSTARTED {
		return
	}

	global.DataCollected.Add(1)
	ins.Dispatch(&types.Control{Cmd: "data"})
}

func (ins *Instance) FlagDataCollected(ok string) {
	if atomic.LoadUint32(&ins.status) == INSTANCE_UNSTARTED {
		return
	}

	ins.log.Debug("Data collected: %s", ok)
	global.DataCollected.Done()
}
