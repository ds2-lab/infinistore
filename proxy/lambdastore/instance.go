package lambdastore

import (
	"context"
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
	"github.com/mason-leap-lab/go-utils"
	"github.com/mason-leap-lab/go-utils/mapreduce"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/infinicache/common/util/hashmap"
	"github.com/mason-leap-lab/infinicache/common/util/promise"
	"github.com/mason-leap-lab/infinicache/lambda/invoker"
	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	INSTANCE_MASK_STATUS_START      = 0x0000000F
	INSTANCE_MASK_STATUS_CONNECTION = 0x000000F0
	INSTANCE_MASK_STATUS_BACKING    = 0x00000F00
	INSTANCE_MASK_STATUS_LIFECYCLE  = 0x0000F000
	INSTANCE_MASK_STATUS_FAILURE    = 0xF0000000

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

	// Abnormal status
	FAILURE_MAX_QUEUE_REACHED = 1

	MAX_CMD_QUEUE_LEN = 5
	TEMP_MAP_SIZE     = 10
	BACKING_DISABLED  = 0
	BACKING_RESERVED  = 1
	BACKING_ENABLED   = 2
	BACKING_FORBID    = 3

	DESCRIPTION_UNSTARTED  = "unstarted"
	DESCRIPTION_CLOSED     = "closed"
	DESCRIPTION_SLEEPING   = "sleeping"
	DESCRIPTION_ACTIVATING = "activating"
	DESCRIPTION_ACTIVE     = "active"
	DESCRIPTION_MAYBE      = "unmanaged"
	DESCRIPTION_UNDEFINED  = "undefined"

	DISPATCH_OPT_BUSY_CHECK = 0x0001
	DISPATCH_OPT_RELOCATED  = 0x0002
)

var (
	CM                    ClusterManager
	WarmTimeout           = config.InstanceWarmTimeout
	TriggerTimeout        = 1 * time.Second // Triggering cost is about 20ms, set large enough to avoid exceeded timeout
	RTT                   = 2 * time.Millisecond
	DefaultConnectTimeout = 20 * time.Millisecond // Decide by RTT.
	MaxConnectTimeout     = 1 * time.Second
	MinValidationInterval = 10 * time.Millisecond // MinValidationInterval The minimum interval between validations.
	MaxValidationFailure  = 3
	BackoffFactor         = 2
	MaxControlRequestSize = int64(200000) // 200KB, which can be transmitted in 20ms.
	DefaultPingPayload    = []byte{}
	AwsSession            = awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
		SharedConfigState: awsSession.SharedConfigEnable,
	}))

	// Errors
	ErrInstanceClosed     = errors.New("instance closed")
	ErrInstanceReclaimed  = errors.New("instance reclaimed")
	ErrInstanceSleeping   = errors.New("instance is sleeping")
	ErrInstanceRecovering = errors.New("instance is recovering")
	ErrReservationFailed  = errors.New("reservation failed")
	ErrDuplicatedSession  = errors.New("session has started")
	ErrNotCtrlLink        = errors.New("not control link")
	ErrInstanceValidated  = errors.New("instance has been validated by another connection")
	ErrInstanceBusy       = errors.New("instance busy")
	ErrWarmupReturn       = errors.New("return from warmup")
	ErrUnknown            = errors.New("unknown error")
	ErrValidationTimeout  = &LambdaError{error: errors.New("funciton validation timeout"), typ: LambdaErrorTimeout}
	ErrCapacityExceeded   = errors.New("capacity exceeded")
	ErrQueueTimeout       = &LambdaError{error: errors.New("queue timeout"), typ: LambdaErrorTimeout}
	ErrRelocationFailed   = errors.New("relocation failed")
)

type InstanceManager interface {
	Instance(uint64) *Instance
	Recycle(types.LambdaDeployment) error
	GetBackupCandidates() mapreduce.Iterator
	GetDelegates() []*Instance
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

	chanCmd         chan types.Command
	chanPriorCmd    chan types.Command // Channel for priority commands: control and forwarded backing requests.
	status          uint32             // Status of proxy side instance which can be one of unstarted, running, and closed.
	awakeness       uint32             // Status of lambda node which can be one of sleeping, activating, active, and maybe.
	phase           uint32             // Status of serving mode which can be one of active, backing only, reclaimed, and expired.
	validated       promise.Promise
	numOutbound     int32
	numInbound      int32
	mu              sync.Mutex
	closed          chan struct{}
	coolTimer       *time.Timer
	coolTimeout     time.Duration
	coolReset       chan struct{}
	numFailure      uint32 // # of continues validation failure, which means to node may stop resonding.
	client          invoker.FunctionInvoker
	lambdaCanceller context.CancelFunc

	// Connection management
	lm                *LinkManager
	sessions          hashmap.HashMap
	due               int64
	reconcilingWorker int32 // The worker id of reconciling function.

	// Backup fields
	backups          Backups
	recovering       uint32          // # of backups in use, also if the recovering > 0, the instance is recovering.
	writtens         hashmap.HashMap // Whitelist, write opertions will be added to it during parallel recovery.
	backing          uint32          // backing status, 0 for non backing, 1 for reserved, 2 for backing.
	backingIns       *Instance
	backingId        int // Identifier for backup, ranging from [0, # of backups)
	backingTotal     int // Total # of backups ready for backing instance.
	rerouteThreshold uint64
	// TODO Lineage changed

	// Delegate fields
	delegates *Backups
}

func NewInstanceFromDeployment(dp *Deployment, id uint64) *Instance {
	dp.id = id
	dp.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("%s-%d ", dp.name, dp.id),
		Level:  global.Log.GetLevel(),
		Color:  !global.Options.NoColor,
	}

	ins := &Instance{
		Deployment: dp,
		Meta: Meta{
			Term: 1,
		}, // Term start with 1 to avoid uninitialized term ambigulous.
		awakeness:    INSTANCE_SLEEPING,
		chanCmd:      make(chan types.Command, MAX_CMD_QUEUE_LEN),
		chanPriorCmd: make(chan types.Command, 1),
		validated:    promise.Resolved(), // Initialize with a resolved promise.
		closed:       make(chan struct{}),
		coolTimer:    time.NewTimer(time.Duration(rand.Int63n(int64(WarmTimeout)) + int64(WarmTimeout)/2)), // Differentiate the time to start warming up.
		coolTimeout:  WarmTimeout,
		coolReset:    make(chan struct{}, 1),
		sessions:     hashmap.NewMap(TEMP_MAP_SIZE),
		writtens:     hashmap.NewMap(TEMP_MAP_SIZE),
	}
	ins.Meta.ResetCapacity(global.Options.GetInstanceCapacity(), 0)
	ins.backups.instance = ins
	ins.backups.log = ins.log
	ins.lm = NewLinkManager(ins)
	ins.lm.SetMaxActiveDataLinks(MAX_CMD_QUEUE_LEN)
	return ins
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

// InstantStats implementation
func (ins *Instance) Status() uint64 {
	// 0x000F  status
	// 0x00F0  connection
	// 0x0F00  backing
	var backing uint64
	var failure uint64
	if ins.IsRecovering() {
		backing += INSTANCE_RECOVERING
	}
	if ins.IsBacking(true) {
		backing += INSTANCE_BACKING
	}
	if len(ins.chanCmd) == MAX_CMD_QUEUE_LEN {
		failure += FAILURE_MAX_QUEUE_REACHED
	}
	// 0xF000  lifecycle
	return uint64(atomic.LoadUint32(&ins.status)) +
		(uint64(atomic.LoadUint32(&ins.awakeness)) << 4) +
		(backing << 8) +
		(uint64(atomic.LoadUint32(&ins.phase)) << 12) +
		(failure << 28)
}

func (ins *Instance) Occupancy(mode types.InstanceOccupancyMode) float64 {
	switch mode {
	case types.InstanceOccupancyMain:
		return ins.Meta.ModifiedOccupancy(0)
	case types.InstanceOccupancyModified:
		return float64(ins.Meta.sizeModified) / float64(ins.Meta.EffectiveCapacity())
	case types.InstanceOccupancyMax:
		return float64(ins.Meta.mem) / float64(ins.Meta.Capacity)
	default:
		return 0.0
	}
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
	ins.backups.ResetCandidates(numBak, candidates)
}

func (ins *Instance) Dispatch(cmd types.Command) error {
	return ins.DispatchWithOptions(cmd, 0)
}

func (ins *Instance) DispatchWithOptions(cmd types.Command, opts int) error {
	if ins.IsClosed() {
		// tryRerouteDelegateRequest will always try relocation.
		if !ins.tryRerouteDelegateRequest(cmd.GetRequest(), opts) {
			return ErrRelocationFailed
		} else {
			return nil
		}
	}

	ins.log.Debug("Dispatching %v, %d queued", cmd, len(ins.chanCmd))
	select {
	case ins.chanCmd <- cmd:
		// continue after select
	default:
		if opts&DISPATCH_OPT_BUSY_CHECK > 0 {
			return ErrInstanceBusy
		}

		// wait to be inserted and continue after select
		select {
		case ins.chanCmd <- cmd:
		case <-time.After(protocol.GetHeaderTimeout()):
			return ErrQueueTimeout
		}
	}

	// This check is thread safe for if it is not closed now, HandleRequests() will do the cleaning up.
	if ins.IsClosed() {
		ins.cleanCmdChannel(ins.chanCmd)
	}

	// Once the cmd is sent to chanCmd, HandleRequests() or cleanCmdChannel() will handle the possible error.
	return nil
}

func (ins *Instance) IsBusy(cmd types.Command) bool {
	if len(ins.chanCmd) >= MAX_CMD_QUEUE_LEN {
		return true
	} else if cmd.String() == protocol.CMD_SET {
		return atomic.LoadInt32(&ins.numOutbound) > 0
	} else {
		return false
	}
}

func (ins *Instance) WarmUp() {
	ins.validate(&ValidateOption{WarmUp: true})
	// Force reset
	ins.flagWarmed()
}

func (ins *Instance) IsActive() bool {
	return atomic.LoadUint32(&ins.awakeness) == INSTANCE_ACTIVE
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
		// High priority channels
		select {
		case <-ins.closed:
			// Handle rest commands in channels
			ins.cleanCmdChannel(ins.chanPriorCmd)
			ins.cleanCmdChannel(ins.chanCmd)
			if !ins.coolTimer.Stop() {
				// For parallel access, use select.
				select {
				case <-ins.coolTimer.C:
				default:
				}
			}
			return
		case cmd := <-ins.chanPriorCmd: // Priority queue get
			ins.handleRequest(cmd)
			continue // Skip low priority channels and reselect
		default:
		}

		// Low priority channels
		select {
		case <-ins.closed:
			continue // continue to be handled in high priority section
		case cmd := <-ins.chanPriorCmd:
			ins.handleRequest(cmd)
		case cmd := <-ins.chanCmd:
			ins.handleRequest(cmd)
		case <-ins.coolTimer.C:
			// Warmup will not work until first call.
			// Double check, for it could timeout before a previous request got handled.
			if ins.IsReclaimed() || len(ins.chanPriorCmd) > 0 || len(ins.chanCmd) > 0 || atomic.LoadUint32(&ins.status) == INSTANCE_UNSTARTED {
				ins.resetCoolTimer(false)
			} else {
				// Force warm up.
				ins.warmUp()
			}
		case <-ins.coolReset:
			ins.resetCoolTimer(false)
		}
	}
}

// StartRecovery starts parallel recovery mode.
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
	changes := ins.backups.Reserve(CM.GetBackupCandidates())

	// Start backup and build logs
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

	// Start backups.
	available := ins.backups.Availables()
	atomic.StoreUint32(&ins.recovering, uint32(available))
	if available > ins.backups.Len()/2 {
		ins.log.Info("Parallel recovery started with %d backup instances:%v, changes: %d", available, msg.String(), changes)
	} else if available == 0 {
		ins.log.Warn("Unable to start parallel recovery due to no backup instance available")
	} else {
		ins.log.Warn("Parallel recovery started with insufficient %d backup instances:%v, changes: %d", available, msg.String(), changes)
	}

	return available
}

// StartDelegation delegates the instance to parallel buffer instances.
// Return # of delegates
func (ins *Instance) StartDelegation() int {
	if ins.delegates != nil {
		return ins.delegates.Len()
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	return ins.startDelegationLocked()
}

func (ins *Instance) startDelegationLocked() int {
	if ins.delegates != nil {
		return ins.delegates.Len()
	}

	delegates := CM.GetDelegates()
	// TODO: do some filtering
	ins.delegates = NewBackupsFromInstances(ins, delegates, &DelegateBackerAdapter{})

	// Start backup and build logs
	var msg strings.Builder
	for i := 0; i < ins.delegates.Len(); i++ {
		msg.WriteString(" ")
		delegate, ok := ins.delegates.StartByIndex(i, ins)
		if ok {
			msg.WriteString(strconv.FormatUint(delegate.Id(), 10))
		} else {
			msg.WriteString("N/A")
		}
	}

	// Start backups.
	available := ins.delegates.Availables()
	if available == 0 {
		ins.log.Warn("No delegates available.")
	} else {
		ins.log.Info("Delegate to %d instances:%v", available, msg.String())
	}

	return available
}

// Resume serving
func (ins *Instance) ResumeServing() {
	ins.mu.Lock()
	ins.resumeServingLocked()
	ins.mu.Unlock()
	ins.log.Info("Parallel recovered and service resumed")
}

func (ins *Instance) resumeServingLocked() {
	atomic.StoreUint32(&ins.recovering, 0)
	ins.backups.Stop(ins)
	// Clear whitelist during fast recovery.
	if ins.writtens.Len() > 0 {
		ins.writtens = hashmap.NewMap(TEMP_MAP_SIZE)
	}
}

func (ins *Instance) IsRecovering() bool {
	return atomic.LoadUint32(&ins.recovering) > 0
}

// Check if the instance is available for serving as a backup for specified instance.
// Return false if the instance is backing another instance.
func (ins *Instance) ReserveBacking() error {
	// Keep this lock free, or it may deadlock sometime. eg. When two instances are trying to backing each other.
	if ins.IsClosed() {
		return ErrInstanceClosed
	} else if ins.IsRecovering() {
		return ErrInstanceRecovering
	}

	// We don't check phase because backing and closed due to reclaiming/expiring are exclusive.
	// If instance is backing, reclaiming and expiring will not close instance.
	// If instance is closed due to reclaiming/expiring, it is not backing.
	if !atomic.CompareAndSwapUint32(&ins.backing, BACKING_DISABLED, BACKING_RESERVED) {
		return ErrReservationFailed
	}

	// Double check, or restore if failed.
	if ins.IsClosed() {
		atomic.StoreUint32(&ins.backing, BACKING_DISABLED)
		return ErrInstanceClosed
	} else if ins.IsRecovering() {
		atomic.StoreUint32(&ins.backing, BACKING_DISABLED)
		return ErrInstanceRecovering
	}
	return nil
}

// Start serving as the backup for specified instance.
// Return false if the instance is backing another instance.
func (ins *Instance) StartBacking(bakIns *Instance, bakId int, total int) bool {
	if atomic.LoadUint32(&ins.backing) != BACKING_RESERVED {
		ins.log.Error("Please call ReserveBacking before StartBacking")
		return false
	}

	ins.mu.Lock()
	ins.backingIns = bakIns
	ins.backingId = bakId
	ins.backingTotal = total
	atomic.StoreUint32(&ins.backing, BACKING_ENABLED)
	ins.mu.Unlock()

	// Manually trigger ping with payload to initiate parallel recovery
	payload, err := ins.backingIns.Meta.ToBackupPayload(ins.backingIns.Id(), bakId, total, ins.getRerouteThreshold())
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

func (ins *Instance) ForbidBacking() bool {
	return atomic.CompareAndSwapUint32(&ins.backing, BACKING_DISABLED, BACKING_FORBID)
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

	if ins.ForbidBacking() {
		ins.Close()
	}
}

func (ins *Instance) Phase() uint32 {
	return atomic.LoadUint32(&ins.phase)
}

func (ins *Instance) IsReclaimed() bool {
	return atomic.LoadUint32(&ins.phase) >= PHASE_RECLAIMED
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
	atomic.StoreUint32(&ins.status, INSTANCE_CLOSED)

	ins.log.Debug("[%v]Closing...", ins)
	select {
	case <-ins.closed:
		return
	default:
		close(ins.closed)
	}
	atomic.StoreUint32(&ins.awakeness, INSTANCE_SLEEPING)
	ins.flagValidatedLocked(nil, ErrInstanceClosed)

	// Close all links.
	ins.lm.Close()

	// We can't reset components to nil, for lambda can still running.

	ins.log.Info("[%v]Closed", ins)

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
	return ins.getSid()
}

func (ins *Instance) startSession(sid string) bool {
	_, started := ins.sessions.LoadOrStore(sid, true)
	return !started
}

func (ins *Instance) endSession(sid string) {
	ins.sessions.Delete(sid)
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

	// 1. Unresolved: wait for validation result.
	// 2: MinValidationInterval has not passed since last successful valiation: avoid frequent heartbeat.
	if !ins.validated.IsResolved() {
		ins.mu.Unlock()
		return castValidatedConnection(ins.validated)
	}
	lastOpt, _ := ins.validated.Options().(*ValidateOption)
	goodDue := time.Duration(ins.due - time.Now().Add(RTT).UnixNano())
	if ctrlLink := ins.lm.GetControl(); ctrlLink != nil &&
		(opt.Command == nil || opt.Command.Name() != protocol.CMD_PING) && // Time critical immediate ping
		ins.validated.Error() == nil && lastOpt != nil && !lastOpt.WarmUp && // Lambda extended not thanks to warmup
		atomic.LoadUint32(&ins.awakeness) == INSTANCE_ACTIVE && goodDue > 0 && // Lambda extended long enough to ignore ping.
		ins.reconcilingWorker == 0 { // Reconcilation is required.
		ins.mu.Unlock()
		ins.log.Info("Validation skipped. due in %v", time.Duration(goodDue)+RTT)
		return ctrlLink, nil // ctrl link can be replaced.
	}

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
			// Pass to timeout check.
			ins.validated.SetTimeout(TriggerTimeout)
			connectTimeout /= time.Duration(BackoffFactor) // Deduce by factor, so the timeout of next attempt (ping) start from DefaultConnectTimeout.
		} else if opt.WarmUp && !global.IsWarmupWithFixedInterval() {
			// Instance is warm, skip unnecssary warming up.
			return ins.flagValidatedLocked(ins.lm.GetControl()) // Skip any check in the "FlagValidated".
		} else {
			// If instance is active, PING is issued to ensure active
			// Closed safe: On closing, ctrlLink will be nil. By keeping a local copy and checking it is not nil, it is safe to make PING request.
			//   Like invoking, closed status will be checked in TryFlagValidated on processing the PONG.
			ctrl := ins.lm.GetControl() // Make a reference copy of ctrlLink to avoid it being changed.
			if ctrl != nil {
				if opt.Command != nil && opt.Command.Name() == protocol.CMD_PING {
					// Extra ping request for time critical immediate ping.
					ins.log.Debug("Ping with payload")
					ctrl.SendPing(opt.Command.(*types.Control).Payload) // Ignore err, see comments below.
				} else if ins.reconcilingWorker == ctrl.workerId {
					// Confirm reconciling.
					ins.log.Debug("Ping with reconcilation confirmation")
					payload, _ := ins.Meta.ToPayload(ins.id)
					ctrl.SendPing(payload) // Ignore err, see comments below.
					ins.reconcilingWorker = 0
				} else {
					ctrl.SendPing(DefaultPingPayload) // Ignore err, see comments below.
				}
			}
			// In the case that control is nil or fails to send ping, there is not much
			// we can do. Simply wait for timeout and wish new incoming control or lambda
			// being returned so that we can retrigger it.

			ins.validated.SetTimeout(connectTimeout)
		}

		// Start timeout, possibitilities are:
		// 1. ping may get stucked anytime.
		// 2. pong may lost (both after triggered or after ping), especially pong retrial has been disabled at lambda side.
		// TODO: In this version, no switching and unmanaged instance is handled. So ping will re-ping forever until being activated or proved to be sleeping.
		// ins.validated.SetTimeout(connectTimeout) // moved to above for different circumstances.

		// Wait for timeout or validation get concluded
		// Closed safe: On closing, validation will be concluded.
		if err := ins.validated.Timeout(); err == promise.ErrTimeout {
			// Exponential backoff
			connectTimeout *= time.Duration(BackoffFactor)
			if connectTimeout > MaxConnectTimeout {
				// Time to abandon
				return ins.flagValidatedLocked(nil, ErrValidationTimeout)
			}
			ins.log.Warn("Timeout on validating, re-ping...")
		} else {
			// Validated.
			return castValidatedConnection(ins.validated)
		}
	} // End of for::attemps
}

func (ins *Instance) tryTriggerLambda(opt *ValidateOption) bool {
	ins.log.Debug("[%v]Try activate lambda.", ins)
	switch atomic.LoadUint32(&ins.awakeness) {
	case INSTANCE_ACTIVATING:
		// Cases:
		// On validating, lambda deactivated and reactivating. Then validating timeouts and retries.
		return false // return false to allow issue ping, in case the pong is lost.
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
	err := ins.doTriggerLambda(opt)
	awakeness := atomic.LoadUint32(&ins.awakeness)
	for {
		// For instance of status maybe, simply current invocation.
		if awakeness == INSTANCE_MAYBE {
			return
		}

		// Handle reclaimation
		if err != nil && err == ErrInstanceReclaimed {
			atomic.StoreUint32(&ins.phase, PHASE_RECLAIMED)
			ins.log.Info("Reclaimed")

			// Initiate delegation.
			ins.StartDelegation()

			// We can close the instance if it is not backing any instance.
			if !ins.IsBacking(true) {
				// Close() will handle status and validation and we can safely exit.
				ins.Close()
				return
			}
		}

		// Continue to handle validation
		if ins.validated.IsResolved() {
			// Don't overwrite the MAYBE status.
			if atomic.CompareAndSwapUint32(&ins.awakeness, awakeness, INSTANCE_SLEEPING) {
				awakeness = INSTANCE_SLEEPING
				ins.log.Debug("[%v]Status updated.", ins)
			} else {
				ins.log.Error("[%v]Unexpected status.", ins)
			}
			break // Break to proceed after trigger cleanup.
		} else if ins.validated.Options().(*ValidateOption).WarmUp { // Use ValidationOption of most recent validation to reflect up to date status.
			// No retry for warming up.
			// Possible reasons
			// 1. Pong is delayed and lambda is returned without requesting for recovery, or the lambda will wait for the ending of the recovery.
			ins.mu.Lock()
			atomic.StoreUint32(&ins.awakeness, INSTANCE_SLEEPING)
			// No need to return a validated connection. If someone do require the connection, it is an unexpected error.
			ins.flagValidatedLocked(nil, ErrWarmupReturn)
			ins.mu.Unlock()

			ins.log.Debug("[%v]Detected unvalidated warmup, ignored.", ins)
			break // Break to proceed after trigger cleanup.
		} else {
			// Validating, retrigger.
			atomic.StoreUint32(&ins.awakeness, INSTANCE_ACTIVATING)

			// Added by Tianium: Since lambda is stopped, do some cleanup
			ins.lm.Reset()

			ins.log.Info("[%v]Reactivateing...", ins)
			err = ins.doTriggerLambda(ins.validated.Options().(*ValidateOption))
			awakeness = atomic.LoadUint32(&ins.awakeness)
		}
	}

	// Added by Tianium: Since lambda is stopped, do some cleanup
	ins.lm.Reset()
}

func (ins *Instance) doTriggerLambda(opt *ValidateOption) error {
	if ins.Meta.Stale {
		// TODO: Check stale status
		ins.log.Warn("Detected stale meta: %d", ins.Meta.Term)
	}
	switch global.Options.GetInvoker() {
	case global.InvokerLocal:
		ins.client = &invoker.LocalInvoker{}
	default:
		ins.client = lambda.New(AwsSession, &aws.Config{Region: aws.String(config.AWSRegion)})
	}

	tips := &url.Values{}
	reqInsId := uint64(0) // Initialize reqInsId as -1
	reqInsId--
	if opt.Command != nil && opt.Command.Name() == protocol.CMD_GET {
		tips.Set(protocol.TIP_SERVING_KEY, opt.Command.GetRequest().Key)
		reqInsId = opt.Command.GetRequest().InsId
	}

	var status protocol.Status
	status.Metas = make([]protocol.Meta, 0, 3) // Main, backing, delegating
	// Main store only
	status.Metas = append(status.Metas, *ins.Meta.ToProtocolMeta(ins.Id()))
	if reqInsId == ins.Id() {
		status.Metas[len(status.Metas)-1].Tip = tips.Encode()
	}
	// Backing store
	if ins.IsBacking(false) {
		status.Metas = append(status.Metas, *ins.backingIns.Meta.ToProtocolMeta(ins.backingIns.Id()))
		if reqInsId == ins.Id() {
			// Reset tips
			tips = &url.Values{}
		}
		tips.Set(protocol.TIP_BACKUP_KEY, strconv.Itoa(ins.backingId))
		tips.Set(protocol.TIP_BACKUP_TOTAL, strconv.Itoa(ins.backingTotal))
		tips.Set(protocol.TIP_MAX_CHUNK, strconv.FormatUint(ins.getRerouteThreshold(), 10))
		status.Metas[len(status.Metas)-1].Tip = tips.Encode()
	}
	// Check extra one time store status (eg. delegate store)
	ins.log.Debug("invoke with command: %v", opt.Command)
	if opt.Command != nil {
		// ins.log.Info("command with info: %v", opt.Command.GetInfo())
		if meta, ok := opt.Command.GetInfo().(*protocol.Meta); ok {
			// ins.log.Info("meta added: %v", meta)
			status.Metas = append(status.Metas, *meta)
		}
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
	if opt.WarmUp || opt.Command.Name() == protocol.CMD_PING {
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

	ins.Meta.Stale = event.IsRecoveryEnabled() // Reset to stale if recovery is enabled.

	ctx, cancel := context.WithCancel(context.Background())
	ins.mu.Lock()
	ins.lambdaCanceller = cancel
	ins.mu.Unlock()
	output, err := ins.client.InvokeWithContext(ctx, input)
	ins.client = nil
	ins.mu.Lock()
	ins.lambdaCanceller = nil
	ins.mu.Unlock()

	ins.endSession(event.Sid)

	if err != nil {
		ins.Meta.Stale = false
		ins.log.Error("[%v]Error on activating lambda store: %v", ins, err)
		return err
	}

	ins.log.Debug("[%v]Lambda instance deactivated.", ins)
	if ins.checkError(output) {
		ins.Meta.Stale = false
		return nil
	}

	// Handle output
	if !event.IsRecoveryEnabled() {
		// Ignore output
		return nil
	} else if len(output.Payload) == 0 {
		// Recovery enabled but no output
		ins.log.Error("No instance lineage returned, output: %v", output)
		return nil
	}

	// Parse output
	var outputStatus protocol.Status
	if err := json.Unmarshal(output.Payload, &outputStatus); err != nil {
		ins.log.Error("Failed to unmarshal payload of lambda output: %v, payload", err, string(output.Payload))
		return nil
	}

	// Handle output
	if outputStatus.Capacity > 0 && outputStatus.Mem > 0 {
		ins.Meta.ResetCapacity(outputStatus.Capacity, outputStatus.Effective)
		ins.Meta.mem = outputStatus.Mem
		ins.Meta.sizeModified = outputStatus.Modified
		ins.log.Debug("Capacity synchronized: cap %d, effective %d, stored %d, modified %d", ins.Meta.Capacity, ins.Meta.EffectiveCapacity(), ins.Meta.Size(), outputStatus.Modified)
	}
	if len(outputStatus.Metas) > 0 {
		uptodate, err := ins.Meta.FromProtocolMeta(&outputStatus.Metas[0]) // Ignore backing store
		if err != nil && err == ErrInstanceReclaimed {
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
		// Reset reconciling request after lambda returned.
		ins.reconcilingWorker = 0
	}
	return nil
}

func (ins *Instance) checkError(output *lambda.InvokeOutput) bool {
	var failed bool
	var outputError *protocol.OutputError
	if *output.StatusCode >= 400 {
		failed = true
	}

	if output.FunctionError != nil && output.Payload != nil {
		outputError = &protocol.OutputError{}
		if err := json.Unmarshal(output.Payload, outputError); err != nil {
			outputError.Type = "Unmarshal error payload failed"
			outputError.Message = err.Error()
		}
		failed = true
		ins.log.Error("Lambda deactivated with error(statuscode: %d, %s:%s)", *output.StatusCode, outputError.Type, outputError.Message)
	} else if failed {
		ins.log.Error("Lambda deactivated with error(statuscode: %d)", *output.StatusCode)
	}

	return failed
}

func (ins *Instance) AbandonLambda() {
	if ins.lambdaCanceller == nil {
		ins.log.Info("No invoked lambda found, assuming abandoned.")
		return
	}

	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.lambdaCanceller == nil {
		ins.log.Info("No invoked lambda found, assuming abandoned.")
		return
	}

	ins.lambdaCanceller()
	ins.log.Info("Lambda abandoned")
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
		ins.lm.AddDataLink(conn)
		return conn, ErrNotCtrlLink
	}

	ins.log.Debug("[%v]Confirming validation...", ins)
	ins.flagWarmed()
	// Check possible duplicated session
	oldCtrl := ins.lm.GetControl()
	newSession := ins.startSession(sid)
	if conn != oldCtrl {
		if !newSession && oldCtrl != nil && !conn.IsSameWorker(oldCtrl) {
			// Deny session if session is duplicated and from another worker
			return oldCtrl, ErrDuplicatedSession
		} else if newSession {
			ins.log.Debug("Session %s started.", sid)
		} else {
			ins.log.Debug("Session %s resumed.", sid)
		}

		ins.lm.SetControl(conn)

		// There are three cases for link switch:
		// 1. Old node get reclaimed
		// 2. Reconnection
		atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_ACTIVATING, INSTANCE_ACTIVE)
		// 3. Migration, which we have no way to know its status. (Forget it now)
		// TODO: Fix migration handling later.
		// atomic.StoreUint32(&ins.awakeness, INSTANCE_MAYBE)
		ins.log.Debug("[%v]Control link updated.", ins)
	} else {
		// Simply try update status.
		atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_ACTIVATING, INSTANCE_ACTIVE)
	}

	// Skip actions if we've seen it.
	if newSession {
		// These two flags are exclusive because backing only mode will enable reclaimation claim and disable fast recovery.
		if flags&protocol.PONG_RECOVERY > 0 {
			ins.log.Debug("Parallel recovery requested.")
			ins.startRecoveryLocked()
		} else if (flags&protocol.PONG_ON_INVOKING > 0) && ins.IsRecovering() {
			// If flags indicate it is from function invocation without recovery request, the service is resumed but somehow we missed it.
			ins.resumeServingLocked()
			ins.log.Info("Function invoked without data loss, assuming parallel recovered and service resumed.")
		}

		if flags&protocol.PONG_RECLAIMED > 0 {
			// PONG_RECLAIMED will be issued for instances in PHASE_BACKING_ONLY or PHASE_EXPIRED.
			atomic.StoreUint32(&ins.phase, PHASE_RECLAIMED)
			// Initiate delegation.
			ins.startDelegationLocked()
			// We can close the instance if it is not backing any instance.
			if !ins.IsBacking(true) {
				ins.log.Info("Reclaimed")
				validConn, _ := ins.flagValidatedLocked(conn, ErrInstanceReclaimed)
				return validConn, nil
			} else {
				ins.log.Info("Reclaimed, keep running because the instance is backing another instance.")
			}
		}
	}

	validConn, _ := ins.flagValidatedLocked(conn)
	if validConn != conn {
		ins.log.Debug("[%v]Already validated", ins)
		return validConn, ErrInstanceValidated
	} else {
		ins.SetDue(time.Now().Add(MinValidationInterval).UnixNano()) // Set MinValidationInterval
		return validConn, nil
	}
}

func (ins *Instance) flagValidatedLocked(conn *Connection, errs ...error) (*Connection, error) {
	var err error
	if len(errs) > 0 {
		err = errs[0]
	}
	if _, resolveErr := ins.validated.Resolve(conn, err); resolveErr == nil {
		if err != nil && err != ErrWarmupReturn && err != ErrInstanceReclaimed {
			numFailure := atomic.AddUint32(&ins.numFailure, 1)
			ins.log.Warn("[%v]Validation failed: %v", ins, err)
			if int(numFailure) >= MaxValidationFailure || err == ErrValidationTimeout {
				ins.log.Warn("Maxed validation failure reached, abandon active instance...")
				atomic.StoreUint32(&ins.numFailure, 0)
				go ins.AbandonLambda()
			}
		} else {
			atomic.StoreUint32(&ins.numFailure, 0)
			if err != nil {
				ins.log.Debug("[%v]%v", ins, err)
			} else {
				ins.log.Debug("[%v]Validated", ins)
			}
		}
	}
	return castValidatedConnection(ins.validated)
}

// TODO: bye functionality is to be reviewed. For now, connection close should not trigger bye.
func (ins *Instance) bye(conn *Connection) {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.lm.GetControl() != conn {
		// We don't care any rogue bye
		return
	}

	if atomic.CompareAndSwapUint32(&ins.awakeness, INSTANCE_MAYBE, INSTANCE_SLEEPING) {
		ins.log.Info("[%v]Bye from unmanaged instance, flag as sleeping.", ins)
	}
	// } else {
	// 	ins.log.Debug("[%v]Bye ignored, waiting for return of synchronous invocation.", ins)
	// }
}

func (ins *Instance) handleRequest(cmd types.Command) {
	if req := cmd.GetRequest(); req != nil && req.IsResponded() {
		// Request is responded
		return
	} else if req != nil && req.Cmd == protocol.CMD_GET &&
		ins.IsRecovering() && ins.rerouteGetRequest(req) {
		// On parallel recovering, we will try reroute get requests.
		return
	}

	// Set instance busy on request.
	ins.busy(cmd)
	defer ins.doneBusy(cmd)

	leftAttempts, lastErr := cmd.LastError()
	for leftAttempts > 0 {
		if lastErr != nil {
			ins.log.Info("Attempt %d: %v", types.MAX_ATTEMPTS-leftAttempts+1, cmd)
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
		reclaimPass := false
		if cmd.Name() == protocol.CMD_GET && ins.IsReclaimed() {
			reclaimPass = ins.tryRerouteDelegateRequest(cmd.GetRequest(), util.Ifelse(cmd.GetRequest().Option&protocol.REQUEST_GET_OPTIONAL > 0, DISPATCH_OPT_RELOCATED, 0).(int))
		}

		if reclaimPass {
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

		ins.log.Debug("Ready to %v", cmd)

		// Make request
		lastErr = ins.request(ctrlLink, cmd, validateDuration)
		leftAttempts = cmd.MarkError(err)
		if lastErr == nil || leftAttempts == 0 { // Request can become streaming, so we need to test again everytime.
			break
		} else {
			ins.ResetDue() // Force ping on error
			ins.log.Warn("Failed to %v: %v", cmd, lastErr)
		}
	}
	if lastErr != nil {
		ins.ResetDue() // Force ping on error
		if leftAttempts == 0 {
			ins.log.Error("%v, give up: %v", cmd.FailureError(), lastErr)
		} else {
			ins.log.Error("Abandon retrial: %v", lastErr)
		}
		if request, ok := cmd.(*types.Request); ok {
			request.SetResponse(lastErr)
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
	if _, ok := ins.writtens.Load(req.Key); ok {
		ins.log.Debug("Detected reroute override for key %s", req.Key)
		return false
	}

	// Thread safe
	backup, ok := ins.backups.GetByKey(req.Key)
	if !ok {
		// Backup is not available, can be recovered or simply not available.
		return false
	}

	// No reroute for chunk larger than threshold()
	if req.Size() > int64(backup.getRerouteThreshold()) {
		return false
	}

	select {
	case backup.chanPriorCmd <- req: // Rerouted request should not be queued again.
	default:
		ins.log.Info("We will not try to overload backup node, stop reroute %v", req)
		return false
	}

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

func (ins *Instance) tryRerouteDelegateRequest(req *types.Request, opts int) bool {
	if req == nil || req.Cmd != protocol.CMD_GET {
		return false
	}

	// Delegate will always relocate the chunk.
	// This will only happen once since redelegated instance will refuse relocation due to req.InsId != ins.Id()
	fallbacked := opts&DISPATCH_OPT_RELOCATED > 0
	if !fallbacked {
		fallbacked = ins.relocateGetRequest(req)
	}

	if ins.delegates == nil {
		ins.log.Warn("Has not delegated yet, stop reroute %v", req)
		return fallbacked
	}

	loc, total, _ := ins.delegates.Locator().Locate(req.Key)
	delegate, _ := ins.delegates.GetByLocation(loc, total)

	if delegate == nil {
		return fallbacked
	}

	// No reroute for chunk larger than threshold()
	if req.Size() > int64(delegate.getRerouteThreshold()) {
		return fallbacked
	}

	// Set optional flag will not be override.
	if fallbacked {
		req.Option |= protocol.REQUEST_GET_OPTIONAL
	}
	// Chained delegation is safe if the instance triggering relocation is not current instance.
	err := delegate.DispatchWithOptions(req, opts|utils.Ifelse(fallbacked, DISPATCH_OPT_RELOCATED, 0).(int))
	if err != nil {
		ins.log.Warn("Delegate %s to node %d as %d of %d with error: %v.", req.Key, delegate.Id(), loc, total, err)
		return fallbacked
	} else {
		ins.log.Debug("Delegate %s to node %d as %d of %d.", req.Key, delegate.Id(), loc, total)
		return true // fallbacked || true
	}
}

func (ins *Instance) request(ctrlLink *Connection, cmd types.Command, validateDuration time.Duration) error {
	cmdName := cmd.Name()
	switch req := cmd.(type) {
	case *types.Request:
		collector.CollectRequest(collector.LogRequestValidation, req.CollectorEntry, int64(validateDuration))

		// Select link
		useDataLink := req.Size() > MaxControlRequestSize // Changes: will fallback to ctrl link.

		// Record write operations
		switch cmdName {
		case protocol.CMD_SET:
			fallthrough
		case protocol.CMD_DEL:
			if ins.IsRecovering() {
				ins.log.Debug("Override rerouting for key %s due to del", req.Key)
				ins.writtens.Store(req.Key, &struct{}{})
			}
		}
		return ctrlLink.SendRequest(req, useDataLink, ins.lm) // Offer lm, so SendRequest is ctrl link free. (We still need to get ctrl link first to confirm lambda status.)

	case *types.Control:
		isDataRequest := false

		switch cmdName {
		case protocol.CMD_PING:
			// Simply ignore.
			return nil
		case protocol.CMD_DATA:
			isDataRequest = true
		case protocol.CMD_DEL:
			if ins.IsRecovering() {
				ins.log.Debug("Override rerouting for key %s due to del", req.Request.Key)
				ins.writtens.Store(req.Request.Key, &struct{}{})
			}
		}

		if err := ctrlLink.SendControl(req); err != nil && isDataRequest {
			global.DataCollected.Done()
			// No error returned, control commands will not be retry for now.
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
	ins.resetCoolTimer(false)
}

func (ins *Instance) flagWarmed() {
	// Only switch if instance unstarted.
	atomic.CompareAndSwapUint32(&ins.status, INSTANCE_UNSTARTED, INSTANCE_RUNNING)
	if global.IsWarmupWithFixedInterval() || ins.IsReclaimed() {
		return
	}

	ins.resetCoolTimer(true)
}

func (ins *Instance) resetCoolTimer(flag bool) {
	if flag {
		select {
		case ins.coolReset <- struct{}{}:
		default:
			// skip
		}
		return
	}

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

func (ins *Instance) busy(cmd types.Command) {
	if cmd.String() == protocol.CMD_SET {
		atomic.AddInt32(&ins.numOutbound, 1)
	} else {
		atomic.AddInt32(&ins.numInbound, 1)
	}
}

func (ins *Instance) doneBusy(cmd types.Command) {
	if cmd.String() == protocol.CMD_SET {
		atomic.AddInt32(&ins.numOutbound, -1)
	} else {
		atomic.AddInt32(&ins.numInbound, -1)
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

func (ins *Instance) SetDue(due int64) {
	ins.due = due
}

func (ins *Instance) ResetDue() {
	ins.due = time.Now().UnixNano()
}

func (ins *Instance) getRerouteThreshold() uint64 {
	if ins.rerouteThreshold == 0 {
		ins.rerouteThreshold = ins.Meta.EffectiveCapacity() / uint64(ins.backups.Len()) / 2
	}
	return ins.rerouteThreshold
}

func (ins *Instance) reconcileStatus(conn *Connection, meta *protocol.ShortMeta) {
	if meta.Term > ins.Meta.Term {
		ins.log.Debug("Reconciling meta: %v", meta)
		ins.Meta.Reconcile(meta)
	}
	ins.reconcilingWorker = conn.workerId // Track the worker that is reconciling.
}
