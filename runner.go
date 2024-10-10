package boomer

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/sandwich-go/boost"

	"github.com/sandwich-go/boost/xpanic"
)

const (
	stateInit     = "ready"
	stateSpawning = "spawning"
	stateRunning  = "running"
	stateStopped  = "stopped"
	stateQuitting = "quitting"
)

const (
	slaveReportInterval = 3 * time.Second
	heartbeatInterval   = 1 * time.Second
)

type spawnNumber struct{}

func (*spawnNumber) String() string { return "spawn_number" }

var ContextSpawnNumberKey = spawnNumber(struct{}{})

type spawnCount struct{}

func (*spawnCount) String() string { return "spawn_count" }

var ContextSpawnCountKey = spawnCount(struct{}{})

type runner struct {
	state string
	stats *requestStats

	// TODO: we save user_class_count in spawn message and send it back to master without modification, may be a bad idea?
	userClassesCountFromMaster map[string]int64

	numClients int
	spawnRate  int
	spawnChan  chan *SpawnArgs

	// close this channel will stop all goroutines used in runner, including running workers.
	shutdownChan chan bool

	outputs []Output
}

// safeRun runs fn and recovers from unexpected panics.
// it prevents panics from Task.Fn crashing boomer.
func (r *runner) safeRun(ctx context.Context, fn func(context.Context)) {
	defer func() {
		// don't panic
		err := recover()
		if err != nil {
			stackTrace := debug.Stack()
			errMsg := fmt.Sprintf("%v", err)
			os.Stderr.Write([]byte(errMsg))
			os.Stderr.Write([]byte("\n"))
			os.Stderr.Write(stackTrace)
		}
	}()
	fn(ctx)
}

func (r *runner) addOutput(o Output) {
	r.outputs = append(r.outputs, o)
}

func (r *runner) outputOnStart() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStart()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnEevent(data map[string]interface{}) {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnEvent(data)
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnStop() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStop()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) startSpawning(spawnCount int, spawnRate float64, spawnCompleteFunc func()) {
	Events.Publish(EVENT_SPAWN, spawnCount, spawnRate)

	r.numClients = spawnCount

	boost.LogInfof("Spawning %d clients immediately", spawnCount)
	select {
	case <-r.spawnChan:
	default:
	}
	select {
	case r.spawnChan <- &SpawnArgs{Count: spawnCount, Rate: spawnRate}:
	default:
		boost.LogInfo("Failed to send spawn message to logic")
	}
	if spawnCompleteFunc != nil {
		spawnCompleteFunc()
	}
}

func (r *runner) stop() {
	// publish the boomer stop event
	// user's code can subscribe to this event and do thins like cleaning up
	Events.Publish(EVENT_STOP)
}

type localRunner struct {
	runner

	spawnCount int
}

func newLocalRunner(spawnChan chan *SpawnArgs, spawnCount int, spawnRate int) (r *localRunner) {
	r = &localRunner{}
	r.spawnRate = spawnRate
	r.spawnCount = spawnCount
	r.spawnChan = spawnChan
	r.shutdownChan = make(chan bool)
	r.stats = newRequestStats()
	return r
}

func (r *localRunner) run(ctx context.Context) error {
	r.state = stateInit
	r.stats.start()
	r.outputOnStart()

	go func() {
		for {
			select {
			case <-ctx.Done():
				Events.Publish(EVENT_QUIT)
				r.stop()
				r.outputOnStop()
				return
			case data := <-r.stats.messageToRunnerChan:
				data["user_count"] = r.numClients
				r.outputOnEevent(data)
			case <-r.shutdownChan:
				Events.Publish(EVENT_QUIT)
				r.stop()
				r.outputOnStop()
				return
			case <-time.After(time.Second):
				if r.numClients < r.spawnCount {
					totalCount := r.numClients + r.spawnRate
					if totalCount > r.spawnCount {
						totalCount = r.spawnCount
					}
					r.startSpawning(totalCount, float64(r.spawnRate), nil)
				}
			}
		}
	}()
	return nil
}

func (r *localRunner) shutdown() {
	if r.stats != nil {
		r.stats.close()
	}
	close(r.shutdownChan)
}

// SlaveRunner connects to the master, spawns goroutines and collects stats.
type slaveRunner struct {
	runner

	nodeID                     string
	masterHost                 string
	masterPort                 int
	waitForAck                 sync.WaitGroup
	lastReceivedSpawnTimestamp int64
	client                     client
}

func newSlaveRunner(masterHost string, masterPort int, spawnChan chan *SpawnArgs) (r *slaveRunner) {
	r = &slaveRunner{}
	r.masterHost = masterHost
	r.masterPort = masterPort
	r.spawnChan = spawnChan
	r.waitForAck = sync.WaitGroup{}
	r.nodeID = getNodeID()
	r.shutdownChan = make(chan bool)
	r.stats = newRequestStats()
	return r
}

func (r *slaveRunner) spawnComplete() {
	data := make(map[string]interface{})
	data["count"] = r.numClients
	data["user_classes_count"] = r.userClassesCountFromMaster
	r.client.sendChannel() <- newGenericMessage("spawning_complete", data, r.nodeID)
	r.state = stateRunning
}

func (r *slaveRunner) onQuiting() {
	if r.state != stateQuitting {
		r.client.sendChannel() <- newGenericMessage("quit", nil, r.nodeID)
	}
}

func (r *slaveRunner) shutdown() {
	if r.stats != nil {
		r.stats.close()
	}
	if r.client != nil {
		r.client.close()
	}
	close(r.shutdownChan)
}

func (r *slaveRunner) sumUsersAmount(msg *genericMessage) int {
	userClassesCount := msg.Data["user_classes_count"]
	userClassesCountMap := userClassesCount.(map[interface{}]interface{})

	// Save the original field and send it back to master in spawnComplete message.
	r.userClassesCountFromMaster = make(map[string]int64)
	amount := 0
	for class, num := range userClassesCountMap {
		c, ok := class.(string)
		n, ok2 := castToInt64(num)
		if !ok || !ok2 {
			boost.LogInfof("user_classes_count in spawn message can't be casted to map[string]int64, current type is map[%T]%T, ignored!\n", class, num)
			continue
		}
		r.userClassesCountFromMaster[c] = n
		amount = amount + int(n)
	}
	return amount
}

// TODO: Since locust 2.0, spawn rate and user count are both handled by master.
// But user count is divided by user classes defined in locustfile, because locust assumes that
// master and workers use the same locustfile. Before we find a better way to deal with this,
// boomer sums up the total amout of users in spawn message and uses task weight to spawn goroutines like before.
func (r *slaveRunner) onSpawnMessage(msg *genericMessage) {
	if timeStamp, ok := msg.Data["timestamp"]; ok {
		if timeStampInt64, ok := castToInt64(timeStamp); ok {
			if timeStampInt64 <= r.lastReceivedSpawnTimestamp {
				boost.LogInfo("Discard spawn message with older or equal timestamp than timestamp of previous spawn message")
				return
			} else {
				r.lastReceivedSpawnTimestamp = timeStampInt64
			}
		}
	}

	r.client.sendChannel() <- newGenericMessage("spawning", nil, r.nodeID)
	workers := r.sumUsersAmount(msg)
	r.startSpawning(workers, float64(workers), r.spawnComplete)
}

func (r *slaveRunner) onAckMessage(msg *genericMessage) {
	r.waitForAck.Done()
	Events.Publish(EVENT_CONNECTED)
}

func (r *slaveRunner) sendClientReadyAndWaitForAck() {
	r.waitForAck = sync.WaitGroup{}
	r.waitForAck.Add(1)
	// locust allows workers to bypass version check by sending -1 as version
	r.client.sendChannel() <- newClientReadyMessage("client_ready", -1, r.nodeID)

	go func() {
		if waitTimeout(&r.waitForAck, 5*time.Second) {
			boost.LogInfo("Timeout waiting for ack message from master, you may use a locust version before 2.10.0 or have a network issue.")
		}
	}()
}

// Runner acts as a state machine.
func (r *slaveRunner) onMessage(msgInterface message) {
	msg, ok := msgInterface.(*genericMessage)
	if !ok {
		boost.LogInfo("Receive unknown type of meesage from master.")
		return
	}
	switch r.state {
	case stateInit:
		switch msg.Type {
		case "ack":
			r.onAckMessage(msg)
		case "spawn":
			r.state = stateSpawning
			r.stats.clearStatsChan <- true
			r.onSpawnMessage(msg)
		case "quit":
			Events.Publish(EVENT_QUIT)
		}
	case stateSpawning:
		fallthrough
	case stateRunning:
		switch msg.Type {
		case "spawn":
			r.state = stateSpawning
			r.onSpawnMessage(msg)
		case "stop":
			r.stop()
			r.state = stateStopped
			boost.LogInfo("Recv stop message from master, all the goroutines are stopped")
			r.client.sendChannel() <- newGenericMessage("client_stopped", nil, r.nodeID)
			r.sendClientReadyAndWaitForAck()
			Events.Publish(EVENT_STOPROBOT)
			r.state = stateInit
		case "quit":
			r.stop()
			boost.LogInfo("Recv quit message from master, all the goroutines are stopped")
			Events.Publish(EVENT_QUIT)
			r.state = stateInit
		}
	case stateStopped:
		switch msg.Type {
		case "spawn":
			r.state = stateSpawning
			r.stats.clearStatsChan <- true
			r.onSpawnMessage(msg)
		case "quit":
			Events.Publish(EVENT_QUIT)
			r.state = stateInit
		}
	}
}

func (r *slaveRunner) startListener() {
	go func() {
		for {
			select {
			case msg := <-r.client.recvChannel():
				r.onMessage(msg)
			case <-r.shutdownChan:
				return
			}
		}
	}()
}

func (r *slaveRunner) run(ctx context.Context) error {
	r.state = stateInit
	r.client = newClient(r.masterHost, r.masterPort, r.nodeID)

	err := r.client.connect(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "Socket type DEALER is not compatible with PULL") {
			boost.LogInfo("Newer version of locust changes ZMQ socket to DEALER and ROUTER, you should update your locust version.")
		} else {
			boost.LogInfof("Failed to connect to master(%s:%d) with error %v\n", r.masterHost, r.masterPort, err)
		}
		return err
	}

	// listen to master
	r.startListener()

	r.stats.start()
	r.outputOnStart()

	r.sendClientReadyAndWaitForAck()

	// report to master
	go xpanic.AutoRecover("report to master", func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				if r.state == stateInit || r.state == stateStopped {
					continue
				}
				data["user_count"] = r.numClients
				data["user_classes_count"] = r.userClassesCountFromMaster
				r.client.sendChannel() <- newGenericMessage("stats", data, r.nodeID)
				r.outputOnEevent(data)
			case <-r.shutdownChan:
				r.outputOnStop()
				return
			}
		}
	})

	// heartbeat
	// See: https://github.com/locustio/locust/commit/a8c0d7d8c588f3980303358298870f2ea394ab93
	go xpanic.AutoRecover("heart beat", func() {
		var ticker = time.NewTicker(heartbeatInterval)
		for {
			select {
			case <-ticker.C:
				CPUUsage := GetCurrentCPUUsage()
				data := map[string]interface{}{
					"state":             r.state,
					"current_cpu_usage": CPUUsage,
				}
				r.client.sendChannel() <- newGenericMessage("heartbeat", data, r.nodeID)
			case <-r.shutdownChan:
				return
			}
		}
	})

	Events.Subscribe(EVENT_QUIT, r.onQuiting)
	return nil
}
