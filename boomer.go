package boomer

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sandwich-go/boost/xerror"
)

var defaultBoomer = &Boomer{}

// Mode is the running mode of boomer, both standalone and distributed are supported.
type Mode int

const (
	// DistributedMode requires connecting to a master.
	DistributedMode Mode = iota
	// StandaloneMode will run without a master.
	StandaloneMode
)

// A Boomer is used to run tasks.
// This type is exposed, so users can create and control a Boomer instance programmatically.
type Boomer struct {
	masterHost  string
	masterPort  int
	mode        Mode
	slaveRunner *slaveRunner

	localRunner *localRunner
	spawnCount  int
	spawnRate   int

	cpuProfileFile     string
	cpuProfileDuration time.Duration

	memoryProfileFile     string
	memoryProfileDuration time.Duration

	outputs   []Output
	spawnChan chan<- *SpawnArgs
}

type SpawnArgs struct {
	Count int
	Rate  float64
}

// NewBoomer returns a new Boomer.
func NewBoomer(masterHost string, masterPort int) *Boomer {
	return &Boomer{
		masterHost: masterHost,
		masterPort: masterPort,
		mode:       DistributedMode,
	}
}

// NewStandaloneBoomer returns a new Boomer, which can run without master.
func NewStandaloneBoomer(spawnCount int, spawnRate int) *Boomer {
	return &Boomer{
		spawnCount: spawnCount,
		spawnRate:  spawnRate,
		mode:       StandaloneMode,
	}
}

// SetMode only accepts boomer.DistributedMode and boomer.StandaloneMode.
func (b *Boomer) SetMode(mode Mode) {
	switch mode {
	case DistributedMode:
		b.mode = DistributedMode
	case StandaloneMode:
		b.mode = StandaloneMode
	default:
		log.Println("Invalid mode, ignored!")
	}
}

// AddOutput accepts outputs which implements the boomer.Output interface.
func (b *Boomer) AddOutput(o Output) {
	b.outputs = append(b.outputs, o)
}

// EnableCPUProfile will start cpu profiling after run.
func (b *Boomer) EnableCPUProfile(cpuProfileFile string, duration time.Duration) {
	b.cpuProfileFile = cpuProfileFile
	b.cpuProfileDuration = duration
}

// EnableMemoryProfile will start memory profiling after run.
func (b *Boomer) EnableMemoryProfile(memoryProfileFile string, duration time.Duration) {
	b.memoryProfileFile = memoryProfileFile
	b.memoryProfileDuration = duration
}

// Run accepts a slice of Task and connects to the locust master.
func (b *Boomer) Run(ctx context.Context, spawnChan chan *SpawnArgs) (err error) {
	if b.cpuProfileFile != "" {
		err := StartCPUProfile(b.cpuProfileFile, b.cpuProfileDuration)
		if err != nil {
			log.Printf("Error starting cpu profiling, %v", err)
		}
	}
	if b.memoryProfileFile != "" {
		err := StartMemoryProfile(b.memoryProfileFile, b.memoryProfileDuration)
		if err != nil {
			log.Printf("Error starting memory profiling, %v", err)
		}
	}

	switch b.mode {
	case DistributedMode:
		b.slaveRunner = newSlaveRunner(b.masterHost, b.masterPort, spawnChan)
		for _, o := range b.outputs {
			b.slaveRunner.addOutput(o)
		}
		err = b.slaveRunner.run(ctx)
	case StandaloneMode:
		b.localRunner = newLocalRunner(spawnChan, b.spawnCount, b.spawnRate)
		for _, o := range b.outputs {
			b.localRunner.addOutput(o)
		}
		err = b.localRunner.run(ctx)
	default:
		return xerror.NewText("Invalid mode, expected boomer.DistributedMode or boomer.StandaloneMode")
	}
	return
}

// RecordSuccess reports a success.
func (b *Boomer) RecordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	if b.localRunner == nil && b.slaveRunner == nil {
		return
	}
	switch b.mode {
	case DistributedMode:
		b.slaveRunner.stats.requestSuccessChan <- &requestSuccess{
			requestType:    requestType,
			name:           name,
			responseTime:   responseTime,
			responseLength: responseLength,
		}
	case StandaloneMode:
		b.localRunner.stats.requestSuccessChan <- &requestSuccess{
			requestType:    requestType,
			name:           name,
			responseTime:   responseTime,
			responseLength: responseLength,
		}
	}
}

// RecordFailure reports a failure.
func (b *Boomer) RecordFailure(requestType, name string, responseTime int64, exception string) {
	if b.localRunner == nil && b.slaveRunner == nil {
		return
	}
	switch b.mode {
	case DistributedMode:
		b.slaveRunner.stats.requestFailureChan <- &requestFailure{
			requestType:  requestType,
			name:         name,
			responseTime: responseTime,
			error:        exception,
		}
	case StandaloneMode:
		b.localRunner.stats.requestFailureChan <- &requestFailure{
			requestType:  requestType,
			name:         name,
			responseTime: responseTime,
			error:        exception,
		}
	}
}

// Quit will send a quit message to the master.
func (b *Boomer) Quit() {
	Events.Publish(EVENT_QUIT)
	var ticker = time.NewTicker(3 * time.Second)

	switch b.mode {
	case DistributedMode:
		// wait for quit message is sent to master
		select {
		case <-b.slaveRunner.client.disconnectedChannel():
			break
		case <-ticker.C:
			log.Println("Timeout waiting for sending quit message to master, boomer will quit any way.")
			break
		}
		b.slaveRunner.shutdown()
	case StandaloneMode:
		b.localRunner.shutdown()
	}
}

// Run tasks without connecting to the master.
func runTasksForTest(tasks ...*Task) {
	taskNames := strings.Split(runTasks, ",")
	for _, task := range tasks {
		if task.Name == "" {
			continue
		} else {
			for _, name := range taskNames {
				if name == task.Name {
					log.Println("Running " + task.Name)
					task.Fn(context.Background())
				}
			}
		}
	}
}

// Run accepts a slice of Task and connects to a locust master.
// It's a convenience function to use the defaultBoomer.
func Run(ctx context.Context, spawnChan chan *SpawnArgs) {
	if !flag.Parsed() {
		flag.Parse()
	}

	initLegacyEventHandlers()

	defaultBoomer.masterHost = masterHost
	defaultBoomer.masterPort = masterPort
	defaultBoomer.EnableMemoryProfile(memoryProfileFile, memoryProfileDuration)
	defaultBoomer.EnableCPUProfile(cpuProfileFile, cpuProfileDuration)

	defaultBoomer.Run(ctx, spawnChan)

	quitByMe := false
	quitChan := make(chan bool)

	Events.SubscribeOnce(EVENT_QUIT, func() {
		if !quitByMe {
			close(quitChan)
		}
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-c:
		quitByMe = true
		defaultBoomer.Quit()
	case <-quitChan:
	}

	log.Println("shut down")
}

// RecordSuccess reports a success.
// It's a convenience function to use the defaultBoomer.
func RecordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	defaultBoomer.RecordSuccess(requestType, name, responseTime, responseLength)
}

// RecordFailure reports a failure.
// It's a convenience function to use the defaultBoomer.
func RecordFailure(requestType, name string, responseTime int64, exception string) {
	defaultBoomer.RecordFailure(requestType, name, responseTime, exception)
}
