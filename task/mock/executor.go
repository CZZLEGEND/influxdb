// Package mock contains mock implementations of different task interfaces.
package mock

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend/executor"
	"github.com/influxdata/influxdb/task/backend/scheduler"
)

type promise struct {
	run        *influxdb.Run
	hangingFor time.Duration

	done chan struct{}
	err  error

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// ID is the id of the run that was created
func (p *promise) ID() influxdb.ID {
	return p.run.ID
}

// Cancel is used to cancel a executing query
func (p *promise) Cancel(ctx context.Context) {
	// call cancelfunc
	p.cancelFunc()

	// wait for ctx.Done or p.Done
	select {
	case <-p.Done():
	case <-ctx.Done():
	}
}

// Done provides a channel that closes on completion of a promise
func (p *promise) Done() <-chan struct{} {
	return p.done
}

// Error returns the error resulting from a run execution.
// If the execution is not complete error waits on Done().
func (p *promise) Error() error {
	<-p.done
	return p.err
}

func (e *Executor) createPromise(ctx context.Context, run *influxdb.Run) (*promise, error) {
	ctx, cancel := context.WithCancel(ctx)
	p := &promise{
		run:        run,
		done:       make(chan struct{}),
		ctx:        ctx,
		cancelFunc: cancel,
		hangingFor: e.hangingFor,
	}

	go func() {
		time.Sleep(p.hangingFor)
		close(p.done)
	}()

	e.promiseQueue <- p
	e.currentPromises.Store(run.ID, p)
	return p, nil
}

type Executor struct {
	mu         sync.Mutex
	wg         sync.WaitGroup
	hangingFor time.Duration

	// Forced error for next call to Execute.
	nextExecuteErr error

	// currentPromises are all the promises we are made that have not been fulfilled
	currentPromises sync.Map

	// keep a pool of promise's we have in queue
	promiseQueue chan *promise

	// limitFunc LimitFunc

	currentID scheduler.ID
}

var _ scheduler.Executor = (*Executor)(nil)

func NewExecutor() *Executor {
	return &Executor{
		currentPromises: sync.Map{},
		currentID:       scheduler.ID(0),
		promiseQueue:    make(chan *promise, 1000),
		hangingFor:      time.Second,
	}
}

func (e *Executor) CurrentID() scheduler.ID {
	defer e.mu.Unlock()
	e.mu.Lock()

	return e.currentID
}
func (e *Executor) Execute(ctx context.Context, id scheduler.ID, scheduledAt time.Time, runAt time.Time) error {
	// loop until we have no more work to do in the promise queue

	e.mu.Lock()
	e.currentID = scheduler.ID(id)
	e.mu.Unlock()

	for {
		var prom *promise
		// check to see if we can execute
		select {
		case p, ok := <-e.promiseQueue:

			if !ok {
				// the promiseQueue has been closed
				return nil
			}
			prom = p
		default:
			// if nothing is left in the queue we are done
			return nil
		}

		// close promise done channel and set appropriate error
		close(prom.done)

		// remove promise from registry
		e.currentPromises.Delete(prom.run.ID)
	}
}

func (e *Executor) ManualRun(ctx context.Context, id influxdb.ID, runID influxdb.ID) (executor.Promise, error) {
	e.currentID = scheduler.ID(id)

	run := &influxdb.Run{ID: runID, TaskID: id, StartedAt: time.Now().UTC()}
	p, err := e.createPromise(ctx, run)
	return p, err
}

func (e *Executor) Wait() {
	e.wg.Wait()
}

func (e *Executor) Cancel(context.Context, influxdb.ID) error {
	return nil
}

// FailNextCallToExecute causes the next call to e.Execute to unconditionally return err.
func (e *Executor) FailNextCallToExecute(err error) {
	e.mu.Lock()
	e.nextExecuteErr = err
	e.mu.Unlock()
}
