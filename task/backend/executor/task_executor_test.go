package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/scheduler"
	"go.uber.org/zap/zaptest"
)

type tes struct {
	svc     *fakeQueryService
	ex      *TaskExecutor
	metrics *ExecutorMetrics
	i       *kv.Service
	tc      testCreds
}

func taskExecutorSystem(t *testing.T) tes {
	aqs := newFakeQueryService()
	qs := query.QueryServiceBridge{
		AsyncQueryService: aqs,
	}

	i := kv.NewService(zaptest.NewLogger(t), inmem.NewKVStore())

	ex, metrics := NewExecutor(zaptest.NewLogger(t), qs, i, i, taskControlService{i})
	return tes{
		svc:     aqs,
		ex:      ex,
		metrics: metrics,
		i:       i,
		tc:      createCreds(t, i),
	}
}

func TestTaskExecutor(t *testing.T) {
	t.Run("QuerySuccess", testQuerySuccess)
	t.Run("QueryFailure", testQueryFailure)
	t.Run("ManualRun", testManualRun)
	t.Run("ResumeRun", testResumingRun)
	t.Run("WorkerLimit", testWorkerLimit)
	t.Run("LimitFunc", testLimitFunc)
	t.Run("Metrics", testMetrics)
	t.Run("IteratorFailure", testIteratorFailure)
	t.Run("ErrorHandling", testErrorHandling)
}

func testQuerySuccess(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := influxdb.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	if run.RunAt != time.Unix(126, 0).UTC() {
		t.Fatalf("did not correctly set RunAt value, got: %v", run.RunAt)
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	<-promise.Done()

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}
	// confirm run is removed from in-mem store
	run, err = tes.i.FindRunByID(context.Background(), task.ID, run.ID)
	if run != nil || err == nil || !strings.Contains(err.Error(), "run not found") {
		t.Fatal("run was returned when it should have been removed from kv")
	}

}

func testQueryFailure(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := influxdb.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.FailQuery(script, errors.New("blargyblargblarg"))

	<-promise.Done()

	if got := promise.Error(); got == nil {
		t.Fatal("got no error when I should have")
	}
}

func testManualRun(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	manualRun, err := tes.i.ForceRun(ctx, task.ID, 123)
	if err != nil {
		t.Fatal(err)
	}

	mrs, err := tes.i.ManualRuns(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(mrs) != 1 {
		t.Fatal("manual run not created by force run")
	}

	promise, err := tes.ex.ManualRun(ctx, task.ID, manualRun.ID)
	if err != nil {
		t.Fatal(err)
	}

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promise.ID())
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promise.ID() || manualRun.ID != promise.ID() {
		t.Fatal("promise and run and manual run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}
}

func testResumingRun(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	stalledRun, err := tes.i.CreateRun(ctx, task.ID, time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.ResumeCurrentRun(ctx, task.ID, stalledRun.ID)
	if err != nil {
		t.Fatal(err)
	}

	// ensure that it doesn't recreate a promise
	if _, err := tes.ex.ResumeCurrentRun(ctx, task.ID, stalledRun.ID); err != influxdb.ErrRunNotFound {
		t.Fatal("failed to error when run has already been resumed")
	}

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promise.ID())
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promise.ID() || stalledRun.ID != promise.ID() {
		t.Fatal("promise and run and manual run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}
}

func testWorkerLimit(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	if len(tes.ex.workerLimit) != 1 {
		t.Fatal("expected a worker to be started")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.FailQuery(script, errors.New("blargyblargblarg"))

	<-promise.Done()

	if got := promise.Error(); got == nil {
		t.Fatal("got no error when I should have")
	}
}

func testLimitFunc(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}
	forcedErr := errors.New("forced")
	forcedQueryErr := influxdb.ErrQueryError(forcedErr)
	tes.svc.FailNextQuery(forcedErr)

	count := 0
	tes.ex.SetLimitFunc(func(*influxdb.Task, *influxdb.Run) error {
		count++
		if count < 2 {
			return errors.New("not there yet")
		}
		return nil
	})

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	<-promise.Done()

	if got := promise.Error(); got.Error() != forcedQueryErr.Error() {
		t.Fatal("failed to get failure from forced error")
	}

	if count != 2 {
		t.Fatalf("failed to call limitFunc enough times: %d", count)
	}
}

func testMetrics(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)
	metrics := tes.metrics
	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(metrics.PrometheusCollectors()...)

	mg := promtest.MustGather(t, reg)
	m := promtest.MustFindMetric(t, mg, "task_executor_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 0 {
		t.Fatalf("expected 0 total runs active, got %v", got)
	}

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := influxdb.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)

	mg = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mg, "task_executor_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 1 {
		t.Fatalf("expected 1 total runs active, got %v", got)
	}

	tes.svc.SucceedQuery(script)
	<-promise.Done()

	mg = promtest.MustGather(t, reg)

	m = promtest.MustFindMetric(t, mg, "task_executor_total_runs_complete", map[string]string{"task_type": "", "status": "success"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 active runs, got %v", got)
	}
	m = promtest.MustFindMetric(t, mg, "task_executor_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 0 {
		t.Fatalf("expected 0 total runs active, got %v", got)
	}

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}

	// manual runs metrics
	mt, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	scheduledFor := int64(123)

	r, err := tes.i.ForceRun(ctx, mt.ID, scheduledFor)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tes.ex.ManualRun(ctx, mt.ID, r.ID)
	if err != nil {
		t.Fatal(err)
	}

	mg = promtest.MustGather(t, reg)

	m = promtest.MustFindMetric(t, mg, "task_executor_manual_runs_counter", map[string]string{"taskID": string(mt.ID.String())})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 manual run, got %v", got)
	}

	m = promtest.MustFindMetric(t, mg, "task_executor_run_latency_seconds", map[string]string{"task_type": ""})
	if got := *m.Histogram.SampleCount; got < 1 {
		t.Fatal("expected to find run latency metric")
	}

	if got := *m.Histogram.SampleSum; got <= 100 {
		t.Fatalf("expected run latency metric to be very large, got %v", got)
	}

}

func testIteratorFailure(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	// replace iterator exhaust function with one which errors
	tes.ex.workerPool = sync.Pool{New: func() interface{} {
		return &worker{tes.ex, func(flux.Result) error {
			return errors.New("something went wrong exhausting iterator")
		}}
	}}

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := influxdb.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	<-promise.Done()

	if got := promise.Error(); got == nil {
		t.Fatal("got no error when I should have")
	}
}

func testErrorHandling(t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(t)

	metrics := tes.metrics
	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(metrics.PrometheusCollectors()...)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, influxdb.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script, Status: "active"})
	if err != nil {
		t.Fatal(err)
	}

	// encountering a bucket not found error should log an unrecoverable error in the metrics
	forcedErr := errors.New("could not find bucket")
	tes.svc.FailNextQuery(forcedErr)

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	<-promise.Done()

	mg := promtest.MustGather(t, reg)

	m := promtest.MustFindMetric(t, mg, "task_executor_unrecoverable_counter", map[string]string{"taskID": task.ID.String(), "errorType": "internal error"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 unrecoverable error, got %v", got)
	}

	// TODO (al): once user notification system is put in place, this code should be uncommented
	// encountering a bucket not found error should deactivate the task
	/*
		inactive, err := tes.i.FindTaskByID(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}

		if inactive.Status != "inactive" {
			t.Fatal("expected task to be deactivated after permanent error")
		}
	*/
}

type taskControlService struct {
	backend.TaskControlService
}

func (t taskControlService) FinishRun(ctx context.Context, taskID influxdb.ID, runID influxdb.ID) (*influxdb.Run, error) {
	// ensure auth set on context
	_, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		panic(err)
	}

	return t.TaskControlService.FinishRun(ctx, taskID, runID)
}
