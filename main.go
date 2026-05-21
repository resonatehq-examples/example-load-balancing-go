// Package main demonstrates service discovery and load balancing across
// multiple Resonate workers. Three worker instances register the same
// function under the same worker group. A client dispatches N tasks via
// r.RPC targeting the group, and the Resonate server distributes tasks
// across available workers.
//
// This example requires a real Resonate server (resonate dev). The -url
// flag is mandatory. See the README for why localnet cannot demonstrate
// this pattern.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"
)

// WorkArgs carries the task identifier sent from client to worker.
type WorkArgs struct {
	TaskName string `json:"taskName"`
}

// computeSomething simulates a unit of work. It records which worker
// handled the task so the client can print the distribution.
func computeSomething(workerID string) func(_ *resonate.Context, args WorkArgs) (string, error) {
	return func(_ *resonate.Context, args WorkArgs) (string, error) {
		// Simulate a small amount of work.
		time.Sleep(50 * time.Millisecond)
		result := fmt.Sprintf("worker-%s handled %s", workerID, args.TaskName)
		fmt.Printf("[worker-%s] handling %s → done\n", workerID, args.TaskName)
		return result, nil
	}
}

func main() {
	url := flag.String("url", "", "Resonate server URL (required, e.g. http://localhost:8001)")
	workers := flag.Int("workers", 3, "number of worker instances to start")
	tasks := flag.Int("tasks", 6, "number of tasks to dispatch")
	group := flag.String("group", "workers", "worker group name")
	flag.Parse()

	if *url == "" {
		log.Fatal("error: -url is required. Start the Resonate server with `resonate dev` and pass -url=http://localhost:8001")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── Spawn N worker instances ────────────────────────────────────────────
	// Each worker gets a unique PID but registers under the same group.
	// The Resonate server tracks all PIDs polling that group's SSE endpoint
	// and routes `poll://any@<group>` tasks across them.

	workerInstances := make([]*resonate.Resonate, *workers)
	var startWg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		i := i
		workerID := fmt.Sprintf("%d", i+1)
		pid := fmt.Sprintf("worker-%s-%d", workerID, time.Now().UnixNano())

		r, err := resonate.New(resonate.Config{
			Network: httpnet.NewHTTP(*url, httpnet.HTTPOptions{
				PID:   pid,
				Group: *group,
			}),
		})
		if err != nil {
			log.Fatalf("resonate.New (worker-%s): %v", workerID, err)
		}
		workerInstances[i] = r

		// Register the compute function with a closure that captures the worker ID.
		if _, err := resonate.Register(r, "computeSomething", computeSomething(workerID)); err != nil {
			log.Fatalf("Register (worker-%s): %v", workerID, err)
		}

		startWg.Add(1)
		go func(id string) {
			defer startWg.Done()
			fmt.Printf("[worker-%s] started (pid=%s, group=%s)\n", id, pid, *group)
		}(workerID)
	}

	startWg.Wait()

	// Give the workers a moment to establish SSE connections before dispatching.
	time.Sleep(300 * time.Millisecond)

	// ── Client: dispatch M tasks ────────────────────────────────────────────
	// The client uses its own Resonate instance with a distinct PID and group.
	// It dispatches via r.RPC with Target set to poll://any@<group>, which tells
	// the server to route each task to any available worker in that group.

	clientPID := fmt.Sprintf("client-%d", time.Now().UnixNano())
	client, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(*url, httpnet.HTTPOptions{
			PID:   clientPID,
			Group: "client",
		}),
	})
	if err != nil {
		log.Fatalf("resonate.New (client): %v", err)
	}
	defer func() { _ = client.Stop() }()

	// Build the anycast target address for the worker group.
	target := fmt.Sprintf("poll://any@%s", *group)

	runID := fmt.Sprintf("lb-demo-%d", time.Now().UnixNano())

	type dispatchResult struct {
		taskName string
		result   string
		err      error
	}

	results := make(chan dispatchResult, *tasks)

	for i := 0; i < *tasks; i++ {
		taskName := fmt.Sprintf("task-%d", i)
		fmt.Printf("[client] dispatching %s\n", taskName)

		id := fmt.Sprintf("%s-%s", runID, taskName)
		h, err := client.RPC(ctx, id, "computeSomething", WorkArgs{TaskName: taskName},
			resonate.RPCOptions{Target: target},
		)
		if err != nil {
			log.Printf("[client] RPC error for %s: %v", taskName, err)
			results <- dispatchResult{taskName: taskName, err: err}
			continue
		}

		go func(name string, handle *resonate.Handle) {
			var out string
			err := handle.Result(ctx, &out)
			results <- dispatchResult{taskName: name, result: out, err: err}
		}(taskName, h)
	}

	// ── Collect results ─────────────────────────────────────────────────────
	fmt.Printf("\n[client] waiting for %d tasks to complete...\n\n", *tasks)

	succeeded := 0
	failed := 0
	for i := 0; i < *tasks; i++ {
		r := <-results
		if r.err != nil {
			fmt.Printf("[client] %s FAILED: %v\n", r.taskName, r.err)
			failed++
		} else {
			fmt.Printf("[client] %s → %q\n", r.taskName, r.result)
			succeeded++
		}
	}

	// ── Shutdown ────────────────────────────────────────────────────────────
	for i, r := range workerInstances {
		if err := r.Stop(); err != nil {
			log.Printf("Stop worker-%d: %v", i+1, err)
		}
	}

	fmt.Printf("\nDone. %d/%d tasks completed successfully.\n", succeeded, *tasks)
}
