// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package api

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"
)

// EdgeWorkerIntegration shows how to integrate the Edge API client with the worker
type EdgeWorkerIntegration struct {
	edgeClient EdgeClientInterface
	hostname   string
	queues     []string

	// Worker state
	concurrency     int
	freeConcurrency int
	activeJobs      map[TaskInstanceKey]*EdgeJobFetched
}

// NewEdgeWorkerIntegration creates a new worker integration with Edge API
func NewEdgeWorkerIntegration(edgeAPIURL string, hostname string, queues []string, concurrency int) (*EdgeWorkerIntegration, error) {
	config := EdgeClientConfig{
		BaseURL:        edgeAPIURL,
		RetryAttempts:  10,
		RetryWaitMin:   1 * time.Second,
		RetryWaitMax:   90 * time.Second,
		RequestTimeout: 30 * time.Second,
	}

	edgeClient := NewEdgeClient(config)

	return &EdgeWorkerIntegration{
		edgeClient:      edgeClient,
		hostname:        hostname,
		queues:          queues,
		concurrency:     concurrency,
		freeConcurrency: concurrency,
		activeJobs:      make(map[TaskInstanceKey]*EdgeJobFetched),
	}, nil
}

// Start starts the Edge worker lifecycle
func (w *EdgeWorkerIntegration) Start(ctx context.Context) error {
	// 1. Register worker with Edge API
	if err := w.registerWorker(ctx); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	// 2. Start main worker loop
	return w.workerLoop(ctx)
}

// registerWorker registers this worker with the Edge API
func (w *EdgeWorkerIntegration) registerWorker(ctx context.Context) error {
	sysinfo := w.getSysInfo()

	_, err := w.edgeClient.RegisterWorker(
		ctx,
		w.hostname,
		EdgeWorkerStateIdle,
		w.queues,
		sysinfo,
	)

	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	fmt.Printf("Worker %s registered successfully with Edge API\n", w.hostname)
	return nil
}

// workerLoop is the main worker loop that fetches and executes jobs
func (w *EdgeWorkerIntegration) workerLoop(ctx context.Context) error {
	heartbeatTicker := time.NewTicker(5 * time.Second)
	jobFetchTicker := time.NewTicker(1 * time.Second)

	defer heartbeatTicker.Stop()
	defer jobFetchTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return w.shutdown(ctx)

		case <-heartbeatTicker.C:
			if err := w.sendHeartbeat(ctx); err != nil {
				fmt.Printf("Heartbeat failed: %v\n", err)
			}

		case <-jobFetchTicker.C:
			if w.freeConcurrency > 0 {
				if err := w.fetchAndExecuteJob(ctx); err != nil {
					fmt.Printf("Job fetch/execute failed: %v\n", err)
				}
			}
		}
	}
}

// sendHeartbeat sends a heartbeat to the Edge API
func (w *EdgeWorkerIntegration) sendHeartbeat(ctx context.Context) error {
	state := w.getCurrentState()
	sysinfo := w.getSysInfo()

	_, err := w.edgeClient.SetWorkerState(
		ctx,
		w.hostname,
		state,
		len(w.activeJobs),
		w.queues,
		sysinfo,
		nil, // no maintenance comments
	)

	return err
}

// fetchAndExecuteJob fetches a job from Edge API and executes it
func (w *EdgeWorkerIntegration) fetchAndExecuteJob(ctx context.Context) error {
	// Fetch job from Edge API
	job, err := w.edgeClient.FetchJob(ctx, w.hostname, w.queues, w.freeConcurrency)
	if err != nil {
		return fmt.Errorf("failed to fetch job: %w", err)
	}

	if job == nil {
		// No jobs available
		return nil
	}

	fmt.Printf("Fetched job: %s/%s/%s\n", job.DagID, job.TaskID, job.RunID)

	// Update worker state
	w.freeConcurrency--
	key := job.Key()
	w.activeJobs[key] = job

	// Execute job asynchronously
	go w.executeJob(ctx, job)

	return nil
}

// executeJob executes a single job
func (w *EdgeWorkerIntegration) executeJob(ctx context.Context, job *EdgeJobFetched) {
	key := job.Key()

	defer func() {
		// Clean up when job finishes
		delete(w.activeJobs, key)
		w.freeConcurrency++
	}()

	// Report job as running
	if err := w.edgeClient.SetJobState(ctx, key, TaskStateRunning); err != nil {
		fmt.Printf("Failed to set job state to running: %v\n", err)
		return
	}

	fmt.Printf("Executing job: %s/%s/%s\n", job.DagID, job.TaskID, job.RunID)

	// TODO: This is where you would integrate with the existing worker.ExecuteTaskWorkload
	// For now, simulate job execution
	time.Sleep(2 * time.Second)

	// Report job as successful
	if err := w.edgeClient.SetJobState(ctx, key, TaskStateSuccess); err != nil {
		fmt.Printf("Failed to set job state to success: %v\n", err)
		return
	}

	fmt.Printf("Job completed: %s/%s/%s\n", job.DagID, job.TaskID, job.RunID)
}

// getCurrentState returns the current state of the worker
func (w *EdgeWorkerIntegration) getCurrentState() EdgeWorkerState {
	if len(w.activeJobs) > 0 {
		return EdgeWorkerStateRunning
	}
	return EdgeWorkerStateIdle
}

// getSysInfo returns system information about the worker
func (w *EdgeWorkerIntegration) getSysInfo() map[string]any {
	return map[string]any{
		"concurrency":      w.concurrency,
		"free_concurrency": w.freeConcurrency,
		"active_jobs":      len(w.activeJobs),
		"go_version":       runtime.Version(),
		"os":               runtime.GOOS,
		"arch":             runtime.GOARCH,
	}
}

// shutdown gracefully shuts down the worker
func (w *EdgeWorkerIntegration) shutdown(ctx context.Context) error {
	fmt.Printf("Shutting down worker %s...\n", w.hostname)

	// Wait for active jobs to complete (with timeout)
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for len(w.activeJobs) > 0 {
		select {
		case <-shutdownCtx.Done():
			fmt.Printf("Shutdown timeout reached, %d jobs still active\n", len(w.activeJobs))
			return shutdownCtx.Err()
		case <-time.After(1 * time.Second):
			fmt.Printf("Waiting for %d active jobs to complete...\n", len(w.activeJobs))
		}
	}

	// Set worker state to offline
	_, err := w.edgeClient.SetWorkerState(
		ctx,
		w.hostname,
		EdgeWorkerStateOffline,
		0,
		w.queues,
		w.getSysInfo(),
		nil,
	)

	if err != nil {
		fmt.Printf("Failed to set worker offline: %v\n", err)
	}

	fmt.Printf("Worker %s shut down successfully\n", w.hostname)
	return nil
}

// Example usage function
func ExampleEdgeWorkerUsage() {
	// This shows how to use the Edge worker integration
	ctx := context.Background()

	hostname, _ := os.Hostname()
	queues := []string{"default", "high_priority"}
	concurrency := 4

	worker, err := NewEdgeWorkerIntegration(
		"http://localhost:8080/edge", // Edge API URL
		hostname,
		queues,
		concurrency,
	)
	if err != nil {
		fmt.Printf("Failed to create worker: %v\n", err)
		return
	}

	// Start the worker (this blocks)
	if err := worker.Start(ctx); err != nil {
		fmt.Printf("Worker failed: %v\n", err)
	}
}
