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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEdgeClient(t *testing.T) {
	config := EdgeClientConfig{
		BaseURL:        "http://localhost:8080",
		RetryAttempts:  3,
		RetryWaitMin:   1 * time.Second,
		RetryWaitMax:   5 * time.Second,
		RequestTimeout: 30 * time.Second,
	}

	client := NewEdgeClient(config)
	assert.NotNil(t, client)

	// Verify it implements the interface
	var _ EdgeClientInterface = client
}

func TestTaskInstanceKey(t *testing.T) {
	job := EdgeJobBase{
		DagID:     "test_dag",
		TaskID:    "test_task",
		RunID:     "test_run",
		TryNumber: 1,
		MapIndex:  -1,
	}

	key := job.Key()
	assert.Equal(t, "test_dag", key.DagID)
	assert.Equal(t, "test_task", key.TaskID)
	assert.Equal(t, "test_run", key.RunID)
	assert.Equal(t, 1, key.TryNumber)
	assert.Equal(t, -1, key.MapIndex)
}

func TestEdgeWorkerStates(t *testing.T) {
	// Test that all worker states are defined
	states := []EdgeWorkerState{
		EdgeWorkerStateIdle,
		EdgeWorkerStateRunning,
		EdgeWorkerStateTerminating,
		EdgeWorkerStateOffline,
		EdgeWorkerStateMaintenanceMode,
		EdgeWorkerStateMaintenancePending,
		EdgeWorkerStateOfflineMaintenance,
	}

	for _, state := range states {
		assert.NotEmpty(t, string(state))
	}
}

func TestTaskStates(t *testing.T) {
	// Test that all task states are defined
	states := []TaskState{
		TaskStateQueued,
		TaskStateRunning,
		TaskStateSuccess,
		TaskStateFailed,
		TaskStateUpstream,
		TaskStateSkipped,
		TaskStateUpForRetry,
		TaskStateDeferred,
	}

	for _, state := range states {
		assert.NotEmpty(t, string(state))
	}
}

func TestWorkerStateBody(t *testing.T) {
	sysinfo := map[string]any{
		"concurrency":      4,
		"free_concurrency": 3,
		"airflow_version":  "3.0.0",
	}

	body := WorkerStateBody{
		State:      EdgeWorkerStateIdle,
		JobsActive: 0,
		Queues:     []string{"default", "high_priority"},
		SysInfo:    sysinfo,
	}

	assert.Equal(t, EdgeWorkerStateIdle, body.State)
	assert.Equal(t, 0, body.JobsActive)
	assert.Equal(t, []string{"default", "high_priority"}, body.Queues)
	assert.Equal(t, sysinfo, body.SysInfo)
	assert.Nil(t, body.MaintenanceComments)
}

// Mock test to verify client methods can be called
// This won't actually make HTTP requests but ensures the interface works
func TestEdgeClientInterface(t *testing.T) {
	config := EdgeClientConfig{
		BaseURL: "http://localhost:8080",
	}

	client := NewEdgeClient(config)
	ctx := context.Background()

	// Test that methods exist and can be called (they will fail due to no server, but that's expected)
	_, err := client.RegisterWorker(ctx, "test-worker", EdgeWorkerStateIdle, []string{"default"}, map[string]any{})
	assert.Error(t, err) // Expected to fail since no server is running

	_, err = client.FetchJob(ctx, "test-worker", []string{"default"}, 1)
	assert.Error(t, err) // Expected to fail since no server is running

	key := TaskInstanceKey{
		DagID:     "test_dag",
		TaskID:    "test_task",
		RunID:     "test_run",
		TryNumber: 1,
		MapIndex:  -1,
	}

	err = client.SetJobState(ctx, key, TaskStateRunning)
	assert.Error(t, err) // Expected to fail since no server is running
}
