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
	"time"
)

// EdgeWorkerState represents the state of an edge worker
type EdgeWorkerState string

const (
	EdgeWorkerStateIdle               EdgeWorkerState = "idle"
	EdgeWorkerStateRunning            EdgeWorkerState = "running"
	EdgeWorkerStateTerminating        EdgeWorkerState = "terminating"
	EdgeWorkerStateOffline            EdgeWorkerState = "offline"
	EdgeWorkerStateMaintenanceMode    EdgeWorkerState = "maintenance_mode"
	EdgeWorkerStateMaintenancePending EdgeWorkerState = "maintenance_pending"
	EdgeWorkerStateOfflineMaintenance EdgeWorkerState = "offline_maintenance"
)

// TaskInstanceKey represents a unique identifier for a task instance
type TaskInstanceKey struct {
	DagID     string `json:"dag_id"`
	TaskID    string `json:"task_id"`
	RunID     string `json:"run_id"`
	TryNumber int    `json:"try_number"`
	MapIndex  int    `json:"map_index"`
}

// EdgeJobBase contains basic attributes of a job on the edge worker
type EdgeJobBase struct {
	DagID     string `json:"dag_id"`
	TaskID    string `json:"task_id"`
	RunID     string `json:"run_id"`
	MapIndex  int    `json:"map_index"`
	TryNumber int    `json:"try_number"`
}

// Key returns the TaskInstanceKey for this job
func (e *EdgeJobBase) Key() TaskInstanceKey {
	return TaskInstanceKey{
		DagID:     e.DagID,
		TaskID:    e.TaskID,
		RunID:     e.RunID,
		TryNumber: e.TryNumber,
		MapIndex:  e.MapIndex,
	}
}

// EdgeJobFetched represents a job that is to be executed on the edge worker
type EdgeJobFetched struct {
	EdgeJobBase
	Command           ExecuteTaskWorkload `json:"command"`
	ConcurrencySlots  int                 `json:"concurrency_slots"`
}

// WorkerQueuesBody represents queues that a worker supports to run jobs on
type WorkerQueuesBody struct {
	Queues          []string `json:"queues,omitempty"`
	FreeConcurrency int      `json:"free_concurrency"`
}

// WorkerStateBody represents details of the worker state sent to the scheduler
type WorkerStateBody struct {
	State               EdgeWorkerState    `json:"state"`
	JobsActive          int                `json:"jobs_active"`
	Queues              []string           `json:"queues,omitempty"`
	SysInfo             map[string]any     `json:"sysinfo"`
	MaintenanceComments *string            `json:"maintenance_comments,omitempty"`
}

// WorkerRegistrationReturn represents the return value for worker registration
type WorkerRegistrationReturn struct {
	LastUpdate time.Time `json:"last_update"`
}

// WorkerSetStateReturn represents the return value for worker set state
type WorkerSetStateReturn struct {
	State               EdgeWorkerState `json:"state"`
	Queues              []string        `json:"queues,omitempty"`
	MaintenanceComments *string         `json:"maintenance_comments,omitempty"`
}

// PushLogsBody represents incremental new log content from worker
type PushLogsBody struct {
	LogChunkTime time.Time `json:"log_chunk_time"`
	LogChunkData string    `json:"log_chunk_data"`
}

// TaskState represents the state of a task instance
type TaskState string

const (
	TaskStateQueued    TaskState = "queued"
	TaskStateRunning   TaskState = "running"
	TaskStateSuccess   TaskState = "success"
	TaskStateFailed    TaskState = "failed"
	TaskStateUpstream  TaskState = "upstream_failed"
	TaskStateSkipped   TaskState = "skipped"
	TaskStateUpForRetry TaskState = "up_for_retry"
	TaskStateDeferred  TaskState = "deferred"
)
