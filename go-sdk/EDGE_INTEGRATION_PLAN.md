<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Edge API Integration Plan for Go SDK

This document outlines how to integrate the new Edge API client with the existing Go SDK worker.

## Current Architecture

```
Go Worker ←→ Celery/Redis ←→ Airflow Scheduler
```

## Target Architecture

```
Go Worker ←→ Edge API ←→ Airflow Scheduler
```

## Integration Steps

### Phase 1: ✅ COMPLETE - Edge API Client Library
- [x] Created `EdgeClientInterface` and `EdgeClient`
- [x] Implemented all Edge API endpoints:
  - Worker registration and heartbeat
  - Job fetching and state updates
  - Log file handling
- [x] Added comprehensive tests
- [x] Data models matching Python edge3 provider

### Phase 2: Worker Integration Options

#### Option A: Feature Flag Approach (Recommended)
Modify the existing worker to support both Celery and Edge API based on configuration.

```go
// In worker/runner.go
type WorkerConfig struct {
    UseEdgeAPI        bool   `yaml:"use_edge_api"`
    EdgeAPIURL        string `yaml:"edge_api_url"`
    CeleryBrokerURL   string `yaml:"celery_broker_url"`
    // ... other config
}

func (w *worker) Start(config WorkerConfig) error {
    if config.UseEdgeAPI {
        return w.startWithEdgeAPI(config)
    } else {
        return w.startWithCelery(config)
    }
}
```

#### Option B: Separate Worker Implementation
Create a new worker implementation specifically for Edge API.

### Phase 3: Detailed Integration Points

#### 3.1 Modify `worker/runner.go`

**Current job fetching (Celery):**
```go
// Current: Gets jobs from Celery queue
func (w *worker) getJobFromCelery() (*celery.Job, error) {
    // Celery-specific logic
}
```

**New job fetching (Edge API):**
```go
// New: Gets jobs from Edge API
func (w *worker) getJobFromEdgeAPI(ctx context.Context) (*api.EdgeJobFetched, error) {
    return w.edgeClient.FetchJob(ctx, w.hostname, w.queues, w.freeConcurrency)
}
```

#### 3.2 Worker Lifecycle Integration

**Registration:**
```go
func (w *worker) registerWithEdgeAPI(ctx context.Context) error {
    sysinfo := map[string]any{
        "airflow_version": "3.0.0",
        "go_sdk_version":  "1.0.0",
        "concurrency":     w.concurrency,
    }

    _, err := w.edgeClient.RegisterWorker(
        ctx, w.hostname, api.EdgeWorkerStateIdle, w.queues, sysinfo,
    )
    return err
}
```

**Heartbeat:**
```go
func (w *worker) sendEdgeHeartbeat(ctx context.Context) error {
    state := w.getCurrentEdgeState()
    sysinfo := w.getSysInfo()

    _, err := w.edgeClient.SetWorkerState(
        ctx, w.hostname, state, len(w.activeJobs), w.queues, sysinfo, nil,
    )
    return err
}
```

#### 3.3 Task Execution Integration

**Current task execution:**
```go
func (w *worker) ExecuteTaskWorkload(ctx context.Context, workload api.ExecuteTaskWorkload) error {
    // Current implementation using Task API
}
```

**Edge API integration:**
```go
func (w *worker) executeEdgeJob(ctx context.Context, edgeJob *api.EdgeJobFetched) error {
    key := edgeJob.Key()

    // Report job as running
    if err := w.edgeClient.SetJobState(ctx, key, api.TaskStateRunning); err != nil {
        return err
    }

    // Execute the actual task (reuse existing logic)
    err := w.ExecuteTaskWorkload(ctx, edgeJob.Command)

    // Report final state
    if err != nil {
        return w.edgeClient.SetJobState(ctx, key, api.TaskStateFailed)
    } else {
        return w.edgeClient.SetJobState(ctx, key, api.TaskStateSuccess)
    }
}
```

### Phase 4: Configuration Changes

#### 4.1 Add Edge API Configuration

```yaml
# config.yaml
worker:
  # Feature flag
  use_edge_api: true

  # Edge API settings
  edge_api:
    url: "http://localhost:8080/edge"
    retry_attempts: 10
    retry_wait_min: "1s"
    retry_wait_max: "90s"
    request_timeout: "30s"

  # Legacy Celery settings (when use_edge_api: false)
  celery:
    broker_url: "redis://localhost:6379/0"
    # ... other celery config

  # Common settings
  hostname: "worker-1"
  queues: ["default", "high_priority"]
  concurrency: 4
```

#### 4.2 Command Line Interface

```go
// In example/main.go or worker CLI
func main() {
    cmd := &cobra.Command{
        Use: "worker",
        Run: func(cmd *cobra.Command, args []string) {
            config := loadConfig()

            if config.UseEdgeAPI {
                runEdgeWorker(config)
            } else {
                runCeleryWorker(config)
            }
        },
    }

    cmd.Flags().Bool("use-edge-api", false, "Use Edge API instead of Celery")
    cmd.Flags().String("edge-api-url", "", "Edge API URL")
    // ... other flags
}
```

### Phase 5: Testing Strategy

#### 5.1 Unit Tests
- Test Edge API client methods
- Test worker integration logic
- Mock Edge API responses

#### 5.2 Integration Tests
- Test with actual Edge API server
- Test failover scenarios
- Test concurrent job execution

#### 5.3 Performance Tests
- Compare Edge API vs Celery performance
- Test under high load
- Measure latency and throughput

### Phase 6: Migration Path

#### 6.1 Backward Compatibility
1. Keep Celery support as default initially
2. Add Edge API as opt-in feature
3. Gradually migrate users to Edge API
4. Eventually deprecate Celery support

#### 6.2 Deployment Strategy
1. **Week 1-2**: Deploy Edge API client (feature flag off)
2. **Week 3-4**: Enable Edge API for test environments
3. **Week 5-6**: Enable Edge API for staging
4. **Week 7-8**: Enable Edge API for production (gradual rollout)
5. **Month 2**: Make Edge API the default
6. **Month 3**: Deprecate Celery support

### Phase 7: Benefits Realized

#### 7.1 Operational Benefits
- ✅ No Redis/Celery infrastructure needed
- ✅ Direct HTTP communication (easier debugging)
- ✅ Built-in retry and error handling
- ✅ Native Airflow integration

#### 7.2 Development Benefits
- ✅ Cleaner, more maintainable code
- ✅ Better observability and logging
- ✅ Standardized API across languages
- ✅ Future-proof architecture

#### 7.3 Performance Benefits
- ✅ Reduced latency (direct API calls)
- ✅ Better resource utilization
- ✅ Improved scalability

## Next Steps

1. **Immediate**: Test the Edge API client with a real Edge API server
2. **Week 1**: Implement feature flag in existing worker
3. **Week 2**: Add Edge API job fetching logic
4. **Week 3**: Integration testing
5. **Week 4**: Documentation and examples

## Files Modified/Created

### New Files
- `pkg/api/edge_models.go` - Edge API data models
- `pkg/api/edge_client.go` - Edge API client implementation
- `pkg/api/edge_client_test.go` - Tests
- `pkg/api/edge_worker_integration.go` - Integration example

### Files to Modify
- `worker/runner.go` - Add Edge API support
- `example/main.go` - Add CLI flags for Edge API
- `README.md` - Update documentation
- `go.mod` - No changes needed (all dependencies exist)

This integration maintains backward compatibility while providing a clear migration path to the more modern Edge API architecture.
