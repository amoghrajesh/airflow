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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"resty.dev/v3"
)

// EdgeClientInterface defines the interface for interacting with the Edge API
type EdgeClientInterface interface {
	// Worker lifecycle management
	RegisterWorker(ctx context.Context, hostname string, state EdgeWorkerState, queues []string, sysinfo map[string]any) (*WorkerRegistrationReturn, error)
	SetWorkerState(ctx context.Context, hostname string, state EdgeWorkerState, jobsActive int, queues []string, sysinfo map[string]any, maintenanceComments *string) (*WorkerSetStateReturn, error)

	// Job management
	FetchJob(ctx context.Context, hostname string, queues []string, freeConcurrency int) (*EdgeJobFetched, error)
	SetJobState(ctx context.Context, key TaskInstanceKey, state TaskState) error

	// Logging
	GetLogFilePath(ctx context.Context, key TaskInstanceKey) (string, error)
	PushLogs(ctx context.Context, key TaskInstanceKey, logChunkTime time.Time, logChunkData string) error
}

// EdgeClient implements the EdgeClientInterface
type EdgeClient struct {
	client  *resty.Client
	baseURL string
}

// EdgeClientConfig holds configuration for the Edge API client
type EdgeClientConfig struct {
	BaseURL           string
	JWTSecret         string
	JWTValidFor       time.Duration
	RetryAttempts     int
	RetryWaitMin      time.Duration
	RetryWaitMax      time.Duration
	RequestTimeout    time.Duration
}

// NewEdgeClient creates a new Edge API client
func NewEdgeClient(config EdgeClientConfig) EdgeClientInterface {
	client := resty.New()
	client.SetBaseURL(config.BaseURL)
	client.SetHeader("Content-Type", "application/json")
	client.SetHeader("Accept", "application/json")

	// Set timeout
	if config.RequestTimeout > 0 {
		client.SetTimeout(config.RequestTimeout)
	} else {
		client.SetTimeout(30 * time.Second) // Default timeout
	}

	// Set retry configuration
	if config.RetryAttempts > 0 {
		client.SetRetryCount(config.RetryAttempts)
		if config.RetryWaitMin > 0 && config.RetryWaitMax > 0 {
			client.SetRetryWaitTime(config.RetryWaitMin)
			client.SetRetryMaxWaitTime(config.RetryWaitMax)
		}
	}

	// Add retry conditions (if supported by the resty version)
	// client.AddRetryCondition(func(r *resty.Response, err error) bool {
	// 	return r.StatusCode() >= 500 || err != nil
	// })

	return &EdgeClient{
		client:  client,
		baseURL: config.BaseURL,
	}
}

// makeRequest makes a generic HTTP request to the Edge API
func (c *EdgeClient) makeRequest(ctx context.Context, method, endpoint string, body interface{}) (*resty.Response, error) {
	req := c.client.R().SetContext(ctx)

	// TODO: Add JWT authentication here
	// For now, we'll assume the Edge API is configured without authentication
	// or that authentication is handled at the infrastructure level

	if body != nil {
		req.SetBody(body)
	}

	var resp *resty.Response
	var err error

	switch method {
	case "GET":
		resp, err = req.Get(endpoint)
	case "POST":
		resp, err = req.Post(endpoint)
	case "PATCH":
		resp, err = req.Patch(endpoint)
	case "PUT":
		resp, err = req.Put(endpoint)
	case "DELETE":
		resp, err = req.Delete(endpoint)
	default:
		return nil, fmt.Errorf("unsupported HTTP method: %s", method)
	}

	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode() >= 400 {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode(), resp.String())
	}

	return resp, nil
}

// RegisterWorker registers a worker with the Edge API
func (c *EdgeClient) RegisterWorker(ctx context.Context, hostname string, state EdgeWorkerState, queues []string, sysinfo map[string]any) (*WorkerRegistrationReturn, error) {
	body := WorkerStateBody{
		State:      state,
		JobsActive: 0,
		Queues:     queues,
		SysInfo:    sysinfo,
	}

	endpoint := fmt.Sprintf("/worker/%s", url.QueryEscape(hostname))
	resp, err := c.makeRequest(ctx, "POST", endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to register worker: %w", err)
	}

	var result WorkerRegistrationReturn
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

// SetWorkerState updates the state of the worker and sends a heartbeat
func (c *EdgeClient) SetWorkerState(ctx context.Context, hostname string, state EdgeWorkerState, jobsActive int, queues []string, sysinfo map[string]any, maintenanceComments *string) (*WorkerSetStateReturn, error) {
	body := WorkerStateBody{
		State:               state,
		JobsActive:          jobsActive,
		Queues:              queues,
		SysInfo:             sysinfo,
		MaintenanceComments: maintenanceComments,
	}

	endpoint := fmt.Sprintf("/worker/%s", url.QueryEscape(hostname))
	resp, err := c.makeRequest(ctx, "PATCH", endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to set worker state: %w", err)
	}

	var result WorkerSetStateReturn
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

// FetchJob fetches a job to execute on the edge worker
func (c *EdgeClient) FetchJob(ctx context.Context, hostname string, queues []string, freeConcurrency int) (*EdgeJobFetched, error) {
	body := WorkerQueuesBody{
		Queues:          queues,
		FreeConcurrency: freeConcurrency,
	}

	endpoint := fmt.Sprintf("/jobs/fetch/%s", url.QueryEscape(hostname))
	resp, err := c.makeRequest(ctx, "POST", endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch job: %w", err)
	}

	// If response is empty (204 No Content), return nil
	if resp.StatusCode() == http.StatusNoContent {
		return nil, nil
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if len(bodyBytes) == 0 {
		return nil, nil
	}

	var result EdgeJobFetched
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &result, nil
}

// SetJobState sets the state of a job
func (c *EdgeClient) SetJobState(ctx context.Context, key TaskInstanceKey, state TaskState) error {
	endpoint := fmt.Sprintf("/jobs/state/%s/%s/%s/%d/%d/%s",
		url.QueryEscape(key.DagID),
		url.QueryEscape(key.TaskID),
		url.QueryEscape(key.RunID),
		key.TryNumber,
		key.MapIndex,
		url.QueryEscape(string(state)),
	)

	_, err := c.makeRequest(ctx, "PATCH", endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to set job state: %w", err)
	}

	return nil
}

// GetLogFilePath gets the log file path for a task
func (c *EdgeClient) GetLogFilePath(ctx context.Context, key TaskInstanceKey) (string, error) {
	endpoint := fmt.Sprintf("/logs/logfile_path/%s/%s/%s/%d/%d",
		url.QueryEscape(key.DagID),
		url.QueryEscape(key.TaskID),
		url.QueryEscape(key.RunID),
		key.TryNumber,
		key.MapIndex,
	)

	resp, err := c.makeRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get log file path: %w", err)
	}

	var result string
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return result, nil
}

// PushLogs pushes an incremental log chunk from Edge Worker to central site
func (c *EdgeClient) PushLogs(ctx context.Context, key TaskInstanceKey, logChunkTime time.Time, logChunkData string) error {
	body := PushLogsBody{
		LogChunkTime: logChunkTime,
		LogChunkData: logChunkData,
	}

	endpoint := fmt.Sprintf("/logs/push/%s/%s/%s/%d/%d",
		url.QueryEscape(key.DagID),
		url.QueryEscape(key.TaskID),
		url.QueryEscape(key.RunID),
		key.TryNumber,
		key.MapIndex,
	)

	_, err := c.makeRequest(ctx, "POST", endpoint, body)
	if err != nil {
		return fmt.Errorf("failed to push logs: %w", err)
	}

	return nil
}
