#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Test script to verify Task SDK client can connect to Airflow API server."""

import os
import time
import pytest
import requests

# Constants
API_BASE_URL = "http://localhost:28080/execution"  # Default API server port
RETRY_ATTEMPTS = 30
RETRY_DELAY = 1  # seconds

@pytest.fixture
def client():
    """Create a Task SDK API client."""
    from airflow.sdk.api.client import Client
    return Client(
        base_url=API_BASE_URL,
        token="test-token"  # We don't need auth for health endpoint
    )

def test_api_connectivity(client):
    """Test that we can connect to the API server's health endpoint."""
    # Retry logic for API server to become ready
    last_error = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            # Try to connect to the health/ping endpoint
            response = client.get("/health/ping")
            assert response.status_code == 200
            print(f"Successfully connected to API server after {attempt + 1} attempts")
            return
        except Exception as e:
            last_error = e
            print(f"Attempt {attempt + 1}/{RETRY_ATTEMPTS} failed: {str(e)}")
            time.sleep(RETRY_DELAY)

    raise Exception(f"Failed to connect to API server after {RETRY_ATTEMPTS} attempts. Last error: {str(last_error)}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
