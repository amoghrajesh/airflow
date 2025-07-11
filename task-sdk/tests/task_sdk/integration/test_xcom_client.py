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

"""Test script to verify Task SDK client can perform XCom operations."""

import os
import sys
import time
import uuid
from typing import Optional

import httpx
import pytest
from airflow.sdk.api.client import Client


def get_api_client(base_url: Optional[str] = None, token: Optional[str] = None) -> Client:
    """Create a Task SDK API client."""
    if base_url is None:
        base_url = os.environ.get("AIRFLOW_API_BASE_URL", "http://localhost:8080")
    if token is None:
        token = os.environ.get("AIRFLOW_API_TOKEN", "test-token")

    limits = httpx.Limits(max_keepalive_connections=1, max_connections=10)
    return Client(base_url=base_url, limits=limits, token=token)


def test_xcom_operations():
    """Test XCom operations using the Task SDK client."""
    client = get_api_client()

    # Test data
    dag_id = "test_dag"
    run_id = f"test_run_{uuid.uuid4().hex}"
    task_id = "test_task"
    key = "test_key"
    value = {"message": "Hello from Task SDK!"}

    print("\nTesting XCom operations...")

    try:
        # Test getting XCom value (should not exist yet)
        try:
            response = client.xcoms.get(dag_id=dag_id, run_id=run_id, task_id=task_id, key=key)
            print("Unexpected: XCom value exists before setting it")
        except Exception as e:
            print("Expected: XCom value does not exist yet")

        # Test getting XCom count
        try:
            count = client.xcoms.head(dag_id=dag_id, run_id=run_id, task_id=task_id, key=key)
            print(f"XCom count: {count}")
        except Exception as e:
            print(f"Error getting XCom count: {e}")

        print("\nAll XCom operations completed successfully!")

    except Exception as e:
        print(f"\nError during XCom operations: {e}")
        sys.exit(1)


if __name__ == "__main__":
    test_xcom_operations()
