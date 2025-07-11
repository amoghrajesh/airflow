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

"""
Test script for the Task SDK client against a running Airflow API server.
This script assumes you have the Airflow API server running locally using the docker-compose quickstart.
"""

from __future__ import annotations

import os
import sys
import time
import uuid

import pytest
import requests
from airflow.sdk.api.client import Client

try:
    # If you have rich installed, you will have nice colored output
    from rich import print
except ImportError:
    print("Output will not be colored. Install rich for colored output: `pip install rich`")
    pass

# Configuration for the Airflow API server
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

# Test DAG configuration
TEST_DAG_ID = "test_dag"
TEST_TASK_ID = "test_task"


def get_auth_token() -> str:
    """Get an authentication token from the Airflow API server."""
    response = requests.post(
        f"{AIRFLOW_HOST}/api/v2/auth/token",
        json={
            "username": AIRFLOW_USER,
            "password": AIRFLOW_PASSWORD,
        },
        headers={"Content-Type": "application/json"},
    )
    if response.status_code != 201:
        raise RuntimeError(f"Failed to get auth token: {response.status_code} {response.text}")
    return response.json()["access_token"]


@pytest.mark.execution_timeout(400)
def test_task_sdk_client():
    """Test the Task SDK client against a running Airflow API server."""
    errors = False

    # Initialize the Task SDK client
    try:
        access_token = get_auth_token()
        client = Client(host=AIRFLOW_HOST, access_token=access_token)
    except Exception as e:
        print(f"[red]Failed to initialize Task SDK client: {e}")
        sys.exit(1)

    # Create a test DAG
    print("[blue]Creating test DAG")
    try:
        response = requests.post(
            f"{AIRFLOW_HOST}/api/v2/dags",
            json={
                "dag_id": TEST_DAG_ID,
                "schedule_interval": None,
                "tags": ["test"],
                "is_paused": False,
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if response.status_code != 200:
            print(f"[red]Failed to create DAG: {response.status_code} {response.text}")
            errors = True
        else:
            print("[green]Created test DAG successfully")
    except Exception as e:
        print(f"[red]Exception when creating DAG: {e}")
        errors = True

    # Create a test DAG run
    print("[blue]Creating test DAG run")
    try:
        dag_run_id = f"test_run_{uuid.uuid4().hex}"
        response = requests.post(
            f"{AIRFLOW_HOST}/api/v2/dags/{TEST_DAG_ID}/dagRuns",
            json={
                "dag_run_id": dag_run_id,
                "logical_date": "2025-04-07T00:00:00Z",
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if response.status_code != 200:
            print(f"[red]Failed to create DAG run: {response.status_code} {response.text}")
            errors = True
        else:
            print("[green]Created test DAG run successfully")
    except Exception as e:
        print(f"[red]Exception when creating DAG run: {e}")
        errors = True

    # Create a test task instance
    print("[blue]Creating test task instance")
    try:
        response = requests.post(
            f"{AIRFLOW_HOST}/api/v2/dags/{TEST_DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{TEST_TASK_ID}",
            json={
                "task_id": TEST_TASK_ID,
                "state": "success",
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if response.status_code != 200:
            print(f"[red]Failed to create task instance: {response.status_code} {response.text}")
            errors = True
        else:
            print("[green]Created test task instance successfully")
    except Exception as e:
        print(f"[red]Exception when creating task instance: {e}")
        errors = True

    # Test XCom operations using the Task SDK client
    print("[blue]Testing XCom operations")
    try:
        # Create XCom value
        client.xcoms.set(
            dag_id=TEST_DAG_ID,
            run_id=dag_run_id,
            task_id=TEST_TASK_ID,
            key="test_key",
            value="test_value",
        )
        print("[green]Created XCom value successfully")

        # Get XCom value
        xcom = client.xcoms.get(
            dag_id=TEST_DAG_ID,
            run_id=dag_run_id,
            task_id=TEST_TASK_ID,
            key="test_key",
        )
        if xcom.value != "test_value":
            print(f"[red]XCom value mismatch. Expected 'test_value', got '{xcom.value}'")
            errors = True
        else:
            print("[green]Retrieved XCom value successfully")

        # Delete XCom value
        client.xcoms.delete(
            dag_id=TEST_DAG_ID,
            run_id=dag_run_id,
            task_id=TEST_TASK_ID,
            key="test_key",
        )
        print("[green]Deleted XCom value successfully")

        # Verify deletion
        try:
            client.xcoms.get(
                dag_id=TEST_DAG_ID,
                run_id=dag_run_id,
                task_id=TEST_TASK_ID,
                key="test_key",
            )
            print("[red]XCom value still exists after deletion")
            errors = True
        except Exception:
            print("[green]Verified XCom deletion successfully")

    except Exception as e:
        print(f"[red]Exception during XCom operations: {e}")
        errors = True

    if errors:
        print("\n[red]There were errors during the test - see above for details")
        sys.exit(1)
    else:
        print("\n[green]All tests completed successfully")


if __name__ == "__main__":
    test_task_sdk_client()
