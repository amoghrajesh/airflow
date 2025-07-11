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
from __future__ import annotations

import uuid

import pytest
import requests
from airflow.sdk.api.client import Client

from .conftest import AIRFLOW_HOST


@pytest.mark.execution_timeout(400)
def test_xcom_operations_with_task_sdk(airflow_api: Client):
    """Test XCom operations using the Task SDK."""
    # Create a test DAG
    response = requests.post(
        f"{AIRFLOW_HOST}/api/v2/dags",
        json={
            "dag_id": "test_dag",
            "schedule_interval": None,
            "tags": ["test"],
            "is_paused": False,
        },
        headers={"Authorization": f"Bearer {airflow_api.access_token}"},
    )
    assert response.status_code == 200, f"Failed to create DAG: {response.status_code} {response.text}"

    # Create a test DAG run with unique ID
    dag_run_id = f"test_run_{uuid.uuid4().hex}"
    response = requests.post(
        f"{AIRFLOW_HOST}/api/v2/dags/test_dag/dagRuns",
        json={
            "dag_run_id": dag_run_id,
            "logical_date": "2025-04-07T00:00:00Z",
        },
        headers={"Authorization": f"Bearer {airflow_api.access_token}"},
    )
    assert response.status_code == 200, f"Failed to create DAG run: {response.status_code} {response.text}"

    # Create a test task instance
    response = requests.post(
        f"{AIRFLOW_HOST}/api/v2/dags/test_dag/dagRuns/{dag_run_id}/taskInstances/test_task",
        json={
            "task_id": "test_task",
            "state": "success",
        },
        headers={"Authorization": f"Bearer {airflow_api.access_token}"},
    )
    assert response.status_code == 200, f"Failed to create task instance: {response.status_code} {response.text}"

    # Create a test XCom value using the Task SDK
    airflow_api.xcoms.set(
        dag_id="test_dag",
        run_id=dag_run_id,
        task_id="test_task",
        key="test_key",
        value="test_value",
    )

    # Get the XCom value using the Task SDK
    xcom = airflow_api.xcoms.get(
        dag_id="test_dag",
        run_id=dag_run_id,
        task_id="test_task",
        key="test_key",
    )
    assert xcom.value == "test_value"

    # Delete the XCom value using the Task SDK
    airflow_api.xcoms.delete(
        dag_id="test_dag",
        run_id=dag_run_id,
        task_id="test_task",
        key="test_key",
    )

    # Verify the XCom value is deleted
    with pytest.raises(Exception):
        airflow_api.xcoms.get(
            dag_id="test_dag",
            run_id=dag_run_id,
            task_id="test_task",
            key="test_key",
        )
