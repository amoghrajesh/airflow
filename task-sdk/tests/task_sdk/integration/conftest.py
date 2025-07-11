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

import os

import pytest
import requests
from airflow.sdk.api.client import Client

# Configuration for the Airflow API server
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")


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


@pytest.fixture
def airflow_api() -> Client:
    """Fixture that provides an authenticated Task SDK client."""
    access_token = get_auth_token()
    return Client(host=AIRFLOW_HOST, access_token=access_token)
