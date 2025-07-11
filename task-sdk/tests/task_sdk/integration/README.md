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

# Task SDK Integration Tests

This directory contains integration tests for the Apache Airflow Task SDK. The tests verify that the Task SDK can interact correctly with a running Airflow API server.

## Prerequisites

1. A running Airflow API server (using Breeze docker-compose tests)
2. Python 3.10 or later
3. Virtual environment with Task SDK installed

## Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start the Airflow API server using Breeze:
   ```bash
   breeze testing docker-compose-tests --skip-docker-compose-deletion
   ```

## Running the Tests

1. With the API server running, execute:
   ```bash
   python test_api_connectivity.py
   ```

   Or using pytest directly:
   ```bash
   pytest test_api_connectivity.py -v
   ```

## Test Structure

- `test_api_connectivity.py`: Tests basic connectivity to the Airflow API server's health endpoint
- More tests will be added for XCom operations, task instance context, etc.

## Troubleshooting

1. If the tests fail to connect to the API server:
   - Make sure the Breeze docker-compose environment is running
   - Check if the API server port (28080) is correct and accessible
   - Verify there are no conflicting services on the same port
   - Check the API server logs using Breeze
