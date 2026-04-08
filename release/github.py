#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Auxiliary functions to interact with the GitHub REST API.

Set the GITHUB_REPO environment variable to override the target repository
(e.g. "myuser/kafka" to test against a personal fork).

Set GITHUB_DRY_RUN=true to print API calls without executing them.
"""

import json
import os
import urllib.request

from runtime import fail

GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = os.environ.get("GITHUB_REPO", "apache/kafka")
DRY_RUN = os.environ.get("GITHUB_DRY_RUN", "").lower() in ("true", "1", "yes")


def _api_request(token, method, path, body=None):
    """
    Make an authenticated request to the GitHub REST API.
    In dry-run mode, prints the request details without executing.
    """
    url = f"{GITHUB_API_URL}{path}"

    if DRY_RUN:
        print(f"[DRY RUN] {method} {url}")
        if body:
            print(f"[DRY RUN] Body: {json.dumps(body, indent=2)}")
        return None

    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github.v3+json")
    req.add_header("Authorization", f"token {token}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            if resp.status == 204:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        fail(f"GitHub API error {e.code} for {method} {path}: {error_body}")


def trigger_workflow(token, workflow_file, ref, inputs):
    """
    Trigger a GitHub Actions workflow_dispatch event.
    Returns None on success (HTTP 204).
    """
    path = f"/repos/{GITHUB_REPO}/actions/workflows/{workflow_file}/dispatches"
    body = {"ref": ref, "inputs": inputs}
    print(f"Triggering workflow {workflow_file} on {GITHUB_REPO} with inputs: {json.dumps(inputs)}")
    _api_request(token, "POST", path, body)
    print(f"Successfully triggered {workflow_file}")


def trigger_docker_build_test(token, ref, image_type, kafka_url):
    """
    Trigger the Docker Build Test workflow for the given image type.
    """
    print(f"\n--- Docker Build Test ({image_type}) ---")
    print(f"  Image type : {image_type}")
    print(f"  Branch/ref : {ref}")
    print(f"  Kafka URL  : {kafka_url}")
    trigger_workflow(token, "docker_build_and_test.yml", ref, {
        "image_type": image_type,
        "kafka_url": kafka_url,
    })


def trigger_docker_rc_release(token, ref, image_type, rc_docker_image, kafka_url):
    """
    Trigger the Docker RC Release workflow for the given image type.
    """
    print(f"\n--- Docker RC Release ({image_type}) ---")
    print(f"  Image type   : {image_type}")
    print(f"  Docker image : {rc_docker_image}")
    print(f"  Branch/ref   : {ref}")
    print(f"  Kafka URL    : {kafka_url}")
    trigger_workflow(token, "docker_rc_release.yml", ref, {
        "image_type": image_type,
        "rc_docker_image": rc_docker_image,
        "kafka_url": kafka_url,
    })
