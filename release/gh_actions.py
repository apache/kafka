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
Auxiliary functions to interact with the GitHub REST API via PyGithub.

Set the GITHUB_REPO environment variable to override the target repository
(e.g. "myuser/kafka" to test against a personal fork).

Set GITHUB_DRY_RUN=true to print API calls without executing them.
"""

import json
import os
import time

from github import Github, GithubException

from runtime import fail

GITHUB_REPO = os.environ.get("GITHUB_REPO", "apache/kafka")
DRY_RUN = os.environ.get("GITHUB_DRY_RUN", "").lower() in ("true", "1", "yes")


def _latest_run_url(workflow, workflow_file):
    """
    Return the HTML URL of the most recent run for a workflow,
    falling back to the workflow's runs page on any error.
    """
    fallback = f"https://github.com/{GITHUB_REPO}/actions/workflows/{workflow_file}"
    try:
        runs = workflow.get_runs()
        if runs.totalCount > 0:
            return runs[0].html_url
    except GithubException:
        pass
    return fallback


def trigger_workflow(token, workflow_file, ref, inputs):
    """
    Trigger a GitHub Actions workflow_dispatch event.
    """
    print(f"Triggering {workflow_file} on {GITHUB_REPO} (ref={ref}) with inputs: {json.dumps(inputs)}")

    if DRY_RUN:
        print(f"  [DRY RUN] No API call made.")
        print(f"  View runs: https://github.com/{GITHUB_REPO}/actions/workflows/{workflow_file}")
        return

    try:
        workflow = Github(token).get_repo(GITHUB_REPO).get_workflow(workflow_file)
        if not workflow.create_dispatch(ref=ref, inputs=inputs):
            fail(f"GitHub API failed to dispatch {workflow_file}")
    except GithubException as e:
        fail(f"GitHub API error {e.status} for workflow {workflow_file}: {e.data}")

    # Brief pause to allow GitHub to register the run before querying
    time.sleep(2)
    print(f"  View run: {_latest_run_url(workflow, workflow_file)}")


def trigger_docker_build_test(token, ref, image_type, kafka_url):
    """
    Trigger the Docker Build Test workflow for the given image type.
    """
    trigger_workflow(token, "docker_build_and_test.yml", ref, {
        "image_type": image_type,
        "kafka_url": kafka_url,
    })


def trigger_docker_rc_release(token, ref, image_type, rc_docker_image, kafka_url):
    """
    Trigger the Docker RC Release workflow for the given image type.
    """
    trigger_workflow(token, "docker_rc_release.yml", ref, {
        "image_type": image_type,
        "rc_docker_image": rc_docker_image,
        "kafka_url": kafka_url,
    })
