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

import json
import subprocess
import shlex
import webbrowser


def prompt_user() -> str:
    choices = "ynw?"
    while True:
        try:
            user_input = input(f"Make a selection [{','.join(choices)}]: ")
        except (KeyboardInterrupt, EOFError):
            exit(0)
        clean_input = user_input.strip().lower()
        if clean_input == "?":
            print("\ny: approve the run\nn: do nothing\nw: open the URL for this run")
            continue
        if clean_input not in choices:
            print(f"\nInvalid selection '{clean_input}'. Valid choices are:")
            print("y: approve the run\nn: do nothing\nw: open the URL for this run")
            continue
        else:
            return clean_input


def run_gh_command(cmd: str) -> str:
    "Run a gh CLI command and return the stdout"
    p = subprocess.run(
        shlex.split(cmd),
        capture_output=True
    )
    if p.returncode == 4:
        # Not auth'd
        print(p.stderr.decode())
        exit(1)
    elif p.returncode == 0:
        return p.stdout.decode()
    else:
        print(f"Had an error running '{cmd}'.\nSTDOUT: {p.stdout.decode()}\nSTDERR: {p.stderr.decode()}")


def list_pending_workflow_runs():
    command = r"gh run list --limit 20 --repo apache/kafka --workflow CI --status action_required --json 'databaseId,headBranch'"
    out = run_gh_command(command)
    pending_workflows = json.loads(out)
    runs_by_branch = dict()
    for workflow in pending_workflows:
        run_id = workflow.get("databaseId")
        branch = workflow.get("headBranch")
        if branch in runs_by_branch:
            print(f"Ignoring run {run_id} since there is a newer one: {runs_by_branch[branch]}")
            continue
        runs_by_branch[branch] = run_id
    return runs_by_branch.values()

def load_workflow_run(run_id: str):
    command = rf"gh api --method GET -H 'Accept: application/vnd.github+json' -H 'X-GitHub-Api-Version: 2022-11-28' /repos/apache/kafka/actions/runs/{run_id}"
    out = run_gh_command(command)
    workflow_run = json.loads(out)
    return workflow_run


def approve_workflow_run(run_id: str):
    command = rf"gh api --method POST -H 'Accept: application/vnd.github+json' -H 'X-GitHub-Api-Version: 2022-11-28' /repos/apache/kafka/actions/runs/{run_id}/approve"
    out = run_gh_command(command)
    workflow_run = json.loads(out)
    return workflow_run


if __name__ == "__main__":
    """
    Interactive script to approve pending CI workflow runs. This script can only be used by committers.
    
    Requires the GitHub CLI. See https://cli.github.com/ for installation instructions. 
    
    Once installed authenticate with: gh auth login 
    """
    pending_runs = list_pending_workflow_runs()
    for run_id in pending_runs:
        run = load_workflow_run(run_id)
        branch = run.get("head_branch")
        run_id = run.get("id")
        title = run.get("display_title")
        repo = run.get("head_repository", {}).get("full_name")
        actor = run.get("actor", {}).get("login")
        url = run.get("html_url")
        updated = run.get("updated_at")
        print("-"*80)
        print(f"PR: {title}")
        print(f"Actor: {actor}")
        print(f"Branch: {repo} {branch}")
        print(f"Updated: {updated}")
        print(f"URL: {url}")
        print("")
        selection = prompt_user()
        if selection == "y":
            approve_workflow_run(run_id)
        elif selection == "n":
            continue
        elif selection == "w":
            webbrowser.open(url)
