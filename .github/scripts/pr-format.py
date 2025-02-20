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

import argparse
from collections import defaultdict
import json
import logging
import os
import subprocess
import shlex
import sys
import tempfile
from typing import Dict, Optional


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def get_env(key: str, fn = str) -> Optional:
    value = os.getenv(key)
    if value is None:
        logger.debug(f"Could not find env {key}")
        return None
    else:
        logger.debug(f"Read env {key}: {value}")
        return fn(value)


def has_approval(reviews_json) -> bool:
    for review in reviews:
        if review.get("authorAssociation") not in ("MEMBER", "OWNER"):
            continue
        if review.get("state") == "APPROVED":
            return True
    return False


def parse_trailers(title, body) -> Dict:
    trailers = defaultdict(list)

    with tempfile.NamedTemporaryFile() as fp:
        fp.write(title.encode())
        fp.write(b"\n")
        fp.write(body.encode())
        fp.flush()
        cmd = f"git interpret-trailers --trim-empty --parse {fp.name}"
        p = subprocess.run(shlex.split(cmd), capture_output=True)
        fp.close()

    for line in p.stdout.decode().splitlines():
        key, value = line.split(":", 1)
        trailers[key].append(value.strip())

    return trailers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify the structure of a Pull Request.")
    parser.add_argument("pull_request", type=int, help="The Pull Request number to verify.")
    parser.add_argument("--require-approval",
                        action="store_true",
                        help="If set, cause this command to fail if the PR does not have an approval.")

    if not os.getenv("GITHUB_ACTIONS"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    args = parser.parse_args()

    pr_number = args.pull_request
    cmd = f"gh pr view {pr_number} --json 'title,body,reviews'"
    p = subprocess.run(shlex.split(cmd), capture_output=True)
    if p.returncode != 0:
        logger.error(f"GitHub CLI failed with exit code {p.returncode}.\nSTDOUT: {p.stdout.decode()}\nSTDERR:{p.stderr.decode()}")
        exit(1)

    gh_json = json.loads(p.stdout)
    title = gh_json["title"]
    body = gh_json["body"]
    reviews = gh_json["reviews"]

    warnings = []
    errors = []

    # Check title
    if title.endswith("..."):
        errors.append("Title appears truncated")

    if len(title) > 120:
        errors.append("Title is too long")

    if not title.startswith("KAFKA-") and not title.startswith("MINOR") and not title.startswith("HOTFIX"):
        errors.append("Title is missing KAFKA-XXXXX or MINOR/HOTFIX prefix")

    # Check for Reviewers
    approved = has_approval(reviews)
    if not approved and args.require_approval:
        errors.append("Pull Request is not approved and --require-approvals was given")
    elif approved:
        trailers = parse_trailers(title, body)
        reviewers_in_body = trailers.get("Reviewers", [])
        if len(reviewers_in_body) > 0:
            print(f"Found 'Reviewers' in commit body")
            for reviewer_in_body in reviewers_in_body:
                print(reviewer_in_body)
        else:
            errors.append("Pull Request is approved, but no 'Reviewers' found in commit body")

    for warning in warnings:
        print(warning)

    if len(errors) > 0:
        for error in errors:
            print(error)
        cmd = f"gh pr comment {pr_number} --body 'PR format is bad'"
        p = subprocess.run(shlex.split(cmd), capture_output=True)
        print(p.stdout)
        print(p.stderr)
        exit(1)
