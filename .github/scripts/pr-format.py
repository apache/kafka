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


def has_approval(reviews) -> bool:
    for review in reviews:
        logger.debug(f"Review: {review}")
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
    """
    This script performs some basic linting of our PR titles and body. The PR number is read from the PR_NUMBER
    environment variable. Since this script expects to run on a GHA runner, it expects the "gh" tool to be installed.
    
    The STDOUT from this script is used as the status check message. It should not be too long. Use the logger for
    any necessary logging.
    
    Title checks:
    * Not too short (at least 15 characters)
    * Not too long (at most 120 characters)
    * Not truncated (ending with ...)
    * Starts with "KAFKA-", "MINOR", or "HOTFIX"
    
    Body checks:
    * Is not empty
    * Has "Reviewers:" trailer if the PR is approved
    """

    if not get_env("GITHUB_ACTIONS"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    pr_number = get_env("PR_NUMBER")
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

    if len(title) < 15:
        errors.append("Title is too short")

    if len(title) > 120:
        errors.append("Title is too long")

    if not title.startswith("KAFKA-") and not title.startswith("MINOR") and not title.startswith("HOTFIX"):
        errors.append("Title is missing KAFKA-XXXXX or MINOR/HOTFIX prefix")

    # Check body
    if len(body) == 0:
        errors.append("Body is empty")

    if "Delete this text and replace" in body:
        errors.append("PR template text should be removed")

    # Check for Reviewers
    approved = has_approval(reviews)
    if approved:
        trailers = parse_trailers(title, body)
        reviewers_in_body = trailers.get("Reviewers", [])
        if len(reviewers_in_body) > 0:
            logger.debug(f"Found 'Reviewers' in commit body")
            for reviewer_in_body in reviewers_in_body:
                logger.debug(reviewer_in_body)
        else:
            errors.append("Pull Request is approved, but no 'Reviewers' found in commit body")

    for warning in warnings:
        logger.debug(warning)

    if len(errors) > 0:
        for error in errors:
            logger.debug(error)
        # Just output the first error for the status message
        print(errors[0])
        exit(1)
    else:
        print("PR format looks good!")
        exit(0)
