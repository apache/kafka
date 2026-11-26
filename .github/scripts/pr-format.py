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
from io import BytesIO
import json
import re
import logging
import os
import subprocess
import shlex
import sys
import tempfile
from typing import List, Dict, Optional, TextIO

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

ok = "✅"
err = "❌"


def get_env(key: str, fn = str) -> Optional:
    value = os.getenv(key)
    if value is None:
        logger.debug(f"Could not find env {key}")
        return None
    else:
        logger.debug(f"Read env {key}: {value}")
        return fn(value)


def has_approval(reviews) -> bool:
    approved = False
    for review in reviews:
        if review.get("authorAssociation") not in ("MEMBER", "OWNER"):
            continue
        if review.get("state") == "APPROVED":
            approved = True
    return approved


def write_commit(io: TextIO, title: str, body: str):
    io.write(title.encode())
    io.write(b"\n\n")
    io.write(body.encode())
    io.flush()


def parse_trailers(title, body) -> Dict:
    """Return the trailers that git reads from the message. Keys are in lower case."""
    trailers = defaultdict(list)

    with tempfile.NamedTemporaryFile() as fp:
        write_commit(fp, title, body)
        # --no-divider: a "---" line is a Markdown rule here, not the start of a patch.
        cmd = f"git interpret-trailers --no-divider --trim-empty --parse {fp.name}"
        p = subprocess.run(shlex.split(cmd), capture_output=True)
        fp.close()

    # Split on "\n" only. With CRLF line endings git leaves a "\r" inside a folded value.
    for line in p.stdout.decode().split("\n"):
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        trailers[key.strip().lower()].append(" ".join(value.split()))

    return trailers


def last_paragraph_start(lines: List[str]) -> int:
    """Index of the first line of the last paragraph. Blank lines at the end are not counted."""
    end = len(lines)
    while end > 0 and not lines[end - 1].strip():
        end -= 1
    return max((i + 1 for i in range(end) if not lines[i].strip()), default=0)


def reviewers_trailer_is_misplaced(title: str, body: str) -> bool:
    """True if a "Reviewers:" line is not a trailer in the last paragraph.

    git reads trailers only from the last paragraph. A "Reviewers:" line
    anywhere else is plain text, and the people on it get no credit.
    """
    lines = body.split("\n")
    # A "Reviewers:" line with no value is not counted. git drops an empty trailer.
    is_reviewers = [bool(re.match(r"\s*reviewers\s*:\s*\S", line, re.IGNORECASE)) for line in lines]
    if any(is_reviewers[:last_paragraph_start(lines)]):
        return True
    return sum(is_reviewers) > len(parse_trailers(title, body).get("reviewers", []))


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
    * Every "Reviewers:" line is in the last paragraph, where git reads it as a trailer

    This script only reads the PR. It never edits the title or the body.
    """

    pr_number = get_env("PR_NUMBER")
    cmd = f"gh pr view {pr_number} --json 'title,body,reviews'"
    p = subprocess.run(shlex.split(cmd), capture_output=True)
    if p.returncode != 0:
        logger.error(f"GitHub CLI failed with exit code {p.returncode}.\nSTDOUT: {p.stdout.decode()}\nSTDERR:{p.stderr.decode()}")
        exit(1)

    gh_json = json.loads(p.stdout)
    title = gh_json["title"]
    body = gh_json["body"] or ""
    reviews = gh_json["reviews"]

    checks = [] # (bool (0=ok, 1=error), message)

    def check(positive_assertion, ok_msg, err_msg):
        if positive_assertion:
            checks.append((0, f"{ok} {ok_msg}"))
        else:
            checks.append((1, f"{err} {err_msg}"))

    # Check title
    check(not title.endswith("..."), "Title is not truncated", "Title appears truncated (ends with ...)")
    check(len(title) >= 15, "Title is not too short", "Title is too short (under 15 characters)")
    check(len(title) <= 120, "Title is not too long", "Title is too long (over 120 characters)")
    ok_prefix = title.startswith("KAFKA-") or title.startswith("MINOR") or title.startswith("HOTFIX")
    check(ok_prefix, "Title has expected KAFKA/MINOR/HOTFIX", "Title is missing KAFKA-XXXXX or MINOR/HOTFIX prefix")

    # Check body
    check(len(body) != 0, "Body is not empty", "Body is empty")
    check("Delete this text and replace" not in body, "PR template text not present", "PR template text should be removed")
    check("Committer Checklist" not in body, "Old PR template text not present", "Old PR template text should be removed")

    # Check for Reviewers
    check(not reviewers_trailer_is_misplaced(title, body), "Every 'Reviewers:' line is a trailer",
          "'Reviewers:' must be in the last paragraph. Move the text below it above it.")
    reviewers_in_body = parse_trailers(title, body).get("reviewers", [])

    approved = has_approval(reviews)
    if approved:
        check(len(reviewers_in_body) > 0, "Found 'Reviewers' in commit body", "Pull Request is approved, but no 'Reviewers' found in commit body")
        if len(reviewers_in_body) > 0:
            for reviewer_in_body in reviewers_in_body:
                logger.debug(reviewer_in_body)

    logger.debug("Commit will look like:\n")
    logger.debug("<pre>")
    io = BytesIO()
    title += f" (#{pr_number})"
    write_commit(io, title, body)
    io.seek(0)
    logger.debug(io.read().decode())
    logger.debug("</pre>\n")

    exit_code = 0
    logger.debug("Validation results:")
    for err, msg in checks:
        logger.debug(f"* {msg}")

    for err, msg in checks:
        # Just output the first error for the status message. STDOUT becomes the status check message
        if err:
            print(msg)
            exit(1)

    logger.debug("No validation errors, PR format looks good!")
    print("PR format looks good!")
    exit(0)
