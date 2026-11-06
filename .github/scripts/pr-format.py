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
import logging
import os
import re
import subprocess
import shlex
import sys
import tempfile
import textwrap
from typing import Dict, Optional, TextIO

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
    trailers = defaultdict(list)

    with tempfile.NamedTemporaryFile() as fp:
        write_commit(fp, title, body)
        cmd = f"git interpret-trailers --trim-empty --parse {fp.name}"
        p = subprocess.run(shlex.split(cmd), capture_output=True)
        fp.close()

    for line in p.stdout.decode().splitlines():
        key, value = line.split(":", 1)
        trailers[key].append(value.strip())

    return trailers


_FENCE_START = re.compile(r"^ {0,3}(`{3,}|~{3,})")
_BLOCK_START = re.compile(
    r"^ {0,3}(?:#{1,6}(?:\s|$)|>|[-+*]|\d+[.)]\s+|<|\[[^\]]+\]:)"
)
_THEMATIC_BREAK = re.compile(
    r"^ {0,3}(?:(?:\*\s*){3,}|(?:-\s*){3,}|(?:_\s*){3,}|(?:=\s*){3,})$"
)
_TRAILER = re.compile(r"^[A-Za-z0-9-]+:")


def _wrap_paragraph(lines) -> str:
    text = [line.rstrip("\n") for line in lines]
    reviewers = text[0].startswith("Reviewers:")
    if not reviewers:
        if any(line.startswith((" ", "\t")) for line in text):
            return "".join(lines)
        if any(_BLOCK_START.match(line) or _THEMATIC_BREAK.match(line) for line in text):
            return "".join(lines)
        if _TRAILER.match(text[0]):
            return "".join(lines)
        if any(re.search(r"[\\|<>]| {2,}$", line) for line in text):
            return "".join(lines)

    wrapped = textwrap.fill(
        " ".join(re.sub(r"[ \t]+", " ", line.strip()) for line in text),
        subsequent_indent=" " if reviewers else "",
        width=72,
        break_long_words=False,
        break_on_hyphens=False,
    )
    if not reviewers and any(
        _BLOCK_START.match(line) or _THEMATIC_BREAK.match(line)
        or _FENCE_START.match(line) or _TRAILER.match(line)
        for line in wrapped.splitlines()
    ):
        return "".join(lines)
    return wrapped + ("\n" if lines[-1].endswith("\n") else "")


def format_body(body: str) -> str:
    """Normalize line endings and wrap prose while preserving Markdown blocks."""
    body = body.replace("\r\n", "\n").replace("\r", "\n")
    output = []
    paragraph = []
    fence = None

    def flush_paragraph():
        if paragraph:
            output.append(_wrap_paragraph(paragraph))
            paragraph.clear()

    for line in body.splitlines(keepends=True):
        text = line.rstrip("\r\n")
        if fence:
            output.append(line)
            if re.match(rf"^ {{0,3}}{re.escape(fence[0])}{{{len(fence)},}}\s*$", text):
                fence = None
        else:
            match = _FENCE_START.match(text)
            if match:
                flush_paragraph()
                fence = match.group(1)
                output.append(line)
            elif text.strip() == "":
                flush_paragraph()
                output.append(line)
            else:
                if _TRAILER.match(text) or (
                    paragraph and _TRAILER.match(paragraph[0])
                    and not text.startswith((" ", "\t"))
                ):
                    flush_paragraph()
                paragraph.append(line)

    flush_paragraph()
    return "".join(output)


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
    check("Committer Checklist" not in body, "PR template text not present", "Old PR template text should be removed")

    original_body = body
    body = format_body(body)

    if body == original_body:
        logger.info(f"PR {pr_number} body is already formatted.")
    elif get_env("GITHUB_ACTIONS"):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(body.encode())
            fp.flush()
            cmd = f"gh pr edit {pr_number} --body-file {fp.name}"
            p = subprocess.run(shlex.split(cmd), capture_output=True)
            fp.close()
            if p.returncode != 0:
                logger.error(f"Could not update PR {pr_number}. STDOUT: {p.stdout.decode()}")
    else:
        logger.info(f"Not reformatting {pr_number} since this is not running on GitHub Actions.")

    # Check for Reviewers
    approved = has_approval(reviews)
    if approved:
        trailers = parse_trailers(title, body)
        reviewers_in_body = trailers.get("Reviewers", [])
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
