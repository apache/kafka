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
from typing import Dict, List, Optional, TextIO

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


def split_paragraphs(text: str):
    """
    Split the given text into a generator of paragraph lines and a boolean "markdown" flag.

    If any line of a paragraph starts with a markdown character, we will assume the whole paragraph
    contains markdown.
    """
    lines = text.splitlines(keepends=True)
    paragraph = []
    markdown = False
    for line in lines:
        if line.strip() == "":
            if len(paragraph) > 0:
                yield paragraph, markdown
                paragraph.clear()
                markdown = False
        else:
            if line[0] in ("#", "*", "-", "=") or line[0].isdigit():
                markdown = True
            if "```" in line:
                markdown = True
            paragraph.append(line)
    yield paragraph, markdown


def resolve_reviewer(login: str) -> tuple:
    """Map a GitHub login to (name, email).

    Tries reviewer email sources in order: repo commit author email, past
    `Reviewers:` trailers searched via GitHub commit search API (matched
    by name and verified by PR review login), and GitHub user profile
    public email. Noreply emails (@users.noreply.github.com) are treated
    as missing since they are GitHub privacy placeholders that do not
    identify the reviewer. Returns (name, None) when no usable email is
    found; the caller falls back to the '(github:login)' form in the
    Reviewers trailer.
    """
    def _usable_email(e):
        if not e or e.endswith("@users.noreply.github.com"):
            return None
        return e

    def _run_json(cmd, source):
        try:
            p = subprocess.run(cmd, capture_output=True, text=True)
            if p.returncode == 0:
                return json.loads(p.stdout)
            logger.debug(f"Failed to resolve {login} from {source}: {p.stderr}")
        except Exception as e:
            logger.debug(f"Failed to resolve {login} from {source}: {e}")
        return None

    def _has_pr_review_from_login(commit_sha):
        pulls = _run_json(["gh", "api", f"repos/apache/kafka/commits/{commit_sha}/pulls"],
                          f"associated PRs for commit {commit_sha}") or []
        for pull in pulls:
            pr_number = pull.get("number")
            if not pr_number:
                continue
            reviews = _run_json(["gh", "api", f"repos/apache/kafka/pulls/{pr_number}/reviews?per_page=100"],
                                f"reviews for PR {pr_number}") or []
            if any((review.get("user") or {}).get("login", "").lower() == login.lower()
                   for review in reviews):
                return True
        return False

    commits = _run_json(["gh", "api", f"repos/apache/kafka/commits?author={login}&per_page=1"],
                        "commit history") or []
    author = commits[0].get("commit", {}).get("author", {}) if commits else {}

    # Tier 1: latest repo commit authored by this GitHub login. Misses
    # when the reviewer has no merged commit in apache/kafka, or had
    # "Keep my email private" enabled at commit time (GitHub rewrites
    # the author to the noreply form).
    email = _usable_email(author.get("email"))
    if email:
        return (author.get("name") or login, email)

    user = _run_json(["gh", "api", f"users/{login}"], "GitHub profile") or {}

    name_candidates = []
    for candidate in (user.get("name"), author.get("name"), login):
        if candidate and candidate not in name_candidates:
            name_candidates.append(candidate)

    name = name_candidates[0] if name_candidates else login

    # Tier 2: past Reviewers: trailers in commit history, matched by name,
    # via the GitHub commit search API. Catches pure reviewers (no commits
    # in apache/kafka, no public profile email) who have been credited
    # with a real email in an earlier merged PR. Sort by committer-date
    # desc so the most recent email wins if a reviewer has changed it.
    # Full-text search is tokenized (not strict substring), so we re-verify
    # with a regex client-side. To avoid same-name matches, we only accept
    # a trailer email when the matched commit's associated PR includes a
    # review from this GitHub login.
    for candidate in name_candidates:
        results = _run_json(["gh", "search", "commits",
                             "--repo", "apache/kafka",
                             f'"{candidate} <"',
                             "--limit", "10",
                             "--sort", "committer-date",
                             "--order", "desc",
                             "--json", "sha,commit"],
                            "commit search") or []
        pattern = re.compile(rf"{re.escape(candidate)}\s*<([^>]+)>")
        for result in results:
            msg = result.get("commit", {}).get("message", "")
            commit_sha = result.get("sha")
            for match in pattern.finditer(msg):
                candidate_email = _usable_email(match.group(1))
                if candidate_email and commit_sha and _has_pr_review_from_login(commit_sha):
                    return (candidate, candidate_email)

    # Tier 3: GitHub user profile. Only exposes an email when the reviewer
    # has set a Public email in their profile settings.
    return (name, _usable_email(user.get("email")))


def already_exists(identity: str, existing_reviewers: List[str]) -> bool:
    """Check if a reviewer identity is already in the existing reviewers list.

    identity is the delimited token that uniquely identifies a reviewer, either
    '<email>' (for the email form) or '(github:login)' (for the login fallback).
    """
    return identity.lower() in ", ".join(existing_reviewers).lower()


def update_reviewers_trailer(body: str, trailer: str) -> str:
    """Update the Reviewers trailer in the body using git interpret-trailers."""
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(body.strip().encode())
        fp.write(b"\n")
        fp.flush()
        cmd = f"git interpret-trailers --if-exists replace --trailer {shlex.quote(trailer)} {fp.name}"
        p = subprocess.run(shlex.split(cmd), capture_output=True)
        fp.close()

    if p.returncode == 0:
        return p.stdout.decode()
    return body


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
    cmd = f"gh pr view {pr_number} --json 'title,body,reviews,author'"
    p = subprocess.run(shlex.split(cmd), capture_output=True)
    if p.returncode != 0:
        logger.error(f"GitHub CLI failed with exit code {p.returncode}.\nSTDOUT: {p.stdout.decode()}\nSTDERR:{p.stderr.decode()}")
        exit(1)

    gh_json = json.loads(p.stdout)
    title = gh_json["title"]
    body = gh_json["body"]
    reviews = gh_json["reviews"]

    # Auto-fill reviewer from the current review event.
    # Approvals are also review events, so approvers are automatically added.
    reviewer_login = get_env("REVIEWER_LOGIN")
    pr_author = (gh_json.get("author") or {}).get("login")
    if reviewer_login and reviewer_login != pr_author:
        name, email = resolve_reviewer(reviewer_login)
        if email:
            identity = f"<{email}>"
        else:
            # Tier 4: fall back to the GitHub handle without tagging the reviewer.
            identity = f"(github:{reviewer_login})"
        resolved = f"{name} {identity}"
        existing_reviewers = parse_trailers(title, body).get("Reviewers", [])
        if not already_exists(identity, existing_reviewers):
            existing_value = ", ".join(existing_reviewers)
            new_value = f"{existing_value}, {resolved}" if existing_value else resolved
            body = update_reviewers_trailer(body, f"Reviewers: {new_value}")

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

    paragraph_iter = split_paragraphs(body)
    new_paragraphs = []
    for p, markdown in paragraph_iter:
        if markdown:
            # If a paragraph looks like it has markdown in it, wrap each line separately.
            new_lines = []
            for line in p:
                new_lines.append(textwrap.fill(line, width=72, break_long_words=False, break_on_hyphens=False, replace_whitespace=False))
            rewrapped_p = "\n".join(new_lines)
        else:
            indent = ""
            if len(p) > 0 and p[0].startswith("Reviewers:"):
                indent = " "
            rewrapped_p = textwrap.fill("".join(p), subsequent_indent=indent, width=72, break_long_words=False, break_on_hyphens=False, replace_whitespace=True)
        new_paragraphs.append(rewrapped_p + "\n")
    body = "\n".join(new_paragraphs)

    if get_env("GITHUB_ACTIONS"):
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
