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

# Appends a reviewer to the `Reviewers:` trailer of a PR body. This is the
# shared engine behind the pr-reviewers-trailer-on-review.yml and
# pr-reviewers-trailer-on-comment.yml workflows. It is the only script that
# edits a PR body, and it only changes the trailer block. pr-format.py (the PR
# linter) is separate and read-only.
#
# Reads PR_NUMBER and REVIEWER_LOGIN from the environment. Expects the `gh`
# CLI and `git` to be available. No-op (exit 0) when REVIEWER_LOGIN is unset
# or equal to the PR author.

from collections import defaultdict
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


def get_env(key: str, fn = str) -> Optional:
    value = os.getenv(key)
    if value is None:
        logger.debug(f"Could not find env {key}")
        return None
    else:
        logger.debug(f"Read env {key}: {value}")
        return fn(value)


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
    is_reviewers = [bool(re.match(r"\s*reviewers\s*:", line, re.IGNORECASE)) for line in lines]
    if any(is_reviewers[:last_paragraph_start(lines)]):
        return True
    return sum(is_reviewers) > len(parse_trailers(title, body).get("reviewers", []))


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


def fold_reviewers(reviewers: str) -> str:
    """Build a Reviewers trailer with no line longer than 72 characters.

    Lines are broken after a comma, so a name stays next to its email.
    Continuation lines start with a space, as RFC 822 folding requires, so
    git still reads them as one trailer. GitHub wraps longer lines when it
    builds the squash commit message and does not indent the continuation,
    which cuts the trailer short.
    """
    names = [name.strip() for name in reviewers.split(",") if name.strip()]
    lines = []
    line = "Reviewers:"
    for i, name in enumerate(names):
        piece = f" {name}," if i < len(names) - 1 else f" {name}"
        if len(line) + len(piece) > 72 and line not in ("Reviewers:", ""):
            lines.append(line)
            line = ""
        line += piece
    lines.append(line)
    # A single reviewer can still be too long for one line.
    return "\n".join(textwrap.fill(line, width=72, subsequent_indent=" ",
                                   break_long_words=False, break_on_hyphens=False)
                     for line in lines)


def update_reviewers_trailer(title: str, body: str, reviewers: str) -> Optional[str]:
    """Set the Reviewers trailer of the body.

    git is used only to read the trailers. The new body is built from the
    original text, so every byte that the author wrote is kept. Only the
    Reviewers trailer is removed and written again at the end of the last
    paragraph. Returns None if git does not read the result as expected.
    """
    newline = "\r\n" if "\r\n" in body else "\n"
    lines = body.split("\n")
    last_paragraph = last_paragraph_start(lines)
    while lines and not lines[-1].strip():
        lines.pop()
    if lines:
        lines[-1] = lines[-1].rstrip("\r")
    old_trailers = parse_trailers(title, body)
    had_trailers = bool(old_trailers)

    # Remove the existing Reviewers trailer and its continuation lines.
    found = [i for i in range(last_paragraph, len(lines)) if re.match(r"reviewers\s*:", lines[i], re.IGNORECASE)]
    if found:
        end = found[-1] + 1
        while end < len(lines) and lines[end][:1] in (" ", "\t"):
            end += 1
        del lines[found[-1]:end]

    # Join the trailer block if there still is one. Otherwise start a new paragraph.
    in_trailer_block = had_trailers and len(lines) > last_paragraph
    rest = "\n".join(lines).rstrip("\r\n")
    folded = fold_reviewers(reviewers).replace("\n", newline)
    if not rest:
        new_body = folded + newline
    else:
        new_body = rest + (newline if in_trailer_block else newline * 2) + folded + newline

    # Check the result with git. The code above finds the last paragraph by
    # itself, and git has more rules (for example it skips "#" lines).
    expected = " ".join(reviewers.split())
    new_trailers = parse_trailers(title, new_body)
    new_reviewers = new_trailers.pop("reviewers", [])
    old_trailers.pop("reviewers", None)
    if (new_reviewers[-1:] != [expected]
            or reviewers_trailer_is_misplaced(title, new_body)
            or new_trailers != old_trailers):
        logger.error("git does not read the new description as expected; leaving the body unchanged.")
        return None
    return new_body


def gh_pr_view(pr_number: str, fields: str) -> Dict:
    cmd = f"gh pr view {pr_number} --json '{fields}'"
    p = subprocess.run(shlex.split(cmd), capture_output=True)
    if p.returncode != 0:
        logger.error(f"GitHub CLI failed with exit code {p.returncode}.\nSTDOUT: {p.stdout.decode()}\nSTDERR:{p.stderr.decode()}")
        exit(1)
    return json.loads(p.stdout)


def resolve_identity(pr_number: str, reviewer_login: str) -> tuple:
    """Return (text for the trailer, token that identifies the reviewer). Exits if the reviewer is the PR author."""
    pr_author = (gh_pr_view(pr_number, "author").get("author") or {}).get("login")

    if reviewer_login == pr_author:
        logger.info(f"Reviewer {reviewer_login} is the PR author; not adding to Reviewers.")
        exit(0)

    name, email = resolve_reviewer(reviewer_login)
    if email:
        identity = f"<{email}>"
    else:
        # Fall back to the GitHub handle without tagging the reviewer.
        identity = f"(github:{reviewer_login})"
    return f"{name} {identity}", identity


if __name__ == "__main__":
    """
    Appends REVIEWER_LOGIN to the Reviewers trailer of PR_NUMBER's body, and
    folds the trailer if it has a line longer than 72 characters.
    Approvals are review events too, so approvers are credited the same way.
    The PR author is never added as their own reviewer.

    Without REVIEWER_LOGIN the script adds nobody. It only writes the
    existing Reviewers trailer in the standard format. This runs when the
    description is edited, so a trailer typed by hand is folded before the merge.
    """
    pr_number = get_env("PR_NUMBER")
    reviewer_login = get_env("REVIEWER_LOGIN")
    if not pr_number:
        logger.info("PR_NUMBER is required; nothing to do.")
        exit(0)

    resolved = identity = None
    if reviewer_login:
        resolved, identity = resolve_identity(pr_number, reviewer_login)
    # Read the body only now. Resolving the reviewer takes several API calls,
    # and an edit made between reading the body and writing it back is lost.
    gh_json = gh_pr_view(pr_number, "title,body")
    title = gh_json["title"]
    body = gh_json["body"] or ""

    existing_reviewers = parse_trailers(title, body).get("reviewers", [])
    if reviewers_trailer_is_misplaced(title, body):
        # There is text below the Reviewers trailer, so git does not read it.
        # Adding a second trailer would hide the problem. The PR linter reports it.
        logger.info("A 'Reviewers:' line is not in the last paragraph; leaving the body unchanged.")
        exit(0)
    new_value = ", ".join(existing_reviewers)
    if not reviewer_login:
        if len(existing_reviewers) != 1:
            logger.info("No single Reviewers trailer to format; nothing to do.")
            exit(0)
    elif already_exists(identity, existing_reviewers):
        logger.info(f"Reviewer {resolved} already present.")
    else:
        new_value = f"{new_value}, {resolved}" if new_value else resolved

    # Write the trailer even if the reviewer is already present. A trailer
    # typed by hand may be on one long line, and it has to be folded.
    new_body = update_reviewers_trailer(title, body, new_value)
    if new_body is None:
        exit(1)
    if new_body.rstrip() == body.rstrip():
        logger.info("Reviewers trailer is up to date; nothing to do.")
        exit(0)
    body = new_body

    if get_env("GITHUB_ACTIONS"):
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(body.encode())
            fp.flush()
            cmd = f"gh pr edit {pr_number} --body-file {fp.name}"
            p = subprocess.run(shlex.split(cmd), capture_output=True)
            fp.close()
            if p.returncode != 0:
                logger.error(f"Could not update PR {pr_number}. STDOUT: {p.stdout.decode()}")
                exit(1)
        logger.info(f"Updated the Reviewers trailer of PR #{pr_number}: {new_value}")
    else:
        logger.info(f"Not updating {pr_number} since this is not running on GitHub Actions.")
    exit(0)
