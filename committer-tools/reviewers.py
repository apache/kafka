#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import shlex
import subprocess
import tempfile
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


from collections import defaultdict
import os
import re


def prompt_for_user():
    while True:
        try:
            user_input = input("\nName or email (case insensitive): ")
        except (KeyboardInterrupt, EOFError):
            return None
        clean_input = user_input.strip().lower()
        if clean_input != "":
            return clean_input


def update_trailers(body, trailer):
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(body.encode())
        fp.flush()
        cmd = f"git interpret-trailers --if-exists replace --trailer '{trailer}' {fp.name} "
        p = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
        fp.close()

    return p.stdout


def append_message_to_pr_body(pr: int , message: str):
    try:
        pr_url = f"https://github.com/apache/kafka/pull/{pr}"
        cmd_get_pr = f"gh pr view {pr_url} --json title,body"
        result = subprocess.run(shlex.split(cmd_get_pr), capture_output=True, text=True, check=True)
        current_pr_body = json.loads(result.stdout).get("body", {}).strip() + "\n"
        pr_title = json.loads(result.stdout).get("title", {})
        updated_pr_body = update_trailers(current_pr_body, message)
    except subprocess.CalledProcessError as e:
        print("Failed to retrieve PR body:", e.stderr)
        return

    print(f"""New PR body will be:\n\n---\n{updated_pr_body}---\n""")
    choice = input(f'Update the body of "{pr_title}"? [Y/n] ').strip().lower()
    if choice not in ['', 'y']:
        print("Abort.")
        return

    try:
        cmd_edit_body = f"gh pr edit {pr_url} --body {shlex.quote(updated_pr_body)}"
        subprocess.run(shlex.split(cmd_edit_body), check=True)
        print("PR body updated successfully!")
    except subprocess.CalledProcessError as e:
        print("Failed to update PR body:", e.stderr)


if __name__ == "__main__":
    print("Utility to help generate 'Reviewers' string for Pull Requests. Use Ctrl+D or Ctrl+C to exit")

    command = r"git log | grep 'Reviewers\|Author'"
    stream = os.popen(command)
    lines = stream.readlines()
    all_reviewers = defaultdict(int)
    for line in lines:
        stripped = line.strip().lstrip("Reviewers: ").lstrip("Author: ")
        reviewers = stripped.split(",")
        for reviewer in reviewers:
            all_reviewers[reviewer.strip()] += 1
    parsed_reviewers = []

    for item in all_reviewers.items():
        patterns = r"(?P<name>.*)\s<(?P<email>.*)>"
        m = re.match(patterns, item[0])
        if m is not None and len(m.groups()) == 2:
            if item[1] > 2:
                parsed_reviewers.append((m.group("name"), m.group("email"), item[1]))

    selected_reviewers = []
    while True:
        if selected_reviewers:
            print(f"Reviewers so far: {selected_reviewers}")
        user_input = prompt_for_user()
        if user_input is None:
            break
        candidates = []
        for reviewer, email, count in parsed_reviewers:
            if reviewer.lower().startswith(user_input) or email.lower().startswith(user_input):
                candidates.append((reviewer, email, count))
            if len(candidates) == 10:
                break
        if not candidates:
            continue

        print("\nPossible matches (in order of most recent):")
        for i, candidate in zip(range(10), candidates):
            print(f"[{i+1}] {candidate[0]} {candidate[1]} ({candidate[2]})")

        try:
            selection_input = input("\nMake a selection: ")
            selected_candidate = candidates[int(selection_input)-1]
            selected_reviewers.append(selected_candidate)
        except (EOFError, KeyboardInterrupt):
            break
        except (ValueError, IndexError):
            print("Invalid selection")
            continue

    if selected_reviewers:
        reviewer_message = "Reviewers: "
        reviewer_message += ", ".join([f"{name} <{email}>" for name, email, _ in selected_reviewers])
        print(f"\n\n{reviewer_message}\n")

        try:
            pr_number = int(input("\nPull Request (Ctrl+D or Ctrl+C to skip): "))
            append_message_to_pr_body(pr_number, reviewer_message)
        except (EOFError, KeyboardInterrupt):
            exit(0)
