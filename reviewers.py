#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
        out = "\n\nReviewers: "
        out += ", ".join([f"{name} <{email}>" for name, email, _ in selected_reviewers])
        out += "\n"
        print(out)


