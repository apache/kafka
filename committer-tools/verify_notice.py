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

import argparse
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections import defaultdict

# Kafka-own jars carry the project's NOTICE already covered by the hand-written
# header of NOTICE-binary, so we exclude them from the third-party scan.
KAFKA_JAR_PATTERN = re.compile(r"(kafka|connect|trogdor)", re.IGNORECASE)

# Finds NOTICE-like filenames anywhere in a jar, ignoring case.
# Optional prefixes are separated from NOTICE by '-', '_', or '.', e.g.
# FastDoubleParser-NOTICE. The full suffix is validated during extraction;
# unsupported candidates cause an error.
NOTICE_FILE_PATTERN = re.compile(
    r"(?:^|/)(?:[^/]*[-_.])?NOTICE(?P<suffix>[-_.][^/]*)?\Z",
    re.IGNORECASE,
)

# Marker within the final comment block of Kafka's own header in NOTICE-binary.
# The header ends at the first blank line after this marker and is excluded
# from validation against jar NOTICEs. Update this if the marker text changes.
KAFKA_HEADER_END_MARKER = (
    "// NOTICE file corresponding to the section 4d of The Apache License,"
)

# Sentinel placed where a region has already been consumed.
_CONSUMED_SENTINEL = "\x00"


def run_gradlew(project_dir):
    print("Running './gradlew clean releaseTarGz'")
    subprocess.run(["./gradlew", "clean", "releaseTarGz"], check=True, cwd=project_dir)


def get_tarball_path(project_dir):
    distributions_dir = os.path.join(project_dir, "core", "build", "distributions")
    if not os.path.isdir(distributions_dir):
        print("Error: Distributions directory not found:", distributions_dir)
        sys.exit(1)

    pattern = re.compile(r"^kafka_2\.13-(?!.*docs).+\.tgz$", re.IGNORECASE)
    candidates = [
        os.path.join(distributions_dir, filename)
        for filename in os.listdir(distributions_dir)
        if pattern.match(filename)
    ]
    if not candidates:
        print("Error: No tarball matching 'kafka_2.13-*.tgz' found in:", distributions_dir)
        sys.exit(1)
    return max(candidates, key=os.path.getmtime)


def extract_tarball(tarball, extract_dir):
    with tarfile.open(tarball, "r:gz") as tar:
        try:
            tar.extractall(path=extract_dir, filter=lambda tarinfo, dest: tarinfo)
        except TypeError:
            tar.extractall(path=extract_dir)


def get_third_party_jars(libs_dir):
    return [
        os.path.join(libs_dir, jar_name)
        for jar_name in sorted(os.listdir(libs_dir))
        if jar_name.endswith(".jar") and not KAFKA_JAR_PATTERN.search(jar_name)
    ]


def extract_notices_from_jar(jar_path):
    found = []
    with zipfile.ZipFile(jar_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            match = NOTICE_FILE_PATTERN.search(info.filename)
            if match is None:
                continue

            suffix = (match.group("suffix") or "").lower()
            # Add new supported NOTICE suffixes here in lowercase.
            if suffix not in ("", ".txt", ".md", ".markdown"):
                print(
                    f"Error: unsupported NOTICE-like filename "
                    f"{info.filename!r} in {jar_path!r}."
                )
                sys.exit(1)

            try:
                text = zf.read(info).decode("utf-8").strip()
            except UnicodeDecodeError as error:
                print(
                    f"Error: cannot decode NOTICE file {info.filename!r} "
                    f"in {jar_path!r} as UTF-8: {error}"
                )
                sys.exit(1)
            if text:
                found.append((info.filename, text))
    return found


def collect_notices(jars):
    notice_to_sources = defaultdict(list)
    canonical_text = {}

    for jar in jars:
        jar_name = os.path.basename(jar)
        for entry, text in extract_notices_from_jar(jar):
            key = " ".join(text.split())
            notice_to_sources[key].append((jar_name, entry))
            canonical_text.setdefault(key, text)

    return notice_to_sources, canonical_text


def split_kafka_header(notice_binary_text):
    marker_start = notice_binary_text.find(KAFKA_HEADER_END_MARKER)
    if marker_start < 0:
        print(f"Error: header marker not found in NOTICE-binary: {KAFKA_HEADER_END_MARKER!r}")
        sys.exit(1)
    end_of_block = notice_binary_text.find("\n\n", marker_start)
    if end_of_block < 0:
        print("Error: no blank line found after the header marker in NOTICE-binary.")
        sys.exit(1)
    return notice_binary_text[:end_of_block], notice_binary_text[end_of_block:]


def find_match_span(body, normalized_notice, matched_mask):
    normalized_chars = []
    original_offsets = []
    previous_char = _CONSUMED_SENTINEL
    for offset, char in enumerate(body):
        if matched_mask[offset]:
            if previous_char != _CONSUMED_SENTINEL:
                normalized_chars.append(_CONSUMED_SENTINEL)
                original_offsets.append(offset)
                previous_char = _CONSUMED_SENTINEL
            continue
        if char.isspace():
            if previous_char != " " and previous_char != _CONSUMED_SENTINEL:
                normalized_chars.append(" ")
                original_offsets.append(offset)
                previous_char = " "
        else:
            normalized_chars.append(char)
            original_offsets.append(offset)
            previous_char = char

    match_start = "".join(normalized_chars).find(normalized_notice)
    if match_start < 0:
        return None
    return original_offsets[match_start], original_offsets[match_start + len(normalized_notice) - 1] + 1


def trim_blank_edges(lines):
    first_content_line, content_end_line = 0, len(lines)
    while first_content_line < content_end_line and not lines[first_content_line].strip():
        first_content_line += 1
    while content_end_line > first_content_line and not lines[content_end_line - 1].strip():
        content_end_line -= 1
    return first_content_line, content_end_line


def find_leftover_regions(body, matched_mask, notice_binary_text, body_start):
    regions = []
    region_start, body_length = 0, len(body)
    while region_start < body_length:
        if matched_mask[region_start]:
            region_start += 1
            continue
        region_end = region_start
        while region_end < body_length and not matched_mask[region_end]:
            region_end += 1
        region_text = body[region_start:region_end]
        if region_text.strip():
            lines = region_text.split("\n")
            first_content_line, content_end_line = trim_blank_edges(lines)
            absolute_start = body_start + region_start
            line_start = notice_binary_text.count("\n", 0, absolute_start) + 1 + first_content_line
            line_end = line_start + (content_end_line - first_content_line - 1)
            regions.append((line_start, line_end))
        region_start = region_end
    return regions


def print_sources(sources):
    for idx, (jar, entry) in enumerate(sources):
        if idx > 0:
            print()
        print(f"  - {jar}")
        print(f"    NOTICE file inside JAR: {entry}")


def print_section_header(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def print_update_summary(unmatched, leftover):
    print("\nNOTICE-binary needs updates:")
    step = 1
    if leftover:
        print(f"  {step}. Remove the content in the {len(leftover)} "
              f"line range{'s' if len(leftover) != 1 else ''} below in the order shown.")
        step += 1
    if unmatched:
        print(f"  {step}. Add the {len(unmatched)} NOTICE block{'s' if len(unmatched) != 1 else ''} below.")


def match_notices(body, notice_to_sources, canonical_text):
    # Match the longest normalized NOTICEs first so a shorter one can't consume
    # part of a longer match.
    ordered_notices = sorted(
        notice_to_sources,
        key=lambda notice_key: (
            -len(notice_key),
            min(jar_name for jar_name, _ in notice_to_sources[notice_key]),
        ),
    )
    matched_mask = [False] * len(body)
    unmatched = []
    for notice_key in ordered_notices:
        notice_text = canonical_text[notice_key]
        span = find_match_span(body, notice_key, matched_mask)
        if span is None:
            sources = sorted(notice_to_sources[notice_key])
            unmatched.append((sources, notice_text))
        else:
            for offset in range(span[0], span[1]):
                matched_mask[offset] = True
    return matched_mask, unmatched


def print_notices_to_add(unmatched):
    print_section_header(f"ADD TO NOTICE-binary (NOTICE blocks: {len(unmatched)})")
    print("Copy only the text between COPY START and COPY END into NOTICE-binary.")

    for idx, (sources, text) in enumerate(unmatched, 1):
        print("\n" + f" NOTICE TEXT [{idx:02d}] ".center(78, "-"))
        print("\nSource JARs:")
        print_sources(sources)
        print(f"\n>>> COPY START [{idx:02d}]\n")
        print(text)
        print(f"\n<<< COPY END [{idx:02d}]")


def print_blocks_to_remove(regions):
    print_section_header(f"REMOVE FROM NOTICE-binary (line ranges: {len(regions)})")
    print("Line numbers refer to the file as checked.")
    print("Delete these ranges in the order shown (bottom to top) before adding new text:")
    for idx, (line_start, line_end) in enumerate(reversed(regions), 1):
        print(f"  [{idx:02d}] NOTICE-binary: lines {line_start}-{line_end}")


def print_notice_update_actions(unmatched, leftover):
    if unmatched or leftover:
        print_update_summary(unmatched, leftover)
        if leftover:
            print_blocks_to_remove(leftover)
        if unmatched:
            print_notices_to_add(unmatched)
        print("\nAfter updating NOTICE-binary, run:")
        print("  python committer-tools/verify_notice.py --skip-build")
    else:
        print("\nNOTICE-binary matches the third-party NOTICE files "
              "in the selected release tarball.")


def main():
    parser = argparse.ArgumentParser(
        description="Verify NOTICE-binary against NOTICE files inside the "
                    "third-party jars produced by releaseTarGz."
    )
    parser.add_argument("--skip-build", action="store_true", help="skip the build")
    args = parser.parse_args()

    project_dir = os.getcwd()
    print("Using project directory:", project_dir)

    if args.skip_build:
        print("Skip running './gradlew clean releaseTarGz'")
    else:
        run_gradlew(project_dir)

    tarball = get_tarball_path(project_dir)
    print("Tarball located at:", tarball)

    notice_binary_path = os.path.join(project_dir, "NOTICE-binary")
    if not os.path.isfile(notice_binary_path):
        print("Error: NOTICE-binary not found at project root.")
        sys.exit(1)
    with open(notice_binary_path, "r", encoding="utf-8") as f:
        notice_binary_text = f.read()

    with tempfile.TemporaryDirectory() as tmp_dir:
        extract_tarball(tarball, tmp_dir)
        libs_dir = os.path.join(tmp_dir, os.listdir(tmp_dir)[0], "libs")

        third_party_jars = get_third_party_jars(libs_dir)

        notice_to_sources, canonical_text = collect_notices(third_party_jars)

        header, body = split_kafka_header(notice_binary_text)
        body_start = len(header)

        matched_mask, unmatched = match_notices(body, notice_to_sources, canonical_text)
        leftover = find_leftover_regions(body, matched_mask, notice_binary_text, body_start)

        print_notice_update_actions(unmatched, leftover)

        if unmatched or leftover:
            sys.exit(1)


if __name__ == "__main__":
    main()
