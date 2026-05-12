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
from collections import defaultdict, namedtuple
from textwrap import indent

# Kafka-own jars carry the project's NOTICE already covered by the hand-written
# header of NOTICE-binary, so we exclude them from the third-party scan.
KAFKA_JAR_PATTERN = re.compile(r"(kafka|connect|trogdor)", re.IGNORECASE)

# Matches NOTICE-like files anywhere in a jar.  Accepts plain `NOTICE`,
# `NOTICE.{txt,md,markdown}`, and prefix/suffix variants such as
# `META-INF/FastDoubleParser-NOTICE` shipped inside `jackson-core`.
# The `[-_.]` boundary blocks false positives like `NOTICEABLE.java`.
NOTICE_FILE_PATTERN = re.compile(
    r"(?:^|/)(?:[^/]*[-_.])?NOTICE(?:[-_.][^/]*)?(?:\.(?:txt|md|markdown))?$",
    re.IGNORECASE,
)

# Marker that ends Kafka's own header inside NOTICE-binary; everything before
# it (inclusive) is preserved verbatim and not validated against jar NOTICEs.
# If you change the comment block around line 22 of NOTICE-binary, update this.
KAFKA_HEADER_END_MARKER = (
    "// NOTICE file corresponding to the section 4d of The Apache License,"
)

WHITESPACE_PATTERN = re.compile(r"\s+")


NoticeIndex = namedtuple(
    "NoticeIndex",
    "jar_to_notices jars_without_notice notice_to_jars notice_to_sources canonical_text",
)


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
        os.path.join(distributions_dir, f)
        for f in os.listdir(distributions_dir)
        if pattern.match(f)
    ]
    if not candidates:
        print("Error: No tarball matching 'kafka_2.13-*.tgz' found in:", distributions_dir)
        sys.exit(1)
    return max(candidates, key=os.path.getmtime)


def extract_tarball(tarball, extract_dir):
    with tarfile.open(tarball, "r:gz") as tar:
        tar.extractall(path=extract_dir, filter=lambda tarinfo, dest: tarinfo)
    print("Tarball extracted to:", extract_dir)


def classify_jars(libs_dir):
    third_party, kafka_own = [], []
    for fname in sorted(os.listdir(libs_dir)):
        if not fname.endswith(".jar"):
            continue
        path = os.path.join(libs_dir, fname)
        if KAFKA_JAR_PATTERN.search(fname):
            kafka_own.append(path)
        else:
            third_party.append(path)
    return third_party, kafka_own


def extract_notices_from_jar(jar_path):
    found = []
    with zipfile.ZipFile(jar_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir() or not NOTICE_FILE_PATTERN.search(info.filename):
                continue
            raw = zf.read(info)
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("latin-1", errors="replace")
            text = text.strip()
            if text:
                found.append((info.filename, text))
    return found


def collect_notices(jars):
    jar_to_notices = {}
    jars_without_notice = []
    notice_to_jars = defaultdict(list)
    notice_to_sources = defaultdict(list)
    canonical_text = {}

    for jar in jars:
        jar_name = os.path.basename(jar)
        notices = extract_notices_from_jar(jar)
        if not notices:
            jars_without_notice.append(jar_name)
            continue
        jar_to_notices[jar_name] = notices
        seen_keys = set()
        for entry, text in notices:
            key = normalize(text)
            notice_to_sources[key].append((jar_name, entry))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            notice_to_jars[key].append(jar_name)
            canonical_text.setdefault(key, text)

    return NoticeIndex(jar_to_notices, jars_without_notice,
                       notice_to_jars, notice_to_sources, canonical_text)


def split_kafka_header(notice_binary_text):
    idx = notice_binary_text.find(KAFKA_HEADER_END_MARKER)
    if idx < 0:
        print(f"Error: header marker not found in NOTICE-binary: {KAFKA_HEADER_END_MARKER!r}")
        sys.exit(1)
    end_of_block = notice_binary_text.find("\n\n", idx)
    return notice_binary_text[:end_of_block], notice_binary_text[end_of_block:]


def normalize(text):
    return WHITESPACE_PATTERN.sub(" ", text).strip()


# Sentinel placed where a region has already been consumed.
_CONSUMED_SENTINEL = "\x00"


def find_match_span(haystack, needle, mask):
    needle_norm = normalize(needle)
    norm_chars = []
    orig_offsets = []
    prev_char = _CONSUMED_SENTINEL
    for i, ch in enumerate(haystack):
        if mask[i]:
            if prev_char != _CONSUMED_SENTINEL:
                norm_chars.append(_CONSUMED_SENTINEL)
                orig_offsets.append(i)
                prev_char = _CONSUMED_SENTINEL
            continue
        if ch.isspace():
            if prev_char != " " and prev_char != _CONSUMED_SENTINEL:
                norm_chars.append(" ")
                orig_offsets.append(i)
                prev_char = " "
        else:
            norm_chars.append(ch)
            orig_offsets.append(i)
            prev_char = ch

    pos = "".join(norm_chars).find(needle_norm)
    if pos < 0:
        return None
    return orig_offsets[pos], orig_offsets[pos + len(needle_norm) - 1] + 1


def trim_blank_edges(lines):
    head, tail = 0, len(lines)
    while head < tail and not lines[head].strip():
        head += 1
    while tail > head and not lines[tail - 1].strip():
        tail -= 1
    return head, tail


def find_leftover_regions(body, mask, notice_binary_text, body_start):
    regions = []
    i, n = 0, len(body)
    while i < n:
        if mask[i]:
            i += 1
            continue
        j = i
        while j < n and not mask[j]:
            j += 1
        chunk = body[i:j]
        if chunk.strip():
            lines = chunk.splitlines()
            head, tail = trim_blank_edges(lines)
            if head < tail:
                full_start = body_start + i
                line_start = notice_binary_text[:full_start].count("\n") + 1 + head
                line_end = line_start + (tail - head - 1)
                regions.append((line_start, line_end, lines[head:tail]))
        i = j
    return regions


def print_sources(sources, indent_str):
    for jar, entry in sources:
        print(f"{indent_str}- {jar}  ({entry})")


def print_jar_overview(third_party_jars, kafka_own_jars):
    print(f"\nKafka-own jars excluded by regex {KAFKA_JAR_PATTERN.pattern!r} "
          f"({len(kafka_own_jars)}):")
    for jar in kafka_own_jars:
        print("  -", os.path.basename(jar))

    print(f"\nThird-party jars to inspect ({len(third_party_jars)}):")
    for jar in third_party_jars:
        print("  -", os.path.basename(jar))


def print_notice_inventory(index):
    print(f"\nJars containing a NOTICE file ({len(index.jar_to_notices)}):")
    for jar, notices in sorted(index.jar_to_notices.items()):
        entries = ", ".join(entry for entry, _ in notices)
        print(f"  + {jar}  [{entries}]")

    print(f"\nJars WITHOUT a NOTICE file ({len(index.jars_without_notice)}):")
    for jar in index.jars_without_notice:
        print(f"  - {jar}")

    shared_groups = [(k, v) for k, v in index.notice_to_jars.items() if len(v) > 1]
    singletons = len(index.notice_to_jars) - len(shared_groups)
    total_notice_files = sum(len(n) for n in index.jar_to_notices.values())
    print(f"\nNOTICE de-duplication: {total_notice_files} NOTICE file(s) "
          f"across {len(index.jar_to_notices)} jar(s) -> "
          f"{len(index.notice_to_jars)} unique NOTICE text(s) "
          f"({len(shared_groups)} shared group(s) + {singletons} singleton(s)).")
    if shared_groups:
        print(f"\nShared NOTICE groups ({len(shared_groups)}):")
        for key, jars in shared_groups:
            print("  *", ", ".join(sorted(jars)))
            print_sources(sorted(index.notice_to_sources[key]), "      ")


def match_notices(body, index):
    # Longest NOTICEs first so a shorter one can't be absorbed as a substring of
    # a longer one and fragment the longer match.
    ordered = sorted(
        index.notice_to_jars.items(),
        key=lambda kv: (-len(index.canonical_text[kv[0]]), min(kv[1])),
    )
    mask = [False] * len(body)
    matched = []
    unmatched = []
    for key, _jars in ordered:
        sources = sorted(index.notice_to_sources[key])
        span = find_match_span(body, index.canonical_text[key], mask)
        if span is None:
            unmatched.append((sources, index.canonical_text[key]))
        else:
            for i in range(span[0], span[1]):
                mask[i] = True
            matched.append(sources)
    return mask, matched, unmatched


def print_matched(matched):
    print(f"\nMatched upstream NOTICEs already present in NOTICE-binary "
          f"({len(matched)}):")
    for idx, sources in enumerate(matched, 1):
        print(f"  [{idx:02d}] OK  source(s):")
        print_sources(sources, "          ")


def print_unmatched(unmatched, total_unique_notices):
    print(f"\nUpstream NOTICEs NOT matched in NOTICE-binary "
          f"({len(unmatched)} of {total_unique_notices} unique upstream NOTICEs):")
    if not unmatched:
        print("    <none>")
    for idx, (sources, text) in enumerate(unmatched, 1):
        print("\n" + "-" * 78)
        print(f"  [{idx:02d}] source(s):")
        print_sources(sources, "          ")
        print("-" * 78)
        print(indent(text, "  | "))


def print_leftover(regions):
    print(f"\nLeftover content in NOTICE-binary, potentially stale "
          f"({len(regions)} block(s)):")
    if not regions:
        print("    <none>")
    for idx, (line_start, line_end, lines) in enumerate(regions, 1):
        print("\n" + "-" * 78)
        print(f"  [{idx:02d}] NOTICE-binary lines L{line_start}-L{line_end}")
        print("-" * 78)
        for offset, line in enumerate(lines):
            print(f"  {line_start + offset:4d} | {line}")


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

        third_party_jars, kafka_own_jars = classify_jars(libs_dir)
        print_jar_overview(third_party_jars, kafka_own_jars)

        index = collect_notices(third_party_jars)
        print_notice_inventory(index)

        header, body = split_kafka_header(notice_binary_text)
        body_start = len(header)
        body_first_line = notice_binary_text[:body_start].count("\n") + 1
        print(f"\nNOTICE-binary header preserved ({len(header)} chars); "
              f"third-party body to validate ({len(body)} chars, "
              f"starting at line {body_first_line}).")

        mask, matched, unmatched = match_notices(body, index)
        print_matched(matched)
        print_unmatched(unmatched, len(index.notice_to_jars))

        leftover = find_leftover_regions(body, mask, notice_binary_text, body_start)
        print_leftover(leftover)

        if unmatched or leftover:
            print("\nNOTICE-binary needs human review: upstream NOTICEs listed above "
                  "must be added, and leftover blocks should be re-checked.")
            sys.exit(1)
        else:
            print("\nNOTICE-binary fully matches upstream jar NOTICEs.")


if __name__ == "__main__":
    main()
