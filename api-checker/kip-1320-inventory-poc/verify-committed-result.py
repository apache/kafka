#!/usr/bin/env python3

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
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_tsv(path):
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source, delimiter="\t"))


def verify_checksums(result_dir):
    checksum_path = result_dir / "SHA256SUMS"
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        expected, relative_path = line.split("  ", 1)
        actual = sha256(result_dir / relative_path)
        if actual != expected:
            raise ValueError(
                f"Checksum mismatch for {relative_path}: {actual} != {expected}"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    result_dir = args.result_dir.resolve()

    verify_checksums(result_dir)
    manifest = json.loads(
        (result_dir / "selected-result-manifest.json").read_text(encoding="utf-8")
    )
    candidates = read_tsv(result_dir / "selected-candidates.tsv")
    query_results = read_tsv(
        result_dir / "selected-candidate-github-results.tsv"
    )

    expected_candidates = manifest["selected_candidates"]
    expected_queries = manifest["selected_query_results"]
    if len(candidates) != expected_candidates:
        raise ValueError(f"Expected {expected_candidates} candidates, found {len(candidates)}")
    if len(query_results) != expected_queries:
        raise ValueError(f"Expected {expected_queries} query rows, found {len(query_results)}")

    results_by_class = defaultdict(dict)
    for row in query_results:
        if row["incomplete_results"] != "false":
            raise ValueError(f"Incomplete GitHub result for {row['class']}")
        kind = row["query_kind"]
        if kind in results_by_class[row["class"]]:
            raise ValueError(f"Duplicate {kind} result for {row['class']}")
        results_by_class[row["class"]][kind] = row

    expected_kinds = {"java_exact_import", "scala_exact_import"}
    for candidate in candidates:
        class_name = candidate["class"]
        class_results = results_by_class.pop(class_name, {})
        if set(class_results) != expected_kinds:
            raise ValueError(
                f"Expected Java and Scala results for {class_name}, found {set(class_results)}"
            )
        java_count = int(class_results["java_exact_import"]["total_count"])
        scala_count = int(class_results["scala_exact_import"]["total_count"])
        combined_count = int(candidate["combined_import_files"])
        if combined_count != java_count + scala_count:
            raise ValueError(
                f"Count mismatch for {class_name}: {combined_count} != "
                f"{java_count} + {scala_count}"
            )
        if combined_count < manifest["usage_threshold"]:
            raise ValueError(f"{class_name} is below the selection threshold")

    if results_by_class:
        raise ValueError(
            "GitHub results contain unselected classes: "
            + ", ".join(sorted(results_by_class))
        )

    print(
        "Verified "
        f"{len(candidates)} candidates and {len(query_results)} GitHub query results "
        f"for Kafka {manifest['kafka_sha']}."
    )


if __name__ == "__main__":
    main()
