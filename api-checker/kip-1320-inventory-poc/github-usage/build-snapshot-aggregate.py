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

"""Validate query evidence and build deterministic aggregate snapshot files."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        for block in iter(lambda: input_file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as input_file:
        return json.load(input_file)


def write_tsv(output_file, values) -> None:
    output_file.write("\t".join(str(value) for value in values) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--kafka-sha", required=True)
    parser.add_argument("--query-manifest-sha256", required=True)
    parser.add_argument("--inventory-sha256", required=True)
    parser.add_argument("--query-set-version", required=True)
    parser.add_argument("--github-api-version", required=True)
    parser.add_argument("--per-page", type=int, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    temporary_dir = args.output_dir / ".aggregate-tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    results_path = temporary_dir / "results.tsv"
    observed_path = temporary_dir / "observed-results.tsv"
    evidence_path = temporary_dir / "evidence-files.tsv"

    completed_times: list[str] = []
    started_times: list[str] = []
    deprecations: set[str] = set()
    sunsets: set[str] = set()
    origins = {"github_api": 0, "reused_snapshot": 0}
    incomplete_queries = 0
    completed_queries = 0
    total_queries = 0
    input_manifest_sha = None
    scanner_sha = None

    with args.query_manifest.open(newline="", encoding="utf-8") as manifest_file, \
            results_path.open("w", encoding="utf-8") as results_file, \
            observed_path.open("w", encoding="utf-8") as observed_file, \
            evidence_path.open("w", encoding="utf-8") as evidence_file:
        reader = csv.DictReader(manifest_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        write_tsv(results_file, [
            "index", "class", "query_version", "query_kind", "total_count",
            "incomplete_results", "observed_items", "observed_repositories",
            "started_at", "completed_at", "raw_sha256", "github_request_id", "query",
        ])
        write_tsv(observed_file, [
            "index", "class", "repository", "path", "blob_sha", "html_url",
        ])
        write_tsv(evidence_file, [
            "index", "class", "metadata_sha256", "raw_sha256", "header_sha256",
        ])

        for index, row in enumerate(reader, start=1):
            total_queries = index
            if row["snapshot_sha"] != args.kafka_sha:
                raise ValueError(f"Kafka SHA mismatch at query {index}")
            if input_manifest_sha is None:
                input_manifest_sha = row["input_manifest_sha256"]
                scanner_sha = row["scanner_sha256"]
            if row["input_manifest_sha256"] != input_manifest_sha \
                    or row["scanner_sha256"] != scanner_sha:
                raise ValueError("Query manifest contains mixed input/scanner lineage")

            stem = f"{index:05d}"
            raw_path = args.output_dir / "raw" / f"{stem}.json"
            header_path = args.output_dir / "headers" / f"{stem}.headers"
            metadata_path = args.output_dir / "metadata" / f"{stem}.json"
            evidence_exists = [raw_path.exists(), header_path.exists(), metadata_path.exists()]
            if not any(evidence_exists):
                continue
            if not all(evidence_exists):
                raise ValueError(f"Partial evidence-file set for query {index}")
            metadata = load_json(metadata_path)
            raw_sha = sha256(raw_path)
            header_sha = sha256(header_path)
            expected = {
                "schema": args.schema,
                "kafka_sha": args.kafka_sha,
                "query_manifest_sha256": args.query_manifest_sha256,
                "inventory_sha256": args.inventory_sha256,
                "input_manifest_sha256": input_manifest_sha,
                "scanner_sha256": scanner_sha,
                "index": index,
                "class": row["class"],
                "query_version": row["query_version"],
                "query_kind": row["query_kind"],
                "query": row["query"],
                "http_status": 200,
            }
            for key, value in expected.items():
                if metadata.get(key) != value:
                    raise ValueError(f"Metadata mismatch for query {index}: {key}")
            if metadata.get("raw_sha256") != raw_sha:
                raise ValueError(f"Raw checksum mismatch for query {index}")
            if metadata.get("header_sha256") != header_sha:
                raise ValueError(f"Header checksum mismatch for query {index}")
            if not isinstance(metadata.get("incomplete_results"), bool):
                raise ValueError(f"Invalid incomplete-results value for query {index}")
            if metadata["incomplete_results"]:
                incomplete_queries += 1

            raw = load_json(raw_path)
            if raw.get("total_count") != metadata.get("total_count") \
                    or raw.get("incomplete_results") != metadata["incomplete_results"]:
                raise ValueError(f"Raw response mismatch for query {index}")
            items = raw.get("items")
            if not isinstance(items, list) or len(items) != metadata.get("observed_items"):
                raise ValueError(f"Observed item mismatch for query {index}")
            observed_repositories = len({item["repository"]["full_name"] for item in items})
            if observed_repositories != metadata.get("observed_repositories"):
                raise ValueError(f"Observed repository mismatch for query {index}")

            write_tsv(results_file, [
                index, row["class"], row["query_version"], row["query_kind"],
                metadata["total_count"], str(metadata["incomplete_results"]).lower(),
                metadata["observed_items"], metadata["observed_repositories"],
                metadata["started_at"], metadata["completed_at"], raw_sha,
                metadata.get("github_request_id", ""), row["query"],
            ])
            observed_rows = sorted(
                (
                    item["repository"]["full_name"], item["path"], item["sha"],
                    item["html_url"],
                )
                for item in items
            )
            for repository, path, blob_sha, html_url in observed_rows:
                write_tsv(observed_file, [
                    index, row["class"], repository, path, blob_sha, html_url,
                ])
            write_tsv(evidence_file, [
                index, row["class"], sha256(metadata_path), raw_sha, header_sha,
            ])

            completed_times.append(metadata["completed_at"])
            lifecycle = metadata.get("api_lifecycle", {})
            if lifecycle.get("deprecation"):
                deprecations.add(lifecycle["deprecation"])
            if lifecycle.get("sunset"):
                sunsets.add(lifecycle["sunset"])
            origin = metadata.get("evidence_origin", {}).get("type", "github_api")
            if origin == "reused_snapshot":
                required_origin_fields = {
                    "source_index", "source_metadata_sha256",
                    "source_snapshot_manifest_sha256", "source_kafka_sha",
                }
                if not required_origin_fields.issubset(metadata["evidence_origin"]) \
                        or any(metadata["evidence_origin"][field] in (None, "")
                               for field in required_origin_fields):
                    raise ValueError(f"Incomplete reuse provenance for query {index}")
            elif origin != "github_api":
                raise ValueError(f"Unknown evidence origin for query {index}: {origin}")
            origins[origin] = origins.get(origin, 0) + 1
            started_times.append(metadata["started_at"])
            completed_queries += 1

    if total_queries == 0:
        for temporary_path in (results_path, observed_path, evidence_path):
            temporary_path.unlink(missing_ok=True)
        temporary_dir.rmdir()
        raise ValueError("Query manifest contains no query records")

    final_results = args.output_dir / "results.tsv"
    final_observed = args.output_dir / "observed-results.tsv"
    final_evidence = args.output_dir / "evidence-files.tsv"
    results_path.replace(final_results)
    observed_path.replace(final_observed)
    evidence_path.replace(final_evidence)
    temporary_dir.rmdir()

    snapshot = {
        "schema": args.schema,
        "kafka_sha": args.kafka_sha,
        "query_manifest_sha256": args.query_manifest_sha256,
        "inventory_sha256": args.inventory_sha256,
        "input_manifest_sha256": input_manifest_sha,
        "scanner_sha256": scanner_sha,
        "query_set_version": args.query_set_version,
        "github_api_version": args.github_api_version,
        "per_page": args.per_page,
        "total_queries": total_queries,
        "completed_queries": completed_queries,
        "pending_queries": total_queries - completed_queries,
        "incomplete_result_queries": incomplete_queries,
        "complete": completed_queries == total_queries and incomplete_queries == 0,
        "evidence_origin": {
            "github_api_queries": origins.get("github_api", 0),
            "reused_snapshot_queries": origins.get("reused_snapshot", 0),
        },
        "first_completed_at": min(completed_times) if completed_times else None,
        "last_completed_at": max(completed_times) if completed_times else None,
        "first_started_at": min(started_times) if started_times else None,
        "api_lifecycle": {
            "deprecation": ";".join(sorted(deprecations)) or None,
            "sunset": ";".join(sorted(sunsets)) or None,
        },
        "output_sha256": {
            "results_tsv": sha256(final_results),
            "observed_results_tsv": sha256(final_observed),
            "evidence_files_tsv": sha256(final_evidence),
        },
    }
    snapshot_path = args.output_dir / "snapshot-manifest.json"
    atomic_path = snapshot_path.with_suffix(".json.tmp")
    with atomic_path.open("w", encoding="utf-8") as output_file:
        json.dump(snapshot, output_file, indent=2)
        output_file.write("\n")
    atomic_path.replace(snapshot_path)
    print(
        f"Built snapshot aggregate for {completed_queries}/{total_queries} "
        "validated query records."
    )


if __name__ == "__main__":
    main()
