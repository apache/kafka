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

"""Reuse GitHub code-search evidence by exact query identity.

This is a local snapshot transformation. It never contacts GitHub. Every reused raw
response and header file is checksum-verified against its source metadata before the
target record is written.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
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


def atomic_copy(source: Path, target: Path) -> None:
    temporary = target.with_suffix(target.suffix + ".tmp")
    shutil.copyfile(source, temporary)
    temporary.replace(target)


def atomic_json(value: dict, target: Path) -> None:
    temporary = target.with_suffix(target.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output_file:
        json.dump(value, output_file, indent=2, sort_keys=False)
        output_file.write("\n")
    temporary.replace(target)


def load_tsv(path: Path) -> dict[int, dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        return {int(row["index"]): row for row in reader}


def verify_source(source_dir: Path) -> tuple[dict, str, dict, dict]:
    manifest_path = source_dir / "snapshot-manifest.json"
    manifest = load_json(manifest_path)
    if not manifest.get("complete") or manifest.get("pending_queries") != 0:
        raise ValueError(f"Cannot reuse incomplete snapshot: {source_dir}")
    if manifest.get("incomplete_result_queries") != 0:
        raise ValueError(f"Cannot reuse incomplete GitHub results: {source_dir}")
    files = {
        "results_tsv": source_dir / "results.tsv",
        "observed_results_tsv": source_dir / "observed-results.tsv",
        "evidence_files_tsv": source_dir / "evidence-files.tsv",
    }
    for key, path in files.items():
        expected = manifest["output_sha256"][key]
        if sha256(path) != expected:
            raise ValueError(f"Source aggregate checksum mismatch: {path}")
    evidence_rows = load_tsv(files["evidence_files_tsv"])
    result_rows = load_tsv(files["results_tsv"])
    if len(evidence_rows) != manifest["completed_queries"] \
            or len(result_rows) != manifest["completed_queries"]:
        raise ValueError(f"Source aggregate coverage mismatch: {source_dir}")
    return manifest, sha256(manifest_path), evidence_rows, result_rows


def target_record_is_valid(
    index: int,
    row: dict[str, str],
    raw_path: Path,
    header_path: Path,
    metadata_path: Path,
    args: argparse.Namespace,
) -> bool:
    """Keep any already-valid target evidence instead of replacing it with reuse."""
    if not raw_path.is_file() or not header_path.is_file() or not metadata_path.is_file():
        return False
    try:
        metadata = load_json(metadata_path)
        expected = {
            "schema": args.schema,
            "kafka_sha": args.kafka_sha,
            "query_manifest_sha256": args.query_manifest_sha256,
            "inventory_sha256": args.inventory_sha256,
            "input_manifest_sha256": args.input_manifest_sha256,
            "scanner_sha256": args.scanner_sha256,
            "index": index,
            "class": row["class"],
            "query_version": row["query_version"],
            "query_kind": row["query_kind"],
            "query": row["query"],
            "http_status": 200,
            "incomplete_results": False,
        }
        if any(metadata.get(key) != value for key, value in expected.items()):
            return False
        if sha256(raw_path) != metadata.get("raw_sha256") \
                or sha256(header_path) != metadata.get("header_sha256"):
            return False
        raw = load_json(raw_path)
        if raw.get("total_count") != metadata.get("total_count") \
                or raw.get("incomplete_results") is not False \
                or not isinstance(raw.get("items"), list):
            return False
        items = raw["items"]
        if len(items) != metadata.get("observed_items"):
            return False
        observed_repositories = len({
            item["repository"]["full_name"]
            for item in items
        })
        if observed_repositories != metadata.get("observed_repositories"):
            return False
        origin = metadata.get("evidence_origin", {}).get("type", "github_api")
        if origin == "github_api":
            return True
        if origin != "reused_snapshot":
            return False
        required = (
            "source_index",
            "source_metadata_sha256",
            "source_snapshot_manifest_sha256",
            "source_kafka_sha",
        )
        return all(metadata["evidence_origin"].get(field) not in (None, "") for field in required)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--marker", type=Path, required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--github-api-version", required=True)
    parser.add_argument("--per-page", type=int, required=True)
    parser.add_argument("--kafka-sha", required=True)
    parser.add_argument("--query-manifest-sha256", required=True)
    parser.add_argument("--inventory-sha256", required=True)
    parser.add_argument("--input-manifest-sha256", required=True)
    parser.add_argument("--scanner-sha256", required=True)
    parser.add_argument("--reuse-output-dir", action="append", type=Path, default=[])
    args = parser.parse_args()

    target_manifest_path = args.output_dir / "snapshot-manifest.json"
    if target_manifest_path.is_file():
        target_manifest = load_json(target_manifest_path)
        expected_target_lineage = {
            "schema": args.schema,
            "kafka_sha": args.kafka_sha,
            "query_manifest_sha256": args.query_manifest_sha256,
            "inventory_sha256": args.inventory_sha256,
            "input_manifest_sha256": args.input_manifest_sha256,
            "scanner_sha256": args.scanner_sha256,
            "github_api_version": args.github_api_version,
            "per_page": args.per_page,
        }
        mismatches = [
            key
            for key, value in expected_target_lineage.items()
            if target_manifest.get(key) != value
        ]
        if mismatches:
            raise ValueError(
                "Existing target snapshot has different lineage; refusing to "
                f"overwrite it ({', '.join(mismatches)}): {args.output_dir}"
            )

    reusable: dict[tuple[str, str, str, str], tuple[tuple, Path, dict, str, Path]] = {}
    for source_dir in args.reuse_output_dir:
        manifest, manifest_sha, evidence_rows, result_rows = verify_source(source_dir)
        if manifest.get("schema") != args.schema \
                or manifest.get("github_api_version") != args.github_api_version \
                or manifest.get("per_page") != args.per_page:
            raise ValueError(f"Reusable snapshot API/schema mismatch: {source_dir}")
        for metadata_path in sorted((source_dir / "metadata").glob("*.json")):
            metadata = load_json(metadata_path)
            if metadata.get("schema") != manifest.get("schema") \
                    or metadata.get("kafka_sha") != manifest.get("kafka_sha") \
                    or metadata.get("query_manifest_sha256") != manifest.get("query_manifest_sha256") \
                    or metadata.get("http_status") != 200 \
                    or metadata.get("incomplete_results") is not False:
                raise ValueError(f"Source metadata lineage mismatch: {metadata_path}")
            source_index = int(metadata["index"])
            evidence_row = evidence_rows.get(source_index)
            result_row = result_rows.get(source_index)
            if metadata_path.stem != f"{source_index:05d}" \
                    or evidence_row is None or result_row is None \
                    or evidence_row["class"] != metadata["class"] \
                    or evidence_row["metadata_sha256"] != sha256(metadata_path) \
                    or evidence_row["raw_sha256"] != metadata["raw_sha256"] \
                    or evidence_row["header_sha256"] != metadata["header_sha256"] \
                    or result_row["class"] != metadata["class"] \
                    or result_row["query_version"] != metadata["query_version"] \
                    or result_row["query"] != metadata["query"] \
                    or result_row["raw_sha256"] != metadata["raw_sha256"] \
                    or int(result_row["total_count"]) != metadata["total_count"]:
                raise ValueError(f"Source metadata is absent from its audit roots: {metadata_path}")
            if "query_kind" in result_row \
                    and result_row["query_kind"] != metadata["query_kind"]:
                raise ValueError(f"Source query-kind mismatch: {metadata_path}")
            key = (
                metadata["query_kind"],
                metadata["class"],
                metadata["query_version"],
                metadata["query"],
            )
            # Prefer evidence produced from the target Kafka SHA. For otherwise
            # equivalent sources, prefer the earliest completion time and then the
            # manifest checksum. This makes reuse independent of CLI directory order.
            priority = (
                0 if manifest["kafka_sha"] == args.kafka_sha else 1,
                metadata["completed_at"],
                manifest_sha,
            )
            existing = reusable.get(key)
            candidate = (priority, source_dir, manifest, manifest_sha, metadata_path)
            if existing is None or priority < existing[0]:
                reusable[key] = candidate

    raw_dir = args.output_dir / "raw"
    header_dir = args.output_dir / "headers"
    metadata_dir = args.output_dir / "metadata"
    for directory in (raw_dir, header_dir, metadata_dir, args.marker.parent):
        directory.mkdir(parents=True, exist_ok=True)

    completed_indices: list[int] = []
    preserved_count = 0
    reused_count = 0
    with args.query_manifest.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file, delimiter="\t", quoting=csv.QUOTE_NONE)
        for index, row in enumerate(reader, start=1):
            target_stem = f"{index:05d}"
            target_raw = raw_dir / f"{target_stem}.json"
            target_header = header_dir / f"{target_stem}.headers"
            target_metadata_path = metadata_dir / f"{target_stem}.json"
            if target_record_is_valid(
                index,
                row,
                target_raw,
                target_header,
                target_metadata_path,
                args,
            ):
                completed_indices.append(index)
                preserved_count += 1
                continue
            key = (row["query_kind"], row["class"], row["query_version"], row["query"])
            source = reusable.get(key)
            if source is None:
                continue
            _, source_dir, source_manifest, source_manifest_sha, source_metadata_path = source
            source_metadata = load_json(source_metadata_path)
            source_index = int(source_metadata["index"])
            source_stem = f"{source_index:05d}"
            source_raw = source_dir / "raw" / f"{source_stem}.json"
            source_header = source_dir / "headers" / f"{source_stem}.headers"
            if sha256(source_raw) != source_metadata["raw_sha256"]:
                raise ValueError(f"Source raw checksum mismatch: {source_raw}")
            if sha256(source_header) != source_metadata["header_sha256"]:
                raise ValueError(f"Source header checksum mismatch: {source_header}")

            atomic_copy(source_raw, target_raw)
            atomic_copy(source_header, target_header)

            target_metadata = dict(source_metadata)
            target_metadata.update(
                {
                    "schema": args.schema,
                    "kafka_sha": args.kafka_sha,
                    "query_manifest_sha256": args.query_manifest_sha256,
                    "inventory_sha256": args.inventory_sha256,
                    "input_manifest_sha256": args.input_manifest_sha256,
                    "scanner_sha256": args.scanner_sha256,
                    "index": index,
                    "evidence_origin": {
                        "type": "reused_snapshot",
                        "source_index": source_index,
                        "source_metadata_sha256": sha256(source_metadata_path),
                        "source_snapshot_manifest_sha256": source_manifest_sha,
                        "source_kafka_sha": source_manifest["kafka_sha"],
                    },
                }
            )
            atomic_json(target_metadata, target_metadata_path)
            completed_indices.append(index)
            reused_count += 1

    marker_tmp = args.marker.with_suffix(args.marker.suffix + ".tmp")
    with marker_tmp.open("w", encoding="utf-8") as output_file:
        for index in completed_indices:
            output_file.write(f"{index}\n")
    marker_tmp.replace(args.marker)
    print(
        f"Prepared {len(completed_indices)} completed records "
        f"({preserved_count} preserved, {reused_count} reused by exact query identity)."
    )


if __name__ == "__main__":
    main()
