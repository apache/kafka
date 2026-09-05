#!/usr/bin/env bash

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

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
POC_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
JAVA_POC_DIR=${1:-${JAVA_POC_DIR:-}}

if [[ -z "$JAVA_POC_DIR" ]]; then
    echo "Usage: $0 /path/to/java-only-poc" >&2
    exit 1
fi

# shellcheck source=../inputs.env
source "$POC_DIR/inputs.env"

SOURCE_INPUTS="$JAVA_POC_DIR/output"
SOURCE_EVIDENCE="$JAVA_POC_DIR/github-usage/output"
SOURCE_MANIFEST="$SOURCE_EVIDENCE/snapshot-manifest.json"
SOURCE_INVENTORY="$SOURCE_INPUTS/inventory.tsv"
SOURCE_QUERY_MANIFEST="$SOURCE_INPUTS/query-manifest.tsv"
SOURCE_INPUT_MANIFEST="$SOURCE_INPUTS/input-manifest.tsv"

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

for file in "$SOURCE_MANIFEST" "$SOURCE_INVENTORY" "$SOURCE_QUERY_MANIFEST" \
    "$SOURCE_INPUT_MANIFEST" "$SOURCE_EVIDENCE/results.tsv" \
    "$SOURCE_EVIDENCE/observed-results.tsv" "$SOURCE_EVIDENCE/evidence-files.tsv"; do
    if [[ ! -f "$file" ]]; then
        echo "Missing source snapshot input: $file" >&2
        exit 1
    fi
done

if ! jq -e '.complete == true and .pending_queries == 0 and .incomplete_result_queries == 0' \
    "$SOURCE_MANIFEST" >/dev/null; then
    echo "Source Java snapshot is incomplete." >&2
    exit 1
fi
if [[ "$(sha256 "$SOURCE_QUERY_MANIFEST")" != "$(jq -r '.query_manifest_sha256' "$SOURCE_MANIFEST")" ]] \
    || [[ "$(sha256 "$SOURCE_INVENTORY")" != "$(jq -r '.inventory_sha256' "$SOURCE_MANIFEST")" ]] \
    || [[ "$(sha256 "$SOURCE_INPUT_MANIFEST")" != "$(jq -r '.input_manifest_sha256' "$SOURCE_MANIFEST")" ]]; then
    echo "Source Java snapshot input lineage does not match its manifest." >&2
    exit 1
fi

for entry in \
    "results_tsv:$SOURCE_EVIDENCE/results.tsv" \
    "observed_results_tsv:$SOURCE_EVIDENCE/observed-results.tsv" \
    "evidence_files_tsv:$SOURCE_EVIDENCE/evidence-files.tsv"; do
    key=${entry%%:*}
    file=${entry#*:}
    expected=$(jq -r ".output_sha256.$key" "$SOURCE_MANIFEST")
    if [[ "$(sha256 "$file")" != "$expected" ]]; then
        echo "Source snapshot checksum mismatch: $file" >&2
        exit 1
    fi
done

KAFKA_SHA=$(jq -r '.kafka_sha' "$SOURCE_MANIFEST")
TARGET_ROOT=${TARGET_ROOT:-"$SCRIPT_DIR/snapshots/$KAFKA_SHA"}
TARGET_INPUTS=${TARGET_INPUTS:-"$TARGET_ROOT/inputs"}
mkdir -p "$TARGET_INPUTS"

cp "$SOURCE_INVENTORY" "$TARGET_INPUTS/inventory.tsv"
cp "$SOURCE_INPUT_MANIFEST" "$TARGET_INPUTS/input-manifest.tsv"

expected_header=$'snapshot_sha\tinput_manifest_sha256\tscanner_sha256\tclass\tbinary_name\tartifact\tsource_path\tsource_line\tquery_version\tquery_kind\tquery'
if [[ "$(sed -n '1p' "$SOURCE_QUERY_MANIFEST")" != "$expected_header" ]]; then
    echo "Unexpected source query-manifest header." >&2
    exit 1
fi
source_input_manifest_sha=$(jq -r '.input_manifest_sha256' "$SOURCE_MANIFEST")
source_scanner_sha=$(jq -r '.scanner_sha256' "$SOURCE_MANIFEST")
if ! awk -F '\t' \
    -v kafka_sha="$KAFKA_SHA" \
    -v input_manifest_sha="$source_input_manifest_sha" \
    -v scanner_sha="$source_scanner_sha" \
    -v java_version="$JAVA_QUERY_VERSION" '
    NR == 1 { next }
    $1 != kafka_sha || $2 != input_manifest_sha || $3 != scanner_sha \
        || $9 != java_version || $10 != "java_exact_import" { exit 1 }
' "$SOURCE_QUERY_MANIFEST"; then
    echo "Source query manifest contains mixed or unexpected lineage." >&2
    exit 1
fi

awk -F '\t' -v OFS='\t' \
    -v scala_version="$SCALA_QUERY_VERSION" '
    NR == 1 {
        print
        next
    }
    {
        print
        $9 = scala_version
        $10 = "scala_exact_import"
        $11 = "\"import " $4 "\" language:Scala NOT repo:apache/kafka NOT is:fork"
        print
    }
' "$SOURCE_QUERY_MANIFEST" >"$TARGET_INPUTS/query-manifest.tsv"

source_query_count=$(($(wc -l <"$SOURCE_QUERY_MANIFEST") - 1))
target_query_count=$(($(wc -l <"$TARGET_INPUTS/query-manifest.tsv") - 1))
if (( target_query_count != source_query_count * 2 )); then
    echo "Combined query manifest does not contain exactly two queries per class." >&2
    exit 1
fi

query_input_manifest_sha=$(awk -F '\t' 'NR == 2 { print $2 }' "$TARGET_INPUTS/query-manifest.tsv")
query_scanner_sha=$(awk -F '\t' 'NR == 2 { print $3 }' "$TARGET_INPUTS/query-manifest.tsv")
if [[ "$(sha256 "$TARGET_INPUTS/input-manifest.tsv")" != "$query_input_manifest_sha" ]]; then
    echo "Source input-manifest checksum does not match its query lineage." >&2
    exit 1
fi
if [[ "$(sha256 "$TARGET_INPUTS/inventory.tsv")" != "$(jq -r '.inventory_sha256' "$SOURCE_MANIFEST")" ]]; then
    echo "Source inventory checksum does not match its snapshot lineage." >&2
    exit 1
fi

cat >"$TARGET_INPUTS/crawl-inputs.env" <<EOF
KAFKA_SHA=$KAFKA_SHA
QUERY_MANIFEST_SHA256=$(sha256 "$TARGET_INPUTS/query-manifest.tsv")
INVENTORY_SHA256=$(sha256 "$TARGET_INPUTS/inventory.tsv")
QUERY_SET_VERSION=$QUERY_SET_VERSION
JAVA_QUERY_VERSION=$JAVA_QUERY_VERSION
SCALA_QUERY_VERSION=$SCALA_QUERY_VERSION
SNAPSHOT_SCHEMA=$SNAPSHOT_SCHEMA
GITHUB_API_VERSION=$GITHUB_API_VERSION
PER_PAGE=$PER_PAGE
EOF

cat >"$TARGET_INPUTS/reuse-inputs.env" <<EOF
SOURCE_JAVA_EVIDENCE=$SOURCE_EVIDENCE
SOURCE_JAVA_SNAPSHOT_MANIFEST_SHA256=$(sha256 "$SOURCE_MANIFEST")
SOURCE_JAVA_KAFKA_SHA=$KAFKA_SHA
SOURCE_JAVA_QUERY_COUNT=$source_query_count
TARGET_COMBINED_QUERY_COUNT=$target_query_count
EOF

echo "Prepared $target_query_count Java and Scala queries at $TARGET_INPUTS"
echo "Kafka SHA: $KAFKA_SHA"
echo "The source Java snapshot remains unchanged and is reused by exact query identity."
