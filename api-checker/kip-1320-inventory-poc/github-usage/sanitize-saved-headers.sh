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

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
OUTPUT_DIR=${OUTPUT_DIR:-"$SCRIPT_DIR/output"}
HEADER_DIR="$OUTPUT_DIR/headers"
META_DIR="$OUTPUT_DIR/metadata"
RAW_DIR="$OUTPUT_DIR/raw"
WORK_DIR="$OUTPUT_DIR/.sanitize-headers.$$"
LOCK_DIR="$OUTPUT_DIR/.crawl.lock"

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

sanitize_response_headers() {
    local input_file=$1
    local output_file=$2
    awk '
        BEGIN {
            allowed["date"] = 1
            allowed["deprecation"] = 1
            allowed["sunset"] = 1
            allowed["warning"] = 1
            allowed["retry-after"] = 1
            allowed["x-github-api-version-selected"] = 1
            allowed["x-github-request-id"] = 1
            allowed["x-ratelimit-limit"] = 1
            allowed["x-ratelimit-remaining"] = 1
            allowed["x-ratelimit-reset"] = 1
            allowed["x-ratelimit-resource"] = 1
        }
        {
            line = $0
            sub(/\r$/, "", line)
            if (line ~ /^HTTP\//) {
                print line
                next
            }
            colon = index(line, ":")
            if (colon > 0) {
                name = tolower(substr(line, 1, colon - 1))
                if (allowed[name]) print line
            }
        }
        END { print "" }
    ' "$input_file" >"$output_file"
}

cleanup() {
    rm -rf "$WORK_DIR"
    rmdir "$LOCK_DIR" 2>/dev/null || true
}

if [[ ! -d "$HEADER_DIR" || ! -d "$META_DIR" || ! -d "$RAW_DIR" ]]; then
    echo "Missing saved evidence directories under $OUTPUT_DIR." >&2
    exit 1
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Another crawl or migration appears to be running: $LOCK_DIR" >&2
    exit 1
fi
trap cleanup EXIT INT TERM

mkdir -p "$WORK_DIR/headers" "$WORK_DIR/metadata"

count=0
for meta_file in "$META_DIR"/*.json; do
    stem=$(basename "$meta_file" .json)
    header_file="$HEADER_DIR/$stem.headers"
    raw_file="$RAW_DIR/$stem.json"
    if [[ ! -s "$header_file" || ! -s "$raw_file" ]]; then
        echo "Missing header/raw evidence for $stem." >&2
        exit 1
    fi
    if [[ "$(sha256 "$header_file")" != "$(jq -r '.header_sha256' "$meta_file")" ]] \
        || [[ "$(sha256 "$raw_file")" != "$(jq -r '.raw_sha256' "$meta_file")" ]]; then
        echo "Existing evidence checksum mismatch for $stem." >&2
        exit 1
    fi

    sanitized_header="$WORK_DIR/headers/$stem.headers"
    sanitized_meta="$WORK_DIR/metadata/$stem.json"
    sanitize_response_headers "$header_file" "$sanitized_header"
    if rg -q -i '^(authorization|cookie|set-cookie|x-oauth-scopes|x-accepted-oauth-scopes|x-oauth-client-id):' \
        "$sanitized_header"; then
        echo "Sensitive or unnecessary authentication header remains in $stem." >&2
        exit 1
    fi
    new_header_sha=$(sha256 "$sanitized_header")
    jq --arg header_sha "$new_header_sha" \
        '.header_sha256 = $header_sha' "$meta_file" >"$sanitized_meta"
    count=$((count + 1))
done

expected=$(find "$META_DIR" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')
if (( count != expected || count == 0 )); then
    echo "Sanitized $count records; expected $expected." >&2
    exit 1
fi

for sanitized_header in "$WORK_DIR/headers"/*.headers; do
    mv "$sanitized_header" "$HEADER_DIR/$(basename "$sanitized_header")"
done
for sanitized_meta in "$WORK_DIR/metadata"/*.json; do
    mv "$sanitized_meta" "$META_DIR/$(basename "$sanitized_meta")"
done

echo "Sanitized $count saved response headers and updated metadata checksums."
echo "Run crawl.sh once to rebuild aggregate audit roots without new search requests."
