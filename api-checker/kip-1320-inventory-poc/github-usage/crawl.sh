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
QUERY_MANIFEST=${QUERY_MANIFEST:-"$POC_DIR/output/query-manifest.tsv"}
INVENTORY=${INVENTORY:-"$POC_DIR/output/inventory.tsv"}
INPUTS_ENV=${INPUTS_ENV:-"$POC_DIR/output/crawl-inputs.env"}
MAX_QUERIES=${MAX_QUERIES:-0}
MAX_RETRIES=${MAX_RETRIES:-5}
REUSE_OUTPUT_DIRS=${REUSE_OUTPUT_DIRS:-}

if [[ ! -f "$INPUTS_ENV" ]]; then
    echo "Missing generated crawl inputs: $INPUTS_ENV" >&2
    echo "Run ../run.sh first." >&2
    exit 1
fi
# shellcheck source=/dev/null
source "$INPUTS_ENV"

OUTPUT_DIR=${OUTPUT_DIR:-"$SCRIPT_DIR/snapshots/$KAFKA_SHA/evidence"}

RAW_DIR="$OUTPUT_DIR/raw"
HEADER_DIR="$OUTPUT_DIR/headers"
META_DIR="$OUTPUT_DIR/metadata"
TMP_DIR="$OUTPUT_DIR/.tmp"
LOCK_DIR="$OUTPUT_DIR/.crawl.lock"
AUTH_HEADER="$TMP_DIR/.github-auth-header"

log() {
    printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

header_value() {
    local file=$1
    local header=$2
    awk -v wanted="$header" '
        {
            line = $0
            sub(/\r$/, "", line)
            colon = index(line, ":")
            if (colon > 0 && tolower(substr(line, 1, colon - 1)) == tolower(wanted)) {
                value = substr(line, colon + 1)
                sub(/^[[:space:]]+/, "", value)
                result = value
            }
        }
        END { print result }
    ' "$file"
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

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

cleanup() {
    rm -f "$AUTH_HEADER"
    rmdir "$LOCK_DIR" 2>/dev/null || true
}

for command in curl gh jq shasum awk sed sort python3; do
    require_command "$command"
done

if [[ ! -f "$QUERY_MANIFEST" ]]; then
    echo "Missing query manifest: $QUERY_MANIFEST" >&2
    exit 1
fi
if [[ ! -f "$INVENTORY" ]]; then
    echo "Missing inventory: $INVENTORY" >&2
    exit 1
fi

actual_query_manifest_sha=$(sha256 "$QUERY_MANIFEST")
if [[ "$actual_query_manifest_sha" != "$QUERY_MANIFEST_SHA256" ]]; then
    echo "Query manifest checksum mismatch." >&2
    echo "Expected: $QUERY_MANIFEST_SHA256" >&2
    echo "Actual:   $actual_query_manifest_sha" >&2
    exit 1
fi
actual_inventory_sha=$(sha256 "$INVENTORY")
if [[ "$actual_inventory_sha" != "$INVENTORY_SHA256" ]]; then
    echo "Inventory checksum mismatch." >&2
    echo "Expected: $INVENTORY_SHA256" >&2
    echo "Actual:   $actual_inventory_sha" >&2
    exit 1
fi

expected_header=$'snapshot_sha\tinput_manifest_sha256\tscanner_sha256\tclass\tbinary_name\tartifact\tsource_path\tsource_line\tquery_version\tquery_kind\tquery'
actual_header=$(sed -n '1p' "$QUERY_MANIFEST")
if [[ "$actual_header" != "$expected_header" ]]; then
    echo "Unexpected query manifest header." >&2
    exit 1
fi

query_input_manifest_sha=$(awk -F '\t' 'NR == 2 { print $2 }' "$QUERY_MANIFEST")
query_scanner_sha=$(awk -F '\t' 'NR == 2 { print $3 }' "$QUERY_MANIFEST")
if ! awk -F '\t' -v input_manifest_sha="$query_input_manifest_sha" -v scanner_sha="$query_scanner_sha" '
    NR > 1 && ($2 != input_manifest_sha || $3 != scanner_sha) { exit 1 }
' "$QUERY_MANIFEST"; then
    echo "Query manifest contains mixed input/scanner lineage." >&2
    exit 1
fi

mkdir -p "$RAW_DIR" "$HEADER_DIR" "$META_DIR" "$TMP_DIR"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Another crawl appears to be running: $LOCK_DIR" >&2
    exit 1
fi
trap cleanup EXIT INT TERM

ensure_github_auth() {
    local github_token
    if [[ -s "$AUTH_HEADER" ]]; then
        return
    fi
    github_token=$(gh auth token)
    if [[ -z "$github_token" ]]; then
        echo "GitHub CLI is not authenticated." >&2
        exit 1
    fi
    umask 077
    printf 'Authorization: Bearer %s\n' "$github_token" >"$AUTH_HEADER"
    chmod 600 "$AUTH_HEADER"
}

wait_for_code_search_slot() {
    local rate_json remaining reset now wait_seconds
    ensure_github_auth
    while true; do
        if ! rate_json=$(gh api rate_limit --jq '.resources.code_search'); then
            log "Unable to read GitHub rate limit; retrying in 5s."
            sleep 5
            continue
        fi
        remaining=$(jq -r '.remaining' <<<"$rate_json")
        reset=$(jq -r '.reset' <<<"$rate_json")
        if (( remaining > 0 )); then
            return
        fi
        now=$(date -u '+%s')
        wait_seconds=$((reset - now + 2))
        if (( wait_seconds < 1 )); then
            wait_seconds=1
        fi
        log "GitHub code-search limit exhausted; waiting ${wait_seconds}s."
        sleep "$wait_seconds"
    done
}

write_metadata() {
    local index=$1 class_name=$2 query_version=$3 query_kind=$4 query=$5
    local started_at=$6 completed_at=$7 http_status=$8 raw_file=$9 header_file=${10}
    local meta_file=${11}
    local origin_type=${12:-github_api}
    local source_index=${13:-}
    local source_metadata_sha=${14:-}
    local source_snapshot_manifest_sha=${15:-}
    local source_kafka_sha=${16:-}
    local total_count incomplete_results observed_items observed_repositories
    local raw_sha header_sha request_id rate_limit rate_remaining rate_reset
    local deprecation sunset warning

    total_count=$(jq -r '.total_count' "$raw_file")
    incomplete_results=$(jq -r '.incomplete_results' "$raw_file")
    observed_items=$(jq -r '.items | length' "$raw_file")
    observed_repositories=$(jq -r '[.items[].repository.full_name] | unique | length' "$raw_file")
    raw_sha=$(sha256 "$raw_file")
    header_sha=$(sha256 "$header_file")
    request_id=$(header_value "$header_file" "x-github-request-id")
    rate_limit=$(header_value "$header_file" "x-ratelimit-limit")
    rate_remaining=$(header_value "$header_file" "x-ratelimit-remaining")
    rate_reset=$(header_value "$header_file" "x-ratelimit-reset")
    deprecation=$(header_value "$header_file" "deprecation")
    sunset=$(header_value "$header_file" "sunset")
    warning=$(header_value "$header_file" "warning")

    jq -n \
        --arg schema "$SNAPSHOT_SCHEMA" \
        --arg kafka_sha "$KAFKA_SHA" \
        --arg query_manifest_sha256 "$QUERY_MANIFEST_SHA256" \
        --arg inventory_sha256 "$INVENTORY_SHA256" \
        --arg input_manifest_sha256 "$query_input_manifest_sha" \
        --arg scanner_sha256 "$query_scanner_sha" \
        --arg query_version "$query_version" \
        --arg query_kind "$query_kind" \
        --arg class "$class_name" \
        --arg query "$query" \
        --arg started_at "$started_at" \
        --arg completed_at "$completed_at" \
        --argjson index "$index" \
        --argjson http_status "$http_status" \
        --argjson total_count "$total_count" \
        --argjson incomplete_results "$incomplete_results" \
        --argjson observed_items "$observed_items" \
        --argjson observed_repositories "$observed_repositories" \
        --arg raw_sha256 "$raw_sha" \
        --arg header_sha256 "$header_sha" \
        --arg request_id "$request_id" \
        --arg rate_limit "$rate_limit" \
        --arg rate_remaining "$rate_remaining" \
        --arg rate_reset "$rate_reset" \
        --arg deprecation "$deprecation" \
        --arg sunset "$sunset" \
        --arg warning "$warning" \
        --arg origin_type "$origin_type" \
        --arg source_index "$source_index" \
        --arg source_metadata_sha "$source_metadata_sha" \
        --arg source_snapshot_manifest_sha "$source_snapshot_manifest_sha" \
        --arg source_kafka_sha "$source_kafka_sha" \
        '{
            schema: $schema,
            kafka_sha: $kafka_sha,
            query_manifest_sha256: $query_manifest_sha256,
            inventory_sha256: $inventory_sha256,
            input_manifest_sha256: $input_manifest_sha256,
            scanner_sha256: $scanner_sha256,
            index: $index,
            class: $class,
            query_version: $query_version,
            query_kind: $query_kind,
            query: $query,
            started_at: $started_at,
            completed_at: $completed_at,
            http_status: $http_status,
            total_count: $total_count,
            incomplete_results: $incomplete_results,
            observed_items: $observed_items,
            observed_repositories: $observed_repositories,
            raw_sha256: $raw_sha256,
            header_sha256: $header_sha256,
            github_request_id: $request_id,
            rate_limit: {
                limit: ($rate_limit | tonumber?),
                remaining: ($rate_remaining | tonumber?),
                reset_epoch: ($rate_reset | tonumber?)
            },
            api_lifecycle: {
                deprecation: (if $deprecation == "" then null else $deprecation end),
                sunset: (if $sunset == "" then null else $sunset end),
                warning: (if $warning == "" then null else $warning end)
            },
            evidence_origin: {
                type: $origin_type,
                source_index: (if $source_index == "" then null else ($source_index | tonumber) end),
                source_metadata_sha256: (if $source_metadata_sha == "" then null else $source_metadata_sha end),
                source_snapshot_manifest_sha256: (if $source_snapshot_manifest_sha == "" then null else $source_snapshot_manifest_sha end),
                source_kafka_sha: (if $source_kafka_sha == "" then null else $source_kafka_sha end)
            }
        }' >"$meta_file"
}

metadata_is_valid() {
    local meta_file=$1 raw_file=$2 header_file=$3 expected_index=$4
    local expected_class=$5 expected_query_version=$6 expected_query_kind=$7 expected_query=$8
    [[ -s "$meta_file" && -s "$raw_file" && -s "$header_file" ]] || return 1
    jq -e \
        --arg schema "$SNAPSHOT_SCHEMA" \
        --arg kafka_sha "$KAFKA_SHA" \
        --arg manifest_sha "$QUERY_MANIFEST_SHA256" \
        --argjson index "$expected_index" \
        --arg class "$expected_class" \
        --arg query_version "$expected_query_version" \
        --arg query_kind "$expected_query_kind" \
        --arg query "$expected_query" \
        '.schema == $schema
         and .kafka_sha == $kafka_sha
         and .query_manifest_sha256 == $manifest_sha
         and .index == $index
         and .class == $class
         and .query_version == $query_version
         and .query_kind == $query_kind
         and .query == $query
         and .http_status == 200
         and (.total_count | type == "number")
         and .incomplete_results == false
         and ((.evidence_origin.type // "github_api") == "github_api"
              or ((.evidence_origin.type == "reused_snapshot")
                  and (.evidence_origin.source_index | type == "number")
                  and (.evidence_origin.source_metadata_sha256 | type == "string" and length > 0)
                  and (.evidence_origin.source_snapshot_manifest_sha256 | type == "string" and length > 0)
                  and (.evidence_origin.source_kafka_sha | type == "string" and length > 0)))' \
        "$meta_file" >/dev/null || return 1
    [[ "$(sha256 "$raw_file")" == "$(jq -r '.raw_sha256' "$meta_file")" ]] || return 1
    [[ "$(sha256 "$header_file")" == "$(jq -r '.header_sha256' "$meta_file")" ]] || return 1
}

build_aggregate_outputs_legacy() {
    local results_tmp="$TMP_DIR/results.tsv"
    local observed_tmp="$TMP_DIR/observed-results.tsv"
    local evidence_files_tmp="$TMP_DIR/evidence-files.tsv"
    local valid_metadata_tmp="$TMP_DIR/valid-metadata.tsv"
    local snapshot_tmp="$TMP_DIR/snapshot-manifest.json"
    local completed=0 incomplete=0 total_queries
    local index=0 class_name query stem raw_file header_file meta_file
    local fresh_queries reused_queries

    printf 'index\tclass\tquery_version\tquery_kind\ttotal_count\tincomplete_results\tobserved_items\tobserved_repositories\tstarted_at\tcompleted_at\traw_sha256\tgithub_request_id\tquery\n' >"$results_tmp"
    printf 'index\tclass\trepository\tpath\tblob_sha\thtml_url\n' >"$observed_tmp"
    printf 'index\tclass\tmetadata_sha256\traw_sha256\theader_sha256\n' >"$evidence_files_tmp"
    printf 'started_at\tcompleted_at\tdeprecation\tsunset\torigin_type\n' >"$valid_metadata_tmp"

    total_queries=$(($(wc -l <"$QUERY_MANIFEST") - 1))
    while IFS=$'\034' read -r snapshot_sha input_manifest_sha scanner_sha class_name binary_name artifact source_path source_line query_version query_kind query; do
        index=$((index + 1))
        stem=$(printf '%05d' "$index")
        raw_file="$RAW_DIR/$stem.json"
        header_file="$HEADER_DIR/$stem.headers"
        meta_file="$META_DIR/$stem.json"
        if ! metadata_is_valid "$meta_file" "$raw_file" "$header_file" "$index" \
                "$class_name" "$query_version" "$query_kind" "$query"; then
            continue
        fi
        completed=$((completed + 1))
        if [[ "$(jq -r '.incomplete_results' "$meta_file")" == "true" ]]; then
            incomplete=$((incomplete + 1))
        fi
        jq -r '[
            .index,
            .class,
            .query_version,
            .query_kind,
            .total_count,
            .incomplete_results,
            .observed_items,
            .observed_repositories,
            .started_at,
            .completed_at,
            .raw_sha256,
            .github_request_id,
            .query
        ] | @tsv' "$meta_file" >>"$results_tmp"
        jq -r --argjson index "$index" --arg class "$class_name" '
            .items[]
            | [$index, $class, .repository.full_name, .path, .sha, .html_url]
            | @tsv
        ' "$raw_file" >>"$observed_tmp"
        printf '%s\t%s\t%s\t%s\t%s\n' \
            "$index" "$class_name" "$(sha256 "$meta_file")" \
            "$(sha256 "$raw_file")" "$(sha256 "$header_file")" >>"$evidence_files_tmp"
        jq -r '[
            .started_at,
            .completed_at,
            (.api_lifecycle.deprecation // ""),
            (.api_lifecycle.sunset // ""),
            (.evidence_origin.type // "github_api")
        ] | @tsv' "$meta_file" >>"$valid_metadata_tmp"
    done < <(sed -n '2,$p' "$QUERY_MANIFEST" | tr '\t' '\034')

    {
        sed -n '1p' "$results_tmp"
        sed -n '2,$p' "$results_tmp" | sort -t $'\t' -k1,1n
    } >"$OUTPUT_DIR/results.tsv"
    {
        sed -n '1p' "$observed_tmp"
        sed -n '2,$p' "$observed_tmp" | sort -t $'\t' -k1,1n -k3,3 -k4,4
    } >"$OUTPUT_DIR/observed-results.tsv"
    mv "$evidence_files_tmp" "$OUTPUT_DIR/evidence-files.tsv"

    first_started=$(sed -n '2,$p' "$valid_metadata_tmp" | cut -f1 | sort | sed -n '1p')
    first_completed=$(sed -n '2,$p' "$valid_metadata_tmp" | cut -f2 | sort | sed -n '1p')
    last_completed=$(sed -n '2,$p' "$valid_metadata_tmp" | cut -f2 | sort | tail -n 1)
    api_deprecation=$(sed -n '2,$p' "$valid_metadata_tmp" | cut -f3 | sed '/^$/d' | sort -u | paste -sd ';' -)
    api_sunset=$(sed -n '2,$p' "$valid_metadata_tmp" | cut -f4 | sed '/^$/d' | sort -u | paste -sd ';' -)
    fresh_queries=$(sed -n '2,$p' "$valid_metadata_tmp" | awk -F '\t' '$5 == "github_api" { count++ } END { print count + 0 }')
    reused_queries=$(sed -n '2,$p' "$valid_metadata_tmp" | awk -F '\t' '$5 == "reused_snapshot" { count++ } END { print count + 0 }')

    jq -n \
        --arg schema "$SNAPSHOT_SCHEMA" \
        --arg kafka_sha "$KAFKA_SHA" \
        --arg query_manifest_sha256 "$QUERY_MANIFEST_SHA256" \
        --arg inventory_sha256 "$INVENTORY_SHA256" \
        --arg input_manifest_sha256 "$query_input_manifest_sha" \
        --arg scanner_sha256 "$query_scanner_sha" \
        --arg query_set_version "$QUERY_SET_VERSION" \
        --arg github_api_version "$GITHUB_API_VERSION" \
        --arg first_started_at "$first_started" \
        --arg first_completed_at "$first_completed" \
        --arg last_completed_at "$last_completed" \
        --arg api_deprecation "$api_deprecation" \
        --arg api_sunset "$api_sunset" \
        --arg results_sha256 "$(sha256 "$OUTPUT_DIR/results.tsv")" \
        --arg observed_results_sha256 "$(sha256 "$OUTPUT_DIR/observed-results.tsv")" \
        --arg evidence_files_sha256 "$(sha256 "$OUTPUT_DIR/evidence-files.tsv")" \
        --argjson per_page "$PER_PAGE" \
        --argjson total_queries "$total_queries" \
        --argjson completed_queries "$completed" \
        --argjson incomplete_queries "$incomplete" \
        --argjson fresh_queries "$fresh_queries" \
        --argjson reused_queries "$reused_queries" \
        '{
            schema: $schema,
            kafka_sha: $kafka_sha,
            query_manifest_sha256: $query_manifest_sha256,
            inventory_sha256: $inventory_sha256,
            input_manifest_sha256: $input_manifest_sha256,
            scanner_sha256: $scanner_sha256,
            query_set_version: $query_set_version,
            github_api_version: $github_api_version,
            per_page: $per_page,
            total_queries: $total_queries,
            completed_queries: $completed_queries,
            pending_queries: ($total_queries - $completed_queries),
            incomplete_result_queries: $incomplete_queries,
            complete: ($completed_queries == $total_queries and $incomplete_queries == 0),
            evidence_origin: {
                github_api_queries: $fresh_queries,
                reused_snapshot_queries: $reused_queries
            },
            first_started_at: (if $first_started_at == "" then null else $first_started_at end),
            first_completed_at: (if $first_completed_at == "" then null else $first_completed_at end),
            last_completed_at: (if $last_completed_at == "" then null else $last_completed_at end),
            api_lifecycle: {
                deprecation: (if $api_deprecation == "" then null else $api_deprecation end),
                sunset: (if $api_sunset == "" then null else $api_sunset end)
            },
            output_sha256: {
                results_tsv: $results_sha256,
                observed_results_tsv: $observed_results_sha256,
                evidence_files_tsv: $evidence_files_sha256
            }
        }' >"$snapshot_tmp"
    mv "$snapshot_tmp" "$OUTPUT_DIR/snapshot-manifest.json"
}

build_snapshot_aggregate() {
    if [[ "${RUN_LEGACY_SHELL_EVIDENCE_AUDIT:-0}" == "1" ]]; then
        build_aggregate_outputs_legacy
        return
    fi
    python3 "$SCRIPT_DIR/build-snapshot-aggregate.py" \
        --query-manifest "$QUERY_MANIFEST" \
        --output-dir "$OUTPUT_DIR" \
        --schema "$SNAPSHOT_SCHEMA" \
        --kafka-sha "$KAFKA_SHA" \
        --query-manifest-sha256 "$QUERY_MANIFEST_SHA256" \
        --inventory-sha256 "$INVENTORY_SHA256" \
        --query-set-version "$QUERY_SET_VERSION" \
        --github-api-version "$GITHUB_API_VERSION" \
        --per-page "$PER_PAGE"
}

total_queries=$(($(wc -l <"$QUERY_MANIFEST") - 1))
processed_this_run=0
completed_total=0
prepared_this_run=0
index=0

reuse_marker="$TMP_DIR/completed-indices.tsv"
reuse_target_index=0
reuse_args=()
if [[ -n "$REUSE_OUTPUT_DIRS" ]]; then
    IFS=':' read -r -a reuse_dirs <<<"$REUSE_OUTPUT_DIRS"
    for reuse_dir in "${reuse_dirs[@]}"; do
        reuse_args+=(--reuse-output-dir "$reuse_dir")
    done
fi
python3 "$SCRIPT_DIR/reuse-snapshot-evidence.py" \
    --query-manifest "$QUERY_MANIFEST" \
    --output-dir "$OUTPUT_DIR" \
    --marker "$reuse_marker" \
    --schema "$SNAPSHOT_SCHEMA" \
    --github-api-version "$GITHUB_API_VERSION" \
    --per-page "$PER_PAGE" \
    --kafka-sha "$KAFKA_SHA" \
    --query-manifest-sha256 "$QUERY_MANIFEST_SHA256" \
    --inventory-sha256 "$INVENTORY_SHA256" \
    --input-manifest-sha256 "$query_input_manifest_sha" \
    --scanner-sha256 "$query_scanner_sha" \
    "${reuse_args[@]}"
prepared_this_run=$(wc -l <"$reuse_marker" | tr -d ' ')
if [[ -s "$reuse_marker" ]]; then
    exec 3<"$reuse_marker"
    if ! IFS= read -r reuse_target_index <&3; then
        reuse_target_index=0
    fi
else
    reuse_target_index=0
fi

log "Starting GitHub usage snapshot: $total_queries queries."

while IFS=$'\034' read -r snapshot_sha input_manifest_sha scanner_sha class_name binary_name artifact source_path source_line query_version query_kind query; do
    index=$((index + 1))

    if [[ "$snapshot_sha" != "$KAFKA_SHA" ]]; then
        echo "Kafka SHA mismatch at query $index: $class_name" >&2
        exit 1
    fi
    case "$query_kind" in
        java_exact_import)
            expected_query_version=$JAVA_QUERY_VERSION
            ;;
        scala_exact_import)
            expected_query_version=$SCALA_QUERY_VERSION
            ;;
        *)
            echo "Unknown query kind at query $index: $query_kind" >&2
            exit 1
            ;;
    esac
    if [[ "$query_version" != "$expected_query_version" ]]; then
        echo "Query version mismatch at query $index: $class_name" >&2
        exit 1
    fi

    stem=$(printf '%05d' "$index")
    raw_file="$RAW_DIR/$stem.json"
    header_file="$HEADER_DIR/$stem.headers"
    meta_file="$META_DIR/$stem.json"

    if (( reuse_target_index == index )); then
        completed_total=$((completed_total + 1))
        if ! IFS= read -r reuse_target_index <&3; then
            reuse_target_index=0
        fi
        continue
    fi

    if (( MAX_QUERIES > 0 && processed_this_run >= MAX_QUERIES )); then
        break
    fi

    attempt=1
    while (( attempt <= MAX_RETRIES )); do
        wait_for_code_search_slot
        started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
        raw_tmp="$TMP_DIR/$stem.raw"
        header_tmp="$TMP_DIR/$stem.headers"
        : >"$raw_tmp"
        : >"$header_tmp"

        if ! http_status=$(curl --silent --show-error \
                --output "$raw_tmp" \
                --dump-header "$header_tmp" \
                --write-out '%{http_code}' \
                --get 'https://api.github.com/search/code' \
                --header "@$AUTH_HEADER" \
                --header 'Accept: application/vnd.github+json' \
                --header "X-GitHub-Api-Version: $GITHUB_API_VERSION" \
                --data-urlencode "q=$query" \
                --data-urlencode "per_page=$PER_PAGE" \
                --data-urlencode 'page=1'); then
            http_status=${http_status:-000}
        fi
        completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

        if [[ "$http_status" == "200" ]] \
            && jq -e '.total_count | type == "number"' "$raw_tmp" >/dev/null \
            && jq -e '.incomplete_results == false' "$raw_tmp" >/dev/null \
            && jq -e '.items | type == "array"' "$raw_tmp" >/dev/null; then
            sanitized_header_tmp="$TMP_DIR/$stem.sanitized.headers"
            sanitize_response_headers "$header_tmp" "$sanitized_header_tmp"
            mv "$raw_tmp" "$raw_file"
            mv "$sanitized_header_tmp" "$header_file"
            rm -f "$header_tmp"
            meta_tmp="$TMP_DIR/$stem.meta.json"
            write_metadata "$index" "$class_name" "$query_version" "$query_kind" "$query" \
                "$started_at" "$completed_at" "$http_status" "$raw_file" "$header_file" "$meta_tmp"
            mv "$meta_tmp" "$meta_file"
            count=$(jq -r '.total_count' "$meta_file")
            remaining=$(jq -r '.rate_limit.remaining // "?"' "$meta_file")
            processed_this_run=$((processed_this_run + 1))
            completed_total=$((completed_total + 1))
            log "[$index/$total_queries] $class_name -> $count import-text matches (rate remaining: $remaining)."
            break
        fi

        error_message=$(jq -r '.message // "non-JSON or unknown API error"' "$raw_tmp" 2>/dev/null || echo "non-JSON or unknown API error")
        log "[$index/$total_queries] attempt $attempt failed with HTTP $http_status: $error_message"
        mv "$raw_tmp" "$TMP_DIR/$stem.failed-$attempt.json"
        failed_header="$TMP_DIR/$stem.failed-$attempt.headers"
        sanitize_response_headers "$header_tmp" "$failed_header"
        rm -f "$header_tmp"
        failed_attempt=$attempt
        attempt=$((attempt + 1))
        if (( attempt <= MAX_RETRIES )); then
            retry_after=$(header_value "$TMP_DIR/$stem.failed-$((attempt - 1)).headers" "retry-after")
            if [[ "$retry_after" =~ ^[0-9]+$ ]] && (( retry_after > 0 )); then
                log "GitHub requested a ${retry_after}s retry delay."
                sleep "$retry_after"
            elif [[ "$http_status" == "429" ]] \
                || { [[ "$http_status" == "403" ]] && [[ "$error_message" == *"rate limit"* ]]; }; then
                secondary_wait=$((failed_attempt * 60))
                log "GitHub secondary rate limit detected; waiting ${secondary_wait}s."
                sleep "$secondary_wait"
            else
                sleep $((attempt * 2))
            fi
        fi
    done

    if (( attempt > MAX_RETRIES )); then
        echo "Query failed after $MAX_RETRIES attempts: $class_name" >&2
        build_snapshot_aggregate
        exit 1
    fi
done < <(sed -n '2,$p' "$QUERY_MANIFEST" | tr '\t' '\034')

build_snapshot_aggregate
completed_total=$(jq -r '.completed_queries' "$OUTPUT_DIR/snapshot-manifest.json")
pending=$(jq -r '.pending_queries' "$OUTPUT_DIR/snapshot-manifest.json")
log "Snapshot checkpoint written: $completed_total completed, $pending pending."
if (( prepared_this_run > 0 )); then
    log "Preserved or reused $prepared_this_run validated query records."
fi
