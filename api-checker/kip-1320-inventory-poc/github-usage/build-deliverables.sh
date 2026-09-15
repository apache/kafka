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
CRAWL_OUTPUT=${CRAWL_OUTPUT:-}
INVENTORY=${INVENTORY:-"$POC_DIR/output/inventory.tsv"}
QUERY_MANIFEST=${QUERY_MANIFEST:-"$POC_DIR/output/query-manifest.tsv"}
OUTPUT_DIR=${OUTPUT_DIR:-}
INPUTS_ENV=${INPUTS_ENV:-"$POC_DIR/output/crawl-inputs.env"}
USAGE_THRESHOLD=${USAGE_THRESHOLD:-1000}

if [[ ! -f "$INPUTS_ENV" ]]; then
    echo "Missing generated crawl inputs: $INPUTS_ENV" >&2
    echo "Run ../run.sh first." >&2
    exit 1
fi
# shellcheck source=/dev/null
source "$INPUTS_ENV"

CRAWL_OUTPUT=${CRAWL_OUTPUT:-"$SCRIPT_DIR/snapshots/$KAFKA_SHA/evidence"}
OUTPUT_DIR=${OUTPUT_DIR:-"$SCRIPT_DIR/snapshots/$KAFKA_SHA/deliverables"}

RESULTS="$CRAWL_OUTPUT/results.tsv"
OBSERVED_RESULTS="$CRAWL_OUTPUT/observed-results.tsv"
EVIDENCE_FILES="$CRAWL_OUTPUT/evidence-files.tsv"
SNAPSHOT_MANIFEST="$CRAWL_OUTPUT/snapshot-manifest.json"

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

for file in "$INVENTORY" "$QUERY_MANIFEST" "$RESULTS" "$OBSERVED_RESULTS" "$EVIDENCE_FILES" "$SNAPSHOT_MANIFEST"; do
    if [[ ! -f "$file" ]]; then
        echo "Missing required input: $file" >&2
        exit 1
    fi
done

# Revalidate every raw/header/metadata record and deterministically rebuild the
# aggregate files before deriving the review documents. The Python implementation
# avoids thousands of shell subprocesses while preserving the same checksum audit.
python3 "$SCRIPT_DIR/build-snapshot-aggregate.py" \
    --query-manifest "$QUERY_MANIFEST" \
    --output-dir "$CRAWL_OUTPUT" \
    --schema "$SNAPSHOT_SCHEMA" \
    --kafka-sha "$KAFKA_SHA" \
    --query-manifest-sha256 "$QUERY_MANIFEST_SHA256" \
    --inventory-sha256 "$INVENTORY_SHA256" \
    --query-set-version "$QUERY_SET_VERSION" \
    --github-api-version "$GITHUB_API_VERSION" \
    --per-page "$PER_PAGE"

if ! jq -e '.complete == true and .pending_queries == 0 and .incomplete_result_queries == 0' \
    "$SNAPSHOT_MANIFEST" >/dev/null; then
    echo "GitHub usage snapshot is not complete; refusing to publish a partial shortlist." >&2
    jq '{completed_queries, pending_queries, incomplete_result_queries}' "$SNAPSHOT_MANIFEST" >&2
    exit 1
fi

snapshot_query_manifest_sha=$(jq -r '.query_manifest_sha256' "$SNAPSHOT_MANIFEST")
snapshot_inventory_sha=$(jq -r '.inventory_sha256' "$SNAPSHOT_MANIFEST")
snapshot_input_manifest_sha=$(jq -r '.input_manifest_sha256' "$SNAPSHOT_MANIFEST")
snapshot_scanner_sha=$(jq -r '.scanner_sha256' "$SNAPSHOT_MANIFEST")
snapshot_kafka_sha=$(jq -r '.kafka_sha' "$SNAPSHOT_MANIFEST")
snapshot_query_set_version=$(jq -r '.query_set_version' "$SNAPSHOT_MANIFEST")
snapshot_first_started=$(jq -r '.first_started_at // .first_completed_at' "$SNAPSHOT_MANIFEST")
snapshot_last_completed=$(jq -r '.last_completed_at' "$SNAPSHOT_MANIFEST")
snapshot_api_sunset=$(jq -r '.api_lifecycle.sunset // "not reported"' "$SNAPSHOT_MANIFEST")
snapshot_fresh_queries=$(jq -r '.evidence_origin.github_api_queries // .completed_queries' "$SNAPSHOT_MANIFEST")
snapshot_reused_queries=$(jq -r '.evidence_origin.reused_snapshot_queries // 0' "$SNAPSHOT_MANIFEST")
snapshot_manifest_sha=$(sha256 "$SNAPSHOT_MANIFEST")
results_sha=$(sha256 "$RESULTS")
observed_results_sha=$(sha256 "$OBSERVED_RESULTS")
evidence_files_sha=$(sha256 "$EVIDENCE_FILES")
expected_results_sha=$(jq -r '.output_sha256.results_tsv' "$SNAPSHOT_MANIFEST")
expected_observed_results_sha=$(jq -r '.output_sha256.observed_results_tsv' "$SNAPSHOT_MANIFEST")
expected_evidence_files_sha=$(jq -r '.output_sha256.evidence_files_tsv' "$SNAPSHOT_MANIFEST")
total_queries=$(jq -r '.total_queries' "$SNAPSHOT_MANIFEST")
mkdir -p "$OUTPUT_DIR"
verify_observed_unsorted="$OUTPUT_DIR/.verify-observed-results.unsorted.tsv"
verify_observed="$OUTPUT_DIR/.verify-observed-results.tsv"
verify_evidence="$OUTPUT_DIR/.verify-evidence-files.tsv"
WORK_DIR=
cleanup_transient_files() {
    rm -f "$verify_observed_unsorted" "$verify_observed" "$verify_evidence"
    if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup_transient_files EXIT
printf 'index\tclass\trepository\tpath\tblob_sha\thtml_url\n' >"$verify_observed_unsorted"
printf 'index\tclass\tmetadata_sha256\traw_sha256\theader_sha256\n' >"$verify_evidence"

if [[ "$(sha256 "$QUERY_MANIFEST")" != "$QUERY_MANIFEST_SHA256" ]] \
    || [[ "$snapshot_query_manifest_sha" != "$QUERY_MANIFEST_SHA256" ]]; then
    echo "Query manifest lineage mismatch." >&2
    exit 1
fi
if [[ "$(sha256 "$INVENTORY")" != "$INVENTORY_SHA256" ]]; then
    echo "Inventory checksum mismatch." >&2
    exit 1
fi
inventory_input_manifest_sha=$(awk -F '\t' '
    NR == 1 { for (i = 1; i <= NF; i++) if ($i == "input_manifest_sha256") col = i; next }
    NR == 2 { print $col }
' "$INVENTORY")
inventory_scanner_sha=$(awk -F '\t' '
    NR == 1 { for (i = 1; i <= NF; i++) if ($i == "scanner_sha256") col = i; next }
    NR == 2 { print $col }
' "$INVENTORY")
if [[ "$snapshot_inventory_sha" != "$INVENTORY_SHA256" ]] \
    || [[ "$snapshot_input_manifest_sha" != "$inventory_input_manifest_sha" ]] \
    || [[ "$snapshot_scanner_sha" != "$inventory_scanner_sha" ]]; then
    echo "Snapshot and inventory lineage mismatch." >&2
    exit 1
fi
if [[ "$snapshot_kafka_sha" != "$KAFKA_SHA" ]]; then
    echo "Kafka SHA mismatch between snapshot and pinned inputs." >&2
    exit 1
fi
if [[ "$results_sha" != "$expected_results_sha" ]] \
    || [[ "$observed_results_sha" != "$expected_observed_results_sha" ]] \
    || [[ "$evidence_files_sha" != "$expected_evidence_files_sha" ]]; then
    echo "Aggregate output checksum mismatch." >&2
    exit 1
fi

if ! awk -F '\t' -v expected_total="$total_queries" '
    NR == 1 {
        for (i = 1; i <= NF; i++) col[$i] = i
        if (!col["index"] || !col["class"] || !col["query"] || !col["query_version"] || !col["query_kind"]) exit 2
        next
    }
    {
        expected_index = NR - 1
        if (($col["index"] + 0) != expected_index) exit 3
        count++
    }
    END {
        if (count != expected_total) exit 4
    }
' "$RESULTS"; then
    echo "Results do not provide exact sequential coverage from 1 to $total_queries." >&2
    exit 1
fi

if ! awk -F '\t' '
    NR == FNR {
        if (FNR == 1) {
            for (i = 1; i <= NF; i++) qcol[$i] = i
            next
        }
        row_index = FNR - 1
        query_class[row_index] = $qcol["class"]
        query_version[row_index] = $qcol["query_version"]
        query_kind[row_index] = $qcol["query_kind"]
        query_text[row_index] = $qcol["query"]
        next
    }
    FNR == 1 {
        for (i = 1; i <= NF; i++) rcol[$i] = i
        next
    }
    {
        row_index = $rcol["index"]
        if ($rcol["class"] != query_class[row_index] || $rcol["query_version"] != query_version[row_index] || $rcol["query_kind"] != query_kind[row_index] || $rcol["query"] != query_text[row_index]) {
            exit 2
        }
    }
' "$QUERY_MANIFEST" "$RESULTS"; then
    echo "Results class/query rows do not match the pinned query manifest." >&2
    exit 1
fi

if ! awk -F '\t' -v kafka_sha="$KAFKA_SHA" '
    NR == 1 {
        for (i = 1; i <= NF; i++) col[$i] = i
        if (!col["snapshot_sha"] || !col["scanner_sha256"] || !col["input_manifest_sha256"]) {
            exit 2
        }
        next
    }
    {
        if ($col["snapshot_sha"] != kafka_sha) exit 3
        if (scanner == "") scanner = $col["scanner_sha256"]
        if (input_manifest == "") input_manifest = $col["input_manifest_sha256"]
        if ($col["scanner_sha256"] != scanner || $col["input_manifest_sha256"] != input_manifest) {
            exit 4
        }
    }
' "$INVENTORY"; then
    echo "Inventory Kafka/scanner/input-manifest lineage is inconsistent." >&2
    exit 1
fi

if [[ "${RUN_LEGACY_SHELL_EVIDENCE_AUDIT:-0}" == "1" ]]; then
log_verified=0
while IFS=$'\t' read -r index class_name query_version query_kind total_count incomplete_results observed_items observed_repositories started_at completed_at raw_sha request_id query; do
    stem=$(printf '%05d' "$index")
    raw_file="$CRAWL_OUTPUT/raw/$stem.json"
    header_file="$CRAWL_OUTPUT/headers/$stem.headers"
    meta_file="$CRAWL_OUTPUT/metadata/$stem.json"
    if [[ ! -s "$raw_file" || ! -s "$header_file" || ! -s "$meta_file" ]]; then
        echo "Missing raw/header/metadata evidence for query $index." >&2
        exit 1
    fi
    if [[ "$(sha256 "$raw_file")" != "$raw_sha" ]] \
        || [[ "$(jq -r '.raw_sha256' "$meta_file")" != "$raw_sha" ]] \
        || [[ "$(sha256 "$header_file")" != "$(jq -r '.header_sha256' "$meta_file")" ]]; then
        echo "Raw/header checksum mismatch for query $index." >&2
        exit 1
    fi
    if ! jq -e \
        --argjson index "$index" \
        --arg class "$class_name" \
        --arg query "$query" \
        --arg query_version "$query_version" \
        --arg query_kind "$query_kind" \
        --arg request_id "$request_id" \
        --argjson total_count "$total_count" \
        '.index == $index
         and .class == $class
         and .query == $query
         and .query_version == $query_version
         and .query_kind == $query_kind
         and .github_request_id == $request_id
         and .total_count == $total_count
         and .http_status == 200
         and .incomplete_results == false' "$meta_file" >/dev/null; then
        echo "Metadata mismatch for query $index." >&2
        exit 1
    fi
    jq -r --argjson index "$index" --arg class "$class_name" '
        .items[]
        | [$index, $class, .repository.full_name, .path, .sha, .html_url]
        | @tsv
    ' "$raw_file" >>"$verify_observed_unsorted"
    printf '%s\t%s\t%s\t%s\t%s\n' \
        "$index" "$class_name" "$(sha256 "$meta_file")" \
        "$(sha256 "$raw_file")" "$(sha256 "$header_file")" >>"$verify_evidence"
    log_verified=$((log_verified + 1))
done < <(sed -n '2,$p' "$RESULTS")

if (( log_verified != total_queries )); then
    echo "Verified $log_verified query records; expected $total_queries." >&2
    exit 1
fi

{
    sed -n '1p' "$verify_observed_unsorted"
    sed -n '2,$p' "$verify_observed_unsorted" | sort -t $'\t' -k1,1n -k3,3 -k4,4
} >"$verify_observed"
if ! cmp -s "$verify_observed" "$OBSERVED_RESULTS"; then
    echo "Observed-results rows do not match raw GitHub responses." >&2
    exit 1
fi
if ! cmp -s "$verify_evidence" "$EVIDENCE_FILES"; then
    echo "Evidence-files audit root does not match raw/header/metadata files." >&2
    exit 1
fi
cleanup_transient_files
else
    cleanup_transient_files
fi

WORK_DIR="$OUTPUT_DIR/.stage.$$"
mkdir -p "$WORK_DIR"
joined_tmp="$WORK_DIR/.fresh-inventory.tsv.tmp"
shortlist_tmp="$WORK_DIR/.shortlist.tsv.tmp"
selected_results_tmp="$WORK_DIR/selected-candidate-github-results.tsv"
examples_tmp="$WORK_DIR/.repository-examples.tsv.tmp"
usage_base_tmp="$WORK_DIR/.usage-by-class.base.tsv"
usage_tmp="$WORK_DIR/.usage-by-class.tsv"
repo_counts_tmp="$WORK_DIR/.repository-counts.tsv"

awk -F '\t' -v OFS='\t' '
    NR == 1 {
        for (i = 1; i <= NF; i++) col[$i] = i
        next
    }
    {
        class = $col["class"]
        kind = $col["query_kind"]
        count[class]++
        total[class] += $col["total_count"]
        items[class] += $col["observed_items"]
        if ($col["incomplete_results"] == "true") incomplete[class] = "true"
        if (started[class] == "" || $col["started_at"] < started[class]) {
            started[class] = $col["started_at"]
        }
        if (completed[class] == "" || $col["completed_at"] > completed[class]) {
            completed[class] = $col["completed_at"]
        }
        if (kind == "java_exact_import") {
            java_total[class] = $col["total_count"]
            java_raw_sha[class] = $col["raw_sha256"]
            java_request_id[class] = $col["github_request_id"]
            java_query_version[class] = $col["query_version"]
            java_query[class] = $col["query"]
        } else if (kind == "scala_exact_import") {
            scala_total[class] = $col["total_count"]
            scala_raw_sha[class] = $col["raw_sha256"]
            scala_request_id[class] = $col["github_request_id"]
            scala_query_version[class] = $col["query_version"]
            scala_query[class] = $col["query"]
        } else {
            exit 2
        }
    }
    END {
        print "class", "fresh_import_hits", "fresh_java_import_hits",
            "fresh_scala_import_hits", "fresh_incomplete_results",
            "fresh_observed_items_first_pages", "fresh_query_started_at",
            "fresh_query_completed_at", "fresh_java_raw_sha256",
            "fresh_scala_raw_sha256", "fresh_java_github_request_id",
            "fresh_scala_github_request_id", "fresh_java_query_version",
            "fresh_scala_query_version", "fresh_java_query", "fresh_scala_query"
        for (class in count) {
            if (count[class] != 2 || java_query[class] == "" || scala_query[class] == "") {
                exit 3
            }
            print class, total[class], java_total[class] + 0, scala_total[class] + 0,
                (incomplete[class] == "true" ? "true" : "false"), items[class],
                started[class], completed[class], java_raw_sha[class],
                scala_raw_sha[class], java_request_id[class],
                scala_request_id[class], java_query_version[class],
                scala_query_version[class], java_query[class], scala_query[class]
        }
    }
' "$RESULTS" |
    {
        IFS= read -r header
        printf '%s\n' "$header"
        sort -t $'\t' -k1,1
    } >"$usage_base_tmp"

awk -F '\t' -v OFS='\t' '
    NR == 1 { next }
    {
        key = $2 SUBSEP $3
        if (!seen[key]++) repos[$2]++
    }
    END {
        print "class", "fresh_observed_repositories_first_pages"
        for (class in repos) print class, repos[class]
    }
' "$OBSERVED_RESULTS" |
    {
        IFS= read -r header
        printf '%s\n' "$header"
        sort -t $'\t' -k1,1
    } >"$repo_counts_tmp"

awk -F '\t' -v OFS='\t' '
    NR == FNR {
        if (FNR > 1) repos[$1] = $2
        next
    }
    FNR == 1 {
        print $0, "fresh_observed_repositories_first_pages"
        next
    }
    { print $0, ($1 in repos ? repos[$1] : 0) }
' "$repo_counts_tmp" "$usage_base_tmp" >"$usage_tmp"

awk -F '\t' -v OFS='\t' '
    NR == FNR {
        if (FNR == 1) {
            for (i = 1; i <= NF; i++) header[i] = $i
            width = NF
            next
        }
        class = $1
        for (i = 2; i <= NF; i++) value[class, i] = $i
        seen[class] = 1
        next
    }
    FNR == 1 {
        line = $0
        for (i = 2; i <= width; i++) line = line OFS header[i]
        print line
        next
    }
    {
        class = $2
        line = $0
        for (i = 2; i <= width; i++) {
            line = line OFS (class in seen ? value[class, i] : "")
        }
        print line
    }
' "$usage_tmp" "$INVENTORY" >"$joined_tmp"

# Verify and replace the class-column assumption above using the real inventory header.
inventory_class_col=$(awk -F '\t' '
    NR == 1 { for (i = 1; i <= NF; i++) if ($i == "class") { print i; exit } }
' "$INVENTORY")
if [[ "$inventory_class_col" != "2" ]]; then
    echo "Unexpected inventory class column: $inventory_class_col" >&2
    exit 1
fi
mv "$joined_tmp" "$WORK_DIR/fresh-inventory.tsv"

awk -F '\t' -v OFS='\t' -v threshold="$USAGE_THRESHOLD" '
    NR == 1 {
        for (i = 1; i <= NF; i++) col[$i] = i
        print "combined_import_files", "java_import_files",
            "scala_import_files", "class", "artifact", "source_path",
            "source_line", "evidence_flags", "observed_repositories_first_pages",
            "java_raw_sha256", "scala_raw_sha256"
        next
    }
    $col["candidate_non_public"] == "true" &&
    $col["already_deprecated_or_moved"] == "false" &&
    $col["fresh_import_hits"] != "" &&
    ($col["fresh_import_hits"] + 0) >= threshold {
        print $col["fresh_import_hits"], $col["fresh_java_import_hits"],
            $col["fresh_scala_import_hits"], $col["class"], $col["artifact"],
            $col["source_path"], $col["source_line"], $col["flags"],
            $col["fresh_observed_repositories_first_pages"],
            $col["fresh_java_raw_sha256"], $col["fresh_scala_raw_sha256"]
    }
' "$WORK_DIR/fresh-inventory.tsv" |
    {
        IFS= read -r header
        printf '%s\n' "$header"
        sort -t $'\t' -k1,1nr -k4,4
    } >"$shortlist_tmp"

awk -F '\t' -v OFS='\t' -v shortlist_file="$shortlist_tmp" '
    BEGIN {
        while ((getline line < shortlist_file) > 0) {
            if (++shortlist_line == 1) continue
            split(line, fields, "\t")
            selected[fields[4]] = 1
        }
        close(shortlist_file)
    }
    NR == 1 {
        for (i = 1; i <= NF; i++) col[$i] = i
        print
        next
    }
    $col["class"] in selected { print }
' "$RESULTS" >"$selected_results_tmp"

awk -F '\t' -v OFS='\t' '
    NR == 1 { next }
    {
        key = $2 SUBSEP $3
        if (!seen[key]++) {
            if (examples[$2] == "") examples[$2] = $3
            else if (counts[$2] < 9) examples[$2] = examples[$2] "," $3
            counts[$2]++
        }
    }
    END {
        for (class in examples) print class, examples[class]
    }
' "$OBSERVED_RESULTS" | sort -t $'\t' -k1,1 >"$examples_tmp"

candidate_count=$(($(wc -l <"$shortlist_tmp") - 1))
{
    echo "# KIP-1320 fresh heavy-usage candidates"
    echo
    echo "- Kafka SHA: \`$snapshot_kafka_sha\`"
    echo "- Query set: \`$snapshot_query_set_version\`"
    echo "- Query-manifest SHA-256: \`$snapshot_query_manifest_sha\`"
    echo "- Snapshot-manifest SHA-256: \`$snapshot_manifest_sha\`"
    echo "- Results SHA-256: \`$results_sha\`"
    echo "- Query window: \`$snapshot_first_started\` to \`$snapshot_last_completed\`"
    echo "- Threshold: at least $USAGE_THRESHOLD combined Java and Scala import-text files"
    echo "- Evidence origins: $snapshot_fresh_queries queried in this snapshot build; $snapshot_reused_queries reused by exact query identity"
    echo "- Active candidates at or above threshold: $candidate_count"
    echo
    echo "The count is GitHub's import-text file count, not an AST count, unique users,"
    echo "or unique repositories."
    echo "Repository counts and examples remain available in the per-class evidence packets."
    echo "The query requests fork exclusion with \`NOT is:fork\`; copied or mirrored repositories"
    echo "can still remain, so the result is not treated as a unique-project count."
    echo "The Scala V1 query is prefix-based: an outer-class import can also match imports of its"
    echo "nested types. Java and Scala counts remain separate in the TSV and evidence packets."
    echo
    echo "## Observed evidence"
    echo
    echo "| Evidence found by the scanner | Candidates |"
    echo "|---|---:|"
    awk -F '\t' '
        NR > 1 {
            flags = $8
            if (flags ~ /PUBLIC_SIGNATURE_LEAK_DIRECT/) direct++
            if (flags ~ /PUBLIC_SIGNATURE_LEAK_TRANSITIVE/ && flags !~ /PUBLIC_SIGNATURE_LEAK_DIRECT/) transitive_only++
            if (flags ~ /PUBLIC_SUPERTYPE/) public_supertype++
            if (flags ~ /CONFIG_REFERENCED/) config_referenced++
            if (flags ~ /PUBLIC_JAVADOC_GAP/) javadoc_gap++
            if (flags ~ /UNREACHABLE_INTERNAL/) no_signal++
        }
        END {
            printf "| Directly exposed by a Public signature | %d |\n", direct + 0
            printf "| Transitively reachable from a Public signature, but not directly exposed | %d |\n", transitive_only + 0
            printf "| Has a Public Kafka supertype | %d |\n", public_supertype + 0
            printf "| Referenced by Kafka configuration or service metadata | %d |\n", config_referenced + 0
            printf "| Appears in Public Javadoc without being effectively Public | %d |\n", javadoc_gap + 0
            printf "| None of the listed Public-contract signals found | %d |\n", no_signal + 0
        }
    ' "$shortlist_tmp"
    echo
    echo "These observations can overlap. They describe bytecode and repository evidence and do"
    echo "not decide whether a class is Public, internal, deprecated, or eligible for a shim."
    echo
    echo "## Candidates"
    echo
    echo "| External import files | Class | Evidence flags | Source |"
    echo "|---:|---|---|---|"
    awk -F '\t' '
        NR == 1 { next }
        {
            flags = $8
            gsub(/\|/, "\\|", flags)
            source = ($6 == "" ? "generated class in " $5 : $6 ":" $7)
            printf "| %s | `%s` | `%s` | `%s` |\n",
                $1, $4, flags, source
        }
    ' "$shortlist_tmp"
    echo
    echo "These flags are evidence for discussion, not automatic Public/Internal decisions."
} >"$WORK_DIR/fresh-heavy-candidates.md"

awk -F '\t' -v examples_file="$examples_tmp" -v shortlist_file="$shortlist_tmp" '
    BEGIN {
        while ((getline line < examples_file) > 0) {
            split(line, fields, "\t")
            examples[fields[1]] = fields[2]
        }
        close(examples_file)
        while ((getline line < shortlist_file) > 0) {
            if (++shortlist_line == 1) continue
            split(line, fields, "\t")
            selected[fields[4]] = 1
        }
        close(shortlist_file)
    }
    NR == 1 {
        for (i = 1; i <= NF; i++) col[$i] = i
        next
    }
    $col["class"] in selected {
        class = $col["class"]
        print "## `" class "`"
        print ""
        source = ($col["source_path"] == "" ? "generated class in " $col["artifact"] : $col["source_path"] ":" $col["source_line"])
        print "- Source: `" source "`"
        print "- Artifact: `" $col["artifact"] "`"
        print "- Direct annotation: `" $col["direct_annotation"] "`"
        print "- Programmatic evidence flags: `" $col["flags"] "`"
        print "- Public signature path: `" value($col["signature_leak_evidence"]) "`"
        print "- Direct Public signature path: `" value($col["direct_signature_leak_evidence"]) "`"
        print "- Public supertype: `" value($col["public_supertype_types"]) "`"
        print "- Configuration evidence: `" value($col["config_paths"]) "`"
        print "- Lifecycle evidence: `" value($col["lifecycle_evidence"]) "`"
        print "- Combined Java and Scala import-text files: " $col["fresh_import_hits"]
        print "- Java exact-import files: " $col["fresh_java_import_hits"]
        print "- Scala V1 import-prefix files: " $col["fresh_scala_import_hits"]
        print "- Unique observed repositories across the first result page of each language query: " $col["fresh_observed_repositories_first_pages"]
        print "- Repository examples: `" value(examples[class]) "`"
        print "- Java query: `" $col["fresh_java_query"] "`"
        print "- Scala query: `" $col["fresh_scala_query"] "`"
        print "- Query time: `" $col["fresh_query_started_at"] "` to `" $col["fresh_query_completed_at"] "`"
        print "- Java GitHub request ID: `" $col["fresh_java_github_request_id"] "`"
        print "- Scala GitHub request ID: `" $col["fresh_scala_github_request_id"] "`"
        print "- Java raw response SHA-256: `" $col["fresh_java_raw_sha256"] "`"
        print "- Scala raw response SHA-256: `" $col["fresh_scala_raw_sha256"] "`"
        print "- Proposed classification: `UNCLASSIFIED`"
        print "- Maintainer decision: pending"
        print ""
    }
    function value(text) {
        return text == "" ? "none observed" : text
    }
' "$WORK_DIR/fresh-inventory.tsv" >"$WORK_DIR/.packet-body.md.tmp"

{
    echo "# KIP-1320 fresh classification evidence packets"
    echo
    echo "- Kafka SHA: \`$snapshot_kafka_sha\`"
    echo "- Query set: \`$snapshot_query_set_version\`"
    echo "- Query-manifest SHA-256: \`$snapshot_query_manifest_sha\`"
    echo "- Snapshot-manifest SHA-256: \`$snapshot_manifest_sha\`"
    echo "- Observed-results SHA-256: \`$observed_results_sha\`"
    echo "- Evidence-files SHA-256: \`$evidence_files_sha\`"
    echo "- Threshold: at least $USAGE_THRESHOLD combined Java and Scala import-text files"
    echo "- Evidence origins: $snapshot_fresh_queries queried in this snapshot build; $snapshot_reused_queries reused by exact query identity"
    echo
    echo "Each packet is evidence for review. It does not automatically decide whether a class"
    echo "is Public, internal, deprecated, moved, or eligible for a compatibility shim."
    echo
    cat "$WORK_DIR/.packet-body.md.tmp"
} >"$WORK_DIR/fresh-evidence-packets.md"

mv "$shortlist_tmp" "$WORK_DIR/selected-candidates.tsv"

rm -f "$examples_tmp" "$usage_base_tmp" "$usage_tmp" \
    "$repo_counts_tmp" "$WORK_DIR/.packet-body.md.tmp"

selected_query_count=$(($(wc -l <"$selected_results_tmp") - 1))
jq -n \
    --arg schema "KIP_1320_SELECTED_RESULT_V1" \
    --arg kafka_sha "$snapshot_kafka_sha" \
    --arg query_set_version "$snapshot_query_set_version" \
    --arg query_manifest_sha256 "$snapshot_query_manifest_sha" \
    --arg snapshot_manifest_sha256 "$snapshot_manifest_sha" \
    --arg source_results_sha256 "$results_sha" \
    --arg first_started_at "$snapshot_first_started" \
    --arg last_completed_at "$snapshot_last_completed" \
    --argjson usage_threshold "$USAGE_THRESHOLD" \
    --argjson total_queries "$total_queries" \
    --argjson selected_candidates "$candidate_count" \
    --argjson selected_query_results "$selected_query_count" \
    '{
        schema: $schema,
        kafka_sha: $kafka_sha,
        query_set_version: $query_set_version,
        query_manifest_sha256: $query_manifest_sha256,
        source_snapshot_manifest_sha256: $snapshot_manifest_sha256,
        source_results_sha256: $source_results_sha256,
        first_started_at: $first_started_at,
        last_completed_at: $last_completed_at,
        usage_threshold: $usage_threshold,
        total_queries: $total_queries,
        selected_candidates: $selected_candidates,
        selected_query_results: $selected_query_results
    }' >"$WORK_DIR/selected-result-manifest.json"

{
    printf 'output\tsha256\n'
    for file in fresh-inventory.tsv fresh-heavy-candidates.md fresh-evidence-packets.md \
        selected-candidates.tsv selected-candidate-github-results.tsv \
        selected-result-manifest.json; do
        printf '%s\t%s\n' "$file" "$(sha256 "$WORK_DIR/$file")"
    done
} >"$WORK_DIR/output-checksums.tsv"

for file in fresh-inventory.tsv fresh-heavy-candidates.md fresh-evidence-packets.md \
    selected-candidates.tsv selected-candidate-github-results.tsv \
    selected-result-manifest.json output-checksums.tsv; do
    mv "$WORK_DIR/$file" "$OUTPUT_DIR/$file"
done
rmdir "$WORK_DIR"

echo "Wrote $OUTPUT_DIR/fresh-inventory.tsv"
echo "Wrote $OUTPUT_DIR/fresh-heavy-candidates.md"
echo "Wrote $OUTPUT_DIR/fresh-evidence-packets.md"
echo "Wrote $OUTPUT_DIR/selected-candidates.tsv"
echo "Wrote $OUTPUT_DIR/selected-candidate-github-results.tsv"
echo "Wrote $OUTPUT_DIR/selected-result-manifest.json"
echo "Wrote $OUTPUT_DIR/output-checksums.tsv"
