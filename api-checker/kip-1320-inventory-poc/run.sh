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

POC_ROOT=$(cd "$(dirname "$0")" && pwd)
KAFKA_ROOT=${KAFKA_ROOT:-"$(git -C "$POC_ROOT" rev-parse --show-toplevel)"}
OUTPUT_DIR=${OUTPUT_DIR:-"$POC_ROOT/output"}

# shellcheck source=inputs.env
source "$POC_ROOT/inputs.env"

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "Required file does not exist: $1" >&2
    exit 1
  fi
}

CURRENT_KAFKA_SHA=$(git -C "$KAFKA_ROOT" rev-parse HEAD)
if [[ -n "${EXPECTED_KAFKA_SHA:-}" && "$CURRENT_KAFKA_SHA" != "$EXPECTED_KAFKA_SHA" ]]; then
  echo "Kafka SHA mismatch: expected $EXPECTED_KAFKA_SHA, got $CURRENT_KAFKA_SHA" >&2
  exit 1
fi
if [[ "${ALLOW_DIRTY_WORKTREE:-0}" != "1" ]] \
    && [[ -n "$(git -C "$KAFKA_ROOT" status --porcelain --untracked-files=no)" ]]; then
  echo "Kafka worktree has tracked changes." >&2
  echo "Commit them first, or use ALLOW_DIRTY_WORKTREE=1 for scanner development." >&2
  exit 1
fi

cd "$KAFKA_ROOT"

./gradlew \
  :api-checker:core:jar \
  :clients:jar :clients:javadocJar \
  :streams:jar :streams:javadocJar \
  :streams:test-utils:jar :streams:test-utils:javadocJar \
  :connect:api:jar :connect:api:javadocJar \
  :connect:transforms:jar :connect:transforms:javadocJar \
  :connect:json:jar :connect:json:javadocJar \
  :connect:file:jar :connect:file:javadocJar \
  :connect:mirror-client:jar :connect:mirror-client:javadocJar \
  :connect:mirror:jar :connect:mirror:javadocJar \
  :connect:basic-auth-extension:jar :connect:basic-auth-extension:javadocJar

MODULES=(
  "$KAFKA_ROOT/clients|kafka-clients"
  "$KAFKA_ROOT/streams|kafka-streams"
  "$KAFKA_ROOT/streams/test-utils|kafka-streams-test-utils"
  "$KAFKA_ROOT/connect/api|connect-api"
  "$KAFKA_ROOT/connect/transforms|connect-transforms"
  "$KAFKA_ROOT/connect/json|connect-json"
  "$KAFKA_ROOT/connect/file|connect-file"
  "$KAFKA_ROOT/connect/mirror-client|connect-mirror-client"
  "$KAFKA_ROOT/connect/mirror|connect-mirror"
  "$KAFKA_ROOT/connect/basic-auth-extension|connect-basic-auth-extension"
)

KAFKA_VERSION=$(awk -F= '$1 == "version" { print $2; exit }' "$KAFKA_ROOT/gradle.properties")
if [[ -z "$KAFKA_VERSION" ]]; then
  echo "Unable to read Kafka version from gradle.properties" >&2
  exit 1
fi

CORE_JAR="$KAFKA_ROOT/api-checker/core/build/libs/core-$KAFKA_VERSION.jar"
require_file "$CORE_JAR"

ASM_JAR=$(find "$HOME/.gradle/caches/modules-2/files-2.1/org.ow2.asm/asm/$PINNED_ASM_VERSION" \
  -type f -name "asm-$PINNED_ASM_VERSION.jar" | sort | head -1)
require_file "$ASM_JAR"
ASM_SHA256=$(sha256_file "$ASM_JAR")
if [[ "$ASM_SHA256" != "$PINNED_ASM_SHA256" ]]; then
  echo "ASM checksum mismatch: expected $PINNED_ASM_SHA256, got $ASM_SHA256" >&2
  exit 1
fi

mkdir -p "$POC_ROOT/.build/classes" "$OUTPUT_DIR"
javac -cp "$CORE_JAR:$ASM_JAR" \
  -d "$POC_ROOT/.build/classes" \
  "$POC_ROOT/src/org/apache/kafka/apicheck/Kip1320ApiInventory.java"

MAIN_JARS=()
JAVADOC_JARS=()
for spec in "${MODULES[@]}"; do
  module=${spec%%|*}
  base=${spec#*|}
  main_jar="$module/build/libs/$base-$KAFKA_VERSION.jar"
  javadoc_jar="$module/build/libs/$base-$KAFKA_VERSION-javadoc.jar"
  require_file "$main_jar"
  require_file "$javadoc_jar"
  MAIN_JARS+=("$main_jar")
  JAVADOC_JARS+=("$javadoc_jar")
done

SCANNER_SHA256=$(sha256_file \
  "$POC_ROOT/src/org/apache/kafka/apicheck/Kip1320ApiInventory.java")
INPUT_MANIFEST="$OUTPUT_DIR/input-manifest.tsv"
{
  printf 'kind\tname\tsha256\n'
  printf 'metadata\tkafka_sha\t%s\n' "$CURRENT_KAFKA_SHA"
  printf 'metadata\tkafka_version\t%s\n' "$KAFKA_VERSION"
  printf 'metadata\tquery_set_version\t%s\n' "$QUERY_SET_VERSION"
  printf 'metadata\tjava_query_version\t%s\n' "$JAVA_QUERY_VERSION"
  printf 'metadata\tjava_query_template\t%s\n' "$JAVA_QUERY_TEMPLATE"
  printf 'metadata\tscala_query_version\t%s\n' "$SCALA_QUERY_VERSION"
  printf 'metadata\tscala_query_template\t%s\n' "$SCALA_QUERY_TEMPLATE"
  printf 'tool\tKip1320ApiInventory.java\t%s\n' "$SCANNER_SHA256"
  printf 'dependency\tasm-%s.jar\t%s\n' "$PINNED_ASM_VERSION" "$ASM_SHA256"
  printf 'artifact\tapi-checker/core/%s\t%s\n' \
    "$(basename "$CORE_JAR")" "$(sha256_file "$CORE_JAR")"
  for jar in "${MAIN_JARS[@]}" "${JAVADOC_JARS[@]}"; do
    printf 'artifact\t%s\t%s\n' "$(basename "$jar")" "$(sha256_file "$jar")"
  done
} >"$INPUT_MANIFEST"
INPUT_MANIFEST_SHA256=$(sha256_file "$INPUT_MANIFEST")

ARGS=(
  --repo-root "$KAFKA_ROOT"
  --scanner-sha256 "$SCANNER_SHA256"
  --input-manifest-sha256 "$INPUT_MANIFEST_SHA256"
  --java-query-version "$JAVA_QUERY_VERSION"
  --java-query-template "$JAVA_QUERY_TEMPLATE"
  --scala-query-version "$SCALA_QUERY_VERSION"
  --scala-query-template "$SCALA_QUERY_TEMPLATE"
  --out-tsv "$OUTPUT_DIR/inventory.tsv"
  --out-md "$OUTPUT_DIR/bytecode-summary.md"
  --out-query-manifest "$OUTPUT_DIR/query-manifest.tsv"
  --out-evidence-md "$OUTPUT_DIR/evidence-packets.md"
  --snapshot-sha "$CURRENT_KAFKA_SHA"
)

for jar in "${MAIN_JARS[@]}"; do
  ARGS+=(--jar "$jar")
done
for jar in "${JAVADOC_JARS[@]}"; do
  ARGS+=(--javadoc-jar "$jar")
done

java -cp "$POC_ROOT/.build/classes:$CORE_JAR:$ASM_JAR" \
  org.apache.kafka.apicheck.Kip1320ApiInventory "${ARGS[@]}"

assert_flag() {
  local fqcn=$1
  local flag=$2
  awk -F '\t' -v fqcn="$fqcn" -v flag="$flag" '
    NR == 1 {
      for (i = 1; i <= NF; i++) {
        if ($i == "class") class_col = i
        if ($i == "flags") flags_col = i
      }
      next
    }
    $class_col == fqcn && index($flags_col, flag) > 0 { found = 1 }
    END { exit(found ? 0 : 1) }
  ' "$OUTPUT_DIR/inventory.tsv" || {
    echo "Missing expected flag $flag for $fqcn" >&2
    exit 1
  }
}

assert_flag org.apache.kafka.common.config.ConfigTransformer PUBLIC_ANNOTATED
assert_flag org.apache.kafka.clients.admin.ConsumerGroupListing PUBLIC_SIGNATURE_LEAK
assert_flag org.apache.kafka.connect.json.JsonConverter PUBLIC_SUPERTYPE
assert_flag org.apache.kafka.connect.json.JsonConverter CONFIG_REFERENCED
assert_flag org.apache.kafka.common.utils.Utils UNREACHABLE_INTERNAL
assert_flag org.apache.kafka.clients.consumer.OffsetResetStrategy ALREADY_DEPRECATED_OR_MOVED

awk -F '\t' '
  NR == 1 {
    for (i = 1; i <= NF; i++) {
      if ($i == "class") class_col = i
      if ($i == "query_version") query_col = i
      if ($i == "query_kind") kind_col = i
    }
    next
  }
  $class_col == "org.apache.kafka.common.protocol.ApiKeys" \
      && $query_col == "JAVA_EXACT_IMPORT_V1" \
      && $kind_col == "java_exact_import" { java_found = 1 }
  $class_col == "org.apache.kafka.common.protocol.ApiKeys" \
      && $query_col == "SCALA_EXACT_IMPORT_V1" \
      && $kind_col == "scala_exact_import" { scala_found = 1 }
  END { exit(java_found && scala_found ? 0 : 1) }
' "$OUTPUT_DIR/query-manifest.tsv" || {
  echo "Missing reproducible ApiKeys query manifest entry" >&2
  exit 1
}

cat >"$OUTPUT_DIR/crawl-inputs.env" <<EOF
KAFKA_SHA=$CURRENT_KAFKA_SHA
QUERY_MANIFEST_SHA256=$(sha256_file "$OUTPUT_DIR/query-manifest.tsv")
INVENTORY_SHA256=$(sha256_file "$OUTPUT_DIR/inventory.tsv")
QUERY_SET_VERSION=$QUERY_SET_VERSION
JAVA_QUERY_VERSION=$JAVA_QUERY_VERSION
SCALA_QUERY_VERSION=$SCALA_QUERY_VERSION
SNAPSHOT_SCHEMA=$SNAPSHOT_SCHEMA
GITHUB_API_VERSION=$GITHUB_API_VERSION
PER_PAGE=$PER_PAGE
EOF

echo "Wrote $OUTPUT_DIR/inventory.tsv"
echo "Wrote $OUTPUT_DIR/bytecode-summary.md"
echo "Wrote $OUTPUT_DIR/query-manifest.tsv"
echo "Wrote $OUTPUT_DIR/evidence-packets.md"
echo "Wrote $OUTPUT_DIR/input-manifest.tsv"
echo "Wrote $OUTPUT_DIR/crawl-inputs.env"
echo "Representative bytecode and Java/Scala query-manifest assertions passed"
