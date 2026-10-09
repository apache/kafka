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

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
OUTPUT_DIR="$SCRIPT_DIR/output"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/kip-1320-repro.XXXXXX")

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

FILES=(
  inventory.tsv
  bytecode-summary.md
  query-manifest.tsv
  evidence-packets.md
  input-manifest.tsv
  crawl-inputs.env
)

run_and_copy() {
  local pass=$1
  "$SCRIPT_DIR/run.sh"

  mkdir -p "$TMP_DIR/$pass"
  for file in "${FILES[@]}"; do
    cp "$OUTPUT_DIR/$file" "$TMP_DIR/$pass/$file"
  done
}

run_and_copy first
run_and_copy second

for file in "${FILES[@]}"; do
  cmp "$TMP_DIR/first/$file" "$TMP_DIR/second/$file"
done

echo "Reproducibility check passed: both runs produced byte-identical outputs."
shasum -a 256 "${FILES[@]/#/$OUTPUT_DIR/}"
