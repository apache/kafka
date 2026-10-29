#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Get the latest commit SHA that contains the Gradle build cache.
sha=$(curl -s "https://api.github.com/repos/apache/kafka/actions/caches?key=gradle-home-v1&ref=refs/heads/trunk" \
  | jq -r '.actions_caches | max_by(.created_at) | .key | split("-")[4]')

if ! git show "$sha" &> /dev/null; then
  printf '\e[33m%s\n%s\e[0m\n' \
  "Cannot update 'trunk-cached' because SHA $sha" \
  "does not exist locally. Please update your remote and try again."
else
  if git branch -f trunk-cached "$sha" &> /dev/null; then
    rel_date="$(git show --no-patch $sha --date=relative --pretty=format:'%cr')"
    printf '%s\n' "Local branch 'trunk-cached' successfully updated to $(head -c 10 <<< "$sha") (from $rel_date)."
  else
    printf '\e[31m%s\e[0m\n' "Failed to update ref for 'trunk-cached' and commit $(head -c 10 <<< "$sha")."
  fi
fi
