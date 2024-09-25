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

key=$(
  gh cache list \
    --key 'gradle-home-v1|Linux-X64|test' \
    --sort 'created_at' \
    --limit 1 \
    --json 'key' \
    --jq '.[].key'
)

cut --delimiter '-' --fields 5 <<< "$key"

if ! git config --get alias.update-cache >/dev/null; then
  this_path=$(realpath "$0")
  git config alias.update-cache "!bash $this_path"
  echo
  echo "Now, you can use 'git update-cache' as alias to execute this script."
fi
