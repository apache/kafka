#!/bin/bash
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

pr_diff=$(gh pr diff $PR_NUM -R $GITHUB_REPOSITORY)

min_size=100
insertions=$(printf "$pr_diff" | grep '^+' | wc -l)
deletions=$(printf "$pr_diff" | grep '^-' | wc -l)

total_changes=$((insertions + deletions))
if [ "$total_changes" -lt "$min_size" ]; then
    gh api -H "Accept: application/vnd.github+json"  \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    /repos/$GITHUB_REPOSITORY/issues/$PR_NUM/labels -f "labels[]=minor"
else
  gh api -X Delete -H "Accept: application/vnd.github+json"  \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      /repos/$GITHUB_REPOSITORY/issues/$PR_NUM/labels -f "labels[]=minor"
fi
