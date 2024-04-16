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

set -eu

self="$(basename "$BASH_SOURCE")"

cd "$(dirname "$(readlink -f "$BASH_SOURCE")")/docker_official_images"

source ../common.sh

highest_version=""

# Output header information
cat <<-EOH
# This file is generated via https://github.com/apache/kafka/blob/$(fileCommit "../$self")/docker/generate_kafka_pr_template.sh

Maintainers: The Apache Kafka Project <dev@kafka.apache.org> (@ApacheKafka)
GitRepo: https://github.com/apache/kafka.git
EOH

highest_version=$(find . -mindepth 1 -maxdepth 1 -type d | sort -Vr | xargs basename | head -n 1)
versions=$(find . -mindepth 1 -maxdepth 1 -type d | sort -V)

for dir in $versions; do
    version=$(basename "$dir")
    major_minor_version=$(echo "$version" | cut -d'.' -f1-2)
    highest_major_minor=$(echo "$highest_version" | cut -d'.' -f1-2)

    latest_in_series=$(find . -mindepth 1 -maxdepth 1 -type d -name "$major_minor_version.*" ! -name "*-rc" | sort -V | tail -n 1)
    latest_in_series_basename=$(basename "$latest_in_series")

    tags="$version"
    if [[ "$version" == "$latest_in_series_basename" && "$major_minor_version" == "$highest_major_minor" ]]; then
        tags+=", latest"
    fi

    commit="$(dirCommit "$dir/jvm")"

    echo
    cat <<-EOE
Tags: $tags
Architectures: amd64,arm64v8
GitCommit: $commit
Directory: ./docker/docker_official_images/$version/jvm
EOE
done

