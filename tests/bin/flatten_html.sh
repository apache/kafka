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

usage() {
    cat <<EOF
flatten_html.sh: This script "flattens" an HTML file by inlining all
files included via "#include virtual".  This is useful when making
changes to the Kafka documentation files.

Typical usage:
    ./gradlew docsJar
    ./tests/bin/flatten_html.sh -f ./docs/protocol.html > /tmp/my-protocol.html
    firefox /tmp/my-protocol.html &

usage:
$0 [flags]

flags:
-f [filename]   The HTML file to process.
-h              Print this help message.
EOF
}

die() {
    echo $@
    exit 1
}

realpath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

process_file() {
    local CUR_FILE="${1}"
    [[ -f "${CUR_FILE}" ]] || die "Unable to open input file ${CUR_FILE}"
    while IFS= read -r LINE; do
        if [[ $LINE =~ \#include\ virtual=\"(.*)\" ]]; then
            local INCLUDED_FILE="${BASH_REMATCH[1]}"
            if [[ $INCLUDED_FILE =~ ../includes/ ]]; then
                : # ignore ../includes
            else
                pushd "$(dirname "${CUR_FILE}")" &> /dev/null \
                    || die "failed to change directory to directory of ${CUR_FILE}"
                process_file "${INCLUDED_FILE}"
                popd &> /dev/null
            fi
        else
            echo "${LINE}"
        fi
    done < "${CUR_FILE}"
}

FILE=""
while getopts "f:h" arg; do
    case $arg in
        f) FILE=$OPTARG;;
        h) usage; exit 0;;
        *) echo "Error parsing command-line arguments."
            usage
            exit 1;;
    esac
done

[[ -z "${FILE}" ]] && die "You must specify which file to process.  -h for help."
process_file "${FILE}"
