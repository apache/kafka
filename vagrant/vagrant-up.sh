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

#! /usr/bin/env bash

set -o nounset
set -o errexit  # exit script if any command exits with nonzero value

readonly PROG_NAME=$(basename $0)
readonly PROG_DIR=$(dirname $(realpath $0))
readonly INVOKE_DIR=$(pwd)
readonly ARGS="$@"

# overrideable defaults
AWS=false
PARALLEL=true
MAX_PARALLEL=5

readonly USAGE="Usage: $PROG_NAME [-h | --help] [--aws [--no-parallel] [--max-parallel MAX]]"
readonly HELP="$(cat <<EOF
Tool to bring up a vagrant cluster on local machine or aws.

    -h | --help             Show this help message
    --aws                   Use if you are running in aws
    --no-parallel           Bring up machines not in parallel. Only applicable on aws
    --max-parallel  MAX     Maximum number of machines to bring up in parallel. Only applicable on aws, default: $MAX_PARALLEL

This wrapper script essentially wraps 2 commands:
vagrant up
vagrant hostmanager

The situation on aws is complicated by the fact that aws imposes a maximum request rate,
which effectively caps the number of machines we are able to bring up in parallel. Therefore, on aws,
this wrapper script attempts to bring up machines in small batches.

If you are seeing rate limit exceeded errors, you may need to use a reduced --max-parallel setting.

EOF
)"

function help {
    echo "$USAGE"
    echo "$HELP"
    exit 0
}

while [[ $# > 0 ]]; do
    key="$1"
    case $key in
        -h | --help)
            help
            ;;
        --aws)
            AWS=true
            ;;
        --no-parallel)
            PARALLEL=false
            ;;
        --max-parallel)
            MAX_PARALLEL="$2"
            shift
            ;;
        *)
            # unknown option
            echo "Unknown option $1"
            exit 1
            ;;
esac
shift # past argument or value
done

# Get a list of vagrant machines (in any state)
function read_vagrant_machines {
    local ignore_state="ignore"
    local reading_state="reading"
    local tmp_file="tmp-$RANDOM"

    local state="$ignore_state"
    local machines=""

    while read -r line; do
        # Lines before the first empty line are ignored
        # The first empty line triggers change from ignore state to reading state
        # When in reading state, we parse in machine names until we hit the next empty line,
        # which signals that we're done parsing
        if [[ -z "$line" ]]; then
            if [[ "$state" == "$ignore_state" ]]; then
                state="$reading_state"
            else
                # all done
                echo "$machines"
                return
            fi
            continue
        fi

        # Parse machine name while in reading state
        if [[ "$state" == "$reading_state" ]]; then
            line=$(echo "$line" | cut -d ' ' -f 1)
            if [[ -z "$machines" ]]; then
                machines="$line"
            else
                machines="${machines} ${line}"
            fi
        fi
    done < <(vagrant status)
}

# Run a vagrant command on batches of machines of size $group_size
# This is annoying but necessary on aws to avoid errors due to AWS request rate
# throttling
#
# Example
#   $ vagrant_batch_command "vagrant up" "m1 m2 m3 m4 m5" "2"
#
#   This is equivalent to running "vagrant up" on groups of machines of size 2 or less, i.e.:
#   $ vagrant up m1 m2
#   $ vagrant up m3 m4
#   $ vagrant up m5
function vagrant_batch_command {
    local vagrant_cmd="$1"
    local machines="$2"
    local group_size="$3"

    local count=1
    local m_group=""
    # Using --provision flag makes this command useable both when bringing up a cluster from scratch,
    # and when bringing up a halted cluster. Permissions on certain directores set during provisioning
    # seem to revert when machines are halted, so --provision ensures permissions are set correctly in all cases
    for machine in $machines; do
        m_group="$m_group $machine"

        if [[ $(expr $count % $group_size) == 0 ]]; then
            # We've reached a full group
            # Bring up this part of the cluster
            $vagrant_cmd $m_group
            m_group=""
        fi
        ((count++))
    done

    # Take care of any leftover partially complete group
    if [[ ! -z "$m_group" ]]; then
        $vagrant_cmd $m_group
    fi
}

# We assume vagrant-hostmanager is installed, but may or may not be disabled during vagrant up
# In this fashion, we ensure we run hostmanager after machines are up, and before provisioning.
# This sequence of commands is necessary for example for bringing up a multi-node zookeeper cluster
function bring_up_local {
    vagrant up --no-provision
    vagrant hostmanager
    vagrant provision
}

function bring_up_aws {
    local parallel="$1"
    local max_parallel="$2"
    local machines="$(read_vagrant_machines)"

    if [[ "$parallel" == "true" ]]; then
        echo "Bringing up machines in batches of size $max_parallel"
        vagrant_batch_command "vagrant up --provider=aws --no-provision" "$machines" "$max_parallel"

        # Ensure hostmanager is run after machines are up, and before provisioning
        vagrant_batch_command "vagrant hostmanager" "$machines" "$max_parallel"

        # Provision the machines. Note we provision with a call to "vagrant up --provision" instead of "vagrant provision"
        # This is because "vagrant provision" does *not* provision machines in parallel
        vagrant_batch_command "vagrant up --provider=aws --provision" "$machines" "$max_parallel"
    else
        vagrant up --no-parallel --no-provision
        vagrant hostmanager
        vagrant provision
    fi
}

function main {
    if [[ "$AWS" == "true" ]]; then
        bring_up_aws "$PARALLEL" "$MAX_PARALLEL"
    else
        bring_up_local
    fi
}

main
