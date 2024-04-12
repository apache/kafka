#!/usr/bin/env bash

# Check if the script is running on the trunk branch
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "trunk" ]; then
    echo "This script can only be run from the trunk branch."
    exit 1
fi

# Function to get the most recent commit which modified any of "$@"
fileCommit() {
    git log -1 --format='format:%H' HEAD -- "$@"
}

# Function to get the most recent commit which modified "$1/Dockerfile" or any file COPY'd from "$1/Dockerfile"
dirCommit() {
    local dir="$1"; shift
    (
        cd "$dir" || exit
        fileCommit \
            Dockerfile \
            $(git show HEAD:./Dockerfile | awk '
                toupper($1) == "COPY" {
                    for (i = 2; i < NF; i++) {
                        print $i
                    }
                }
            ')
    )
}

# Function to get the latest non-RC version for a given major.minor version
get_latest_version() {
    local major_version="$1"
    find ./docker_official_images/ -mindepth 1 -maxdepth 1 -type d -name "${major_version}.*" ! -name "*-rc" | sort -Vr | head -n 1 | xargs basename
}

