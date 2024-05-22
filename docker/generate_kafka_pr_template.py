#!/usr/bin/env python

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

"""
Python script to prepare the PR template for the docker official image
This script is used to prepare the PR template for the docker official image

Usage:
    Example command:-
        generate_kafka_pr_template.py --help
        Get detailed description of each option

        generate_kafka_pr_template.py --image-type <image_type>

        This command will build a PR template for <image_type> as image type (jvm by default) based docker official image,
        on the directories present under docker/docker_official_images.
        This PR template will be used to raise a PR in the Docker Official Images Repo.
"""

import os
import subprocess
import sys
import argparse
from pathlib import Path


# Returns the current branch from which the script is being run
def get_current_branch():
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode('utf-8')


# Returns the hash of the most recent commit that modified any of the specified files.
def file_commit(*files):
    return subprocess.check_output(["git", "log", "-1", "--format=format:%H", "HEAD", "--"] + list(files)).strip().decode('utf-8')


# Returns the latest commit hash for all files in a given directory.
def dir_commit(directory):
    docker_required_scripts = [str(path) for path in Path(directory).rglob('*') if path.is_file()]
    files_to_check = [os.path.join(directory, "Dockerfile")] + docker_required_scripts
    return file_commit(*files_to_check)


# Split the version string into parts and convert them to integers for version comparision
def get_version_parts(version):
    return tuple(int(part) for part in version.name.split('.'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-type", "-type", choices=[
                        "jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    args = parser.parse_args()
    if get_current_branch() != "trunk":
        print("This script can only be run from the trunk branch.")
        sys.exit(1)
    self = os.path.basename(__file__)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    docker_official_images_dir = Path(os.path.join(current_dir, "docker_official_images"))
    highest_version = ""

    header = f"""
# This file is generated via https://github.com/apache/kafka/blob/{file_commit(os.path.join(current_dir, self))}/docker/generate_kafka_pr_template.py
Maintainers: The Apache Kafka Project <dev@kafka.apache.org> (@ApacheKafka)
GitRepo: https://github.com/apache/kafka.git
"""
    print(header)
    versions = sorted((d for d in docker_official_images_dir.iterdir() if d.is_dir()), key=get_version_parts, reverse=True)
    highest_version = max(versions).name if versions else ""

    for dir in versions:
        version = dir.name
        tags = version + (", latest" if version == highest_version else "")
        commit = dir_commit(dir.joinpath(args.image_type))

        info = f"""
Tags: {tags}
Architectures: amd64,arm64v8
GitCommit: {commit}
Directory: ./docker/docker_official_images/{version}/{args.image_type}
"""
        print(info.strip(), '\n')


if __name__ == "__main__":
    main()
