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
Python script to prepare the hardcoded source folder for the docker official image
This script is used to prepare the source folder for the docker official image

Usage:
    prepare_docker_official_image_source.py --help
        Get detailed description of each option

    Example command:-
        prepare_docker_official_image_source.py --image-type <image_type> --kafka-version <kafka_version>

        This command will build a directory with the name as <kafka_version> housing the hardcoded static Dockerfile and scripts for 
        the docker official image, <image_type> as image type (jvm by default), <kafka_version> for the kafka version for which the 
        image is being built.
"""

from datetime import date
import argparse
import os
import shutil
import re


def remove_args_and_hardcode_values(file_path, kafka_version, kafka_url):
    with open(file_path, 'r') as file:
        filedata = file.read()
    filedata = filedata.replace("ARG kafka_url", f"ENV kafka_url {kafka_url}")
    filedata = filedata.replace(
        "ARG build_date", f"ENV build_date {str(date.today())}")
    original_comment = re.compile(r"# Get kafka from https://archive.apache.org/dist/kafka and pass the url through build arguments")
    updated_comment = f"# Get Kafka from https://archive.apache.org/dist/kafka, url passed as env var, for version {kafka_version}"
    filedata = original_comment.sub(updated_comment, filedata)
    with open(file_path, 'w') as file:
        file.write(filedata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-type", "-type", choices=[
                        "jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--kafka-version", "-v", dest="kafka_version",
                        help="Kafka version for which the source for docker official image is to be built")
    args = parser.parse_args()
    kafka_url = f"https://archive.apache.org/dist/kafka/{args.kafka_version}/kafka_2.13-{args.kafka_version}.tgz"
    current_dir = os.path.dirname(os.path.realpath(__file__))
    new_dir = os.path.join(current_dir, 'docker_official_images', args.kafka_version)
    if os.path.exists(new_dir):
        shutil.rmtree(new_dir)
    os.makedirs(new_dir)
    shutil.copytree(os.path.join(current_dir, args.image_type), os.path.join(new_dir, args.image_type), dirs_exist_ok=True)
    shutil.copytree(os.path.join(current_dir, 'resources'), os.path.join(new_dir, args.image_type, 'resources'), dirs_exist_ok=True)
    remove_args_and_hardcode_values(os.path.join(new_dir, args.image_type, 'Dockerfile'), args.kafka_version, kafka_url)
