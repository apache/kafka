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
Python script to build and test a docker image
This script is used to generate a test report

Usage:
    docker_official_image_build_test.py --help
        Get detailed description of each option

    Example command:-
        docker_official_image_build_test.py <image_name> --image-tag <image_tag> --image-type <image_type> --kafka-version <kafka_version>

        This command will build an image with <image_name> as image name, <image_tag> as image_tag (it will be latest by default),
        <image_type> as image type (jvm by default), <kafka_version> for the kafka version for which the image is being built, and
        run tests on the image.
        -b can be passed as additional argument if you just want to build the image.
        -t can be passed if you just want to run tests on the image.
"""

import argparse
import shutil
from common import execute
from docker_build_test import run_docker_tests
import tempfile
import os


def build_docker_official_image(image, tag, kafka_version, image_type):
    image = f'{image}:{tag}'
    current_dir = os.path.dirname(os.path.realpath(__file__))
    temp_dir_path = tempfile.mkdtemp()
    shutil.copytree(f"{current_dir}/docker_official_images/{kafka_version}/{image_type}",
              f"{temp_dir_path}/{image_type}", dirs_exist_ok=True)
    shutil.copytree(f"{current_dir}/docker_official_images/{kafka_version}/jvm/resources",
              f"{temp_dir_path}/{image_type}/resources", dirs_exist_ok=True)
    shutil.copy(f"{current_dir}/server.properties", f"{temp_dir_path}/{image_type}")
    command = f"docker build -f $DOCKER_FILE -t {image} $DOCKER_DIR"
    command = command.replace("$DOCKER_FILE", f"{temp_dir_path}/{image_type}/Dockerfile")
    command = command.replace("$DOCKER_DIR", f"{temp_dir_path}/{image_type}")
    try:
        execute(command.split())
    except:
        raise SystemError("Docker Image Build failed")
    finally:
        shutil.rmtree(temp_dir_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "image", help="Image name that you want to keep for the Docker image")
    parser.add_argument("--image-tag", "-tag", default="latest",
                        dest="tag", help="Image tag that you want to add to the image")
    parser.add_argument("--image-type", "-type", choices=[
                        "jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--kafka-version", "-v", dest="kafka_version",
                        help="Kafka version for which the source for docker official image is to be built")
    parser.add_argument("--build", "-b", action="store_true", dest="build_only",
                        default=False, help="Only build the image, don't run tests")
    parser.add_argument("--test", "-t", action="store_true", dest="test_only",
                        default=False, help="Only run the tests, don't build the image")
    args = parser.parse_args()
    kafka_url = f"https://archive.apache.org/dist/kafka/{args.kafka_version}/kafka_2.13-{args.kafka_version}.tgz"
    if args.build_only or not (args.build_only or args.test_only):
        if args.kafka_version:
            build_docker_official_image(args.image, args.tag, args.kafka_version, args.image_type)
        else:
            raise ValueError(
                "--kafka-version is required argument for jvm docker official image image")
    if args.test_only or not (args.build_only or args.test_only):
        run_docker_tests(args.image, args.tag, kafka_url, args.image_type)
