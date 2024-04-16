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
    docker_build_test.py --help
        Get detailed description of each option

    Example command:-
        docker_build_test.py <image_name> --image-tag <image_tag> --image-type <image_type> --kafka-url <kafka_url>

        This command will build an image with <image_name> as image name, <image_tag> as image_tag (it will be latest by default),
        <image_type> as image type (jvm by default), <kafka_url> for the kafka inside the image and run tests on the image.
        -b can be passed as additional argument if you just want to build the image.
        -t can be passed if you just want to run tests on the image.
"""

from datetime import date
import argparse
from distutils.dir_util import copy_tree
import shutil
from test.docker_sanity_test import run_tests
from common import execute, jvm_image
import tempfile
import os


def set_executable_permissions(directory):
    """
    Sets executable permissions for all files in the specified directory and its subdirectories.
    """
    for root, _, files in os.walk(directory):
        for file in files:
            path = os.path.join(root, file)
            os.chmod(path, os.stat(path).st_mode | 0o111)


def build_jvm(image, tag, kafka_version):
    image = f'{image}:{tag}'
    current_dir = os.path.dirname(os.path.realpath(__file__))
    temp_dir_path = tempfile.mkdtemp()
    directories = [
        f'{current_dir}/docker_official_images/{kafka_version}/jvm',
        f'{current_dir}/docker_official_images/{kafka_version}/jvm/resources'
    ]
    for directory in directories:
        set_executable_permissions(directory)
    copy_tree(f"{current_dir}/docker_official_images/{kafka_version}/jvm",
              f"{temp_dir_path}/jvm")
    copy_tree(f"{current_dir}/docker_official_images/{kafka_version}/jvm/resources",
              f"{temp_dir_path}/jvm/resources")
    command = f"docker build -f $DOCKER_FILE -t {image} $DOCKER_DIR"
    command = command.replace("$DOCKER_FILE", f"{temp_dir_path}/jvm/Dockerfile")
    command = command.replace("$DOCKER_DIR", f"{temp_dir_path}/jvm")
    try:
        execute(command.split())
    except:
        raise SystemError("Docker Image Build failed")
    finally:
        shutil.rmtree(temp_dir_path)


def run_jvm_tests(image, tag, kafka_url):
    temp_dir_path = tempfile.mkdtemp()
    try:
        current_dir = os.path.dirname(os.path.realpath(__file__))
        copy_tree(f"{current_dir}/test/fixtures", f"{temp_dir_path}/fixtures")
        execute(["wget", "-nv", "-O", f"{temp_dir_path}/kafka.tgz", kafka_url])
        execute(["mkdir", f"{temp_dir_path}/fixtures/kafka"])
        execute(["tar", "xfz", f"{temp_dir_path}/kafka.tgz", "-C",
                f"{temp_dir_path}/fixtures/kafka", "--strip-components", "1"])
        failure_count = run_tests(f"{image}:{tag}", "jvm", temp_dir_path)
    except:
        raise SystemError("Failed to run the tests")
    finally:
        shutil.rmtree(temp_dir_path)
    test_report_location_text = f"Test report written to {current_dir}/test/report_jvm.html"
    if failure_count != 0:
        raise SystemError(
            f"{failure_count} tests have failed. {test_report_location_text}")
    else:
        print(f"All tests passed. {test_report_location_text}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "image", help="Image name that you want to keep for the Docker image")
    parser.add_argument("--image-tag", "-tag", default="latest",
                        dest="tag", help="Image tag that you want to add to the image")
    parser.add_argument("--image-type", "-type", choices=[
                        "jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--kafka-url", "-u", dest="kafka_url",
                        help="Kafka url to be used to download kafka binary tarball in the docker image")
    parser.add_argument("--kafka-version", "-v", dest="kafka_version",
                        help="Kafka version for which the source for docker official image is to be built")
    parser.add_argument("--build", "-b", action="store_true", dest="build_only",
                        default=False, help="Only build the image, don't run tests")
    parser.add_argument("--test", "-t", action="store_true", dest="test_only",
                        default=False, help="Only run the tests, don't build the image")
    args = parser.parse_args()

    if args.image_type == "jvm" and (args.build_only or not (args.build_only or args.test_only)):
        if args.kafka_url and args.kafka_version:
            build_jvm(args.image, args.tag, args.kafka_version)
        else:
            raise ValueError(
                "--kafka-url and --kafka-version are required argument for jvm docker official image image")

    if args.image_type == "jvm" and (args.test_only or not (args.build_only or args.test_only)):
        run_jvm_tests(args.image, args.tag, args.kafka_url)
