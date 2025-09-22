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
        docker_build_test.py <image_name> --image-tag <image_tag> --image-type <image_type> --kafka-archive <kafka_archive>

        This command will build an image with <image_name> as image name, <image_tag> as image_tag (it will be latest by default),
        <image_type> as image type (jvm by default), <kafka_url> for the kafka inside the image and run tests on the image.
        <kafka_archive> can be passed as an alternative to <kafka_url> to use a local kafka archive. The path of kafka_archive should be absolute.
        -b can be passed as additional argument if you just want to build the image.
        -t can be passed if you just want to run tests on the image.
"""

from datetime import date
import argparse
import shutil
from test.docker_sanity_test import run_tests
from common import execute, build_docker_image_runner
import tempfile
import os

def run_docker_tests(image, tag, kafka_url, image_type):
    temp_dir_path = tempfile.mkdtemp()
    try:
        current_dir = os.path.dirname(os.path.realpath(__file__))
        shutil.copytree(f"{current_dir}/test/fixtures", f"{temp_dir_path}/fixtures", dirs_exist_ok=True)
        execute(["wget", "-nv", "-O", f"{temp_dir_path}/kafka.tgz", kafka_url])
        execute(["mkdir", f"{temp_dir_path}/fixtures/kafka"])
        execute(["tar", "xfz", f"{temp_dir_path}/kafka.tgz", "-C", f"{temp_dir_path}/fixtures/kafka", "--strip-components", "1"])
        failure_count = run_tests(f"{image}:{tag}", image_type, temp_dir_path)
    except:
        raise SystemError("Failed to run the tests")
    finally:
        shutil.rmtree(temp_dir_path)
    test_report_location_text = f"To view test report please check {current_dir}/test/report_{image_type}.html"
    if failure_count != 0:
        raise SystemError(f"{failure_count} tests have failed. {test_report_location_text}")
    else:
        print(f"All tests passed successfully. {test_report_location_text}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("image", help="Image name that you want to keep for the Docker image")
    parser.add_argument("--image-tag", "-tag", default="latest", dest="tag", help="Image tag that you want to add to the image")
    parser.add_argument("--image-type", "-type", choices=["jvm", "native"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--build", "-b", action="store_true", dest="build_only", default=False, help="Only build the image, don't run tests")
    parser.add_argument("--test", "-t", action="store_true", dest="test_only", default=False, help="Only run the tests, don't build the image")

    archive_group = parser.add_mutually_exclusive_group(required=True)
    archive_group.add_argument("--kafka-url", "-u", dest="kafka_url", help="Kafka url to be used to download kafka binary tarball in the docker image")
    archive_group.add_argument("--kafka-archive", "-a", dest="kafka_archive", help="Kafka archive to be used to extract kafka binary tarball in the docker image")

    args = parser.parse_args()

    if args.build_only or not (args.build_only or args.test_only):
        if args.kafka_url:
            build_docker_image_runner(f"docker build -f $DOCKER_FILE -t {args.image}:{args.tag} --build-arg kafka_url={args.kafka_url} --build-arg build_date={date.today()} --no-cache --progress=plain $DOCKER_DIR", args.image_type)
        elif args.kafka_archive:
            build_docker_image_runner(f"docker build -f $DOCKER_FILE -t {args.image}:{args.tag} --build-arg build_date={date.today()} --no-cache --progress=plain $DOCKER_DIR", args.image_type, args.kafka_archive)
    
    if args.test_only or not (args.build_only or args.test_only):
        run_docker_tests(args.image, args.tag, args.kafka_url, args.image_type)
