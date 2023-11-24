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

from datetime import date
import argparse
from distutils.dir_util import copy_tree
import shutil
from test.docker_sanity_test import run_tests
from common import execute
import tempfile
import os

def build_jvm(image, tag, kafka_url):
    image = f'{image}:{tag}'
    temp_dir_path = tempfile.mkdtemp()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    copy_tree(f"{current_dir}/jvm", f"{temp_dir_path}/jvm")
    copy_tree(f"{current_dir}/resources", f"{temp_dir_path}/jvm/resources")
    try:
        execute(["docker", "build", "-f", f"{temp_dir_path}/jvm/Dockerfile", "-t", image, "--build-arg", f"kafka_url={kafka_url}",
                            "--build-arg", f'build_date={date.today()}', f"{temp_dir_path}/jvm"])
    except:
        print("Docker Image Build failed")
    finally:
        shutil.rmtree(temp_dir_path)

def run_jvm_tests(image, tag, kafka_url):
    temp_dir_path = tempfile.mkdtemp()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    copy_tree(f"{current_dir}/test/fixtures", f"{temp_dir_path}/fixtures")
    execute(["wget", "-nv", "-O", f"{temp_dir_path}/kafka.tgz", kafka_url])
    execute(["mkdir", f"{temp_dir_path}/fixtures/kafka"])
    execute(["tar", "xfz", f"{temp_dir_path}/kafka.tgz", "-C", f"{temp_dir_path}/fixtures/kafka", "--strip-components", "1"])
    failure_count = run_tests(f"{image}:{tag}", "jvm", temp_dir_path)
    shutil.rmtree(temp_dir_path)
    if failure_count != 0:
        raise SystemError("Test Failure. Error count is non 0")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("image", help="Image name that you want to keep for the Docker image")
    parser.add_argument("-tag", "--image-tag", default="latest", dest="tag", help="Image tag that you want to add to the image")
    parser.add_argument("-type", "--image-type", choices=["jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("-u", "--kafka-url", dest="kafka_url", help="Kafka url to be used to download kafka binary tarball in the docker image")
    parser.add_argument("-b", "--build", action="store_true", dest="build_only", default=False, help="Only build the image, don't run tests")
    parser.add_argument("-t", "--test", action="store_true", dest="test_only", default=False, help="Only run the tests, don't build the image")
    args = parser.parse_args()

    if args.image_type == "jvm" and (args.build_only or not (args.build_only or args.test_only)):
        if args.kafka_url:
            build_jvm(args.image, args.tag, args.kafka_url)
        else:
            raise ValueError("--kafka-url is a required argument for jvm image")
    
    if args.image_type == "jvm" and (args.test_only or not (args.build_only or args.test_only)):
        run_jvm_tests(args.image, args.tag, args.kafka_url)