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

import subprocess
from datetime import date
import argparse
from distutils.dir_util import copy_tree
import shutil

def build_jvm(image, tag, kafka_url):
    image = f'{image}:{tag}'
    copy_tree("resources", "jvm/resources")
    result = subprocess.run(["docker", "build", "-f", "jvm/Dockerfile", "-t", image, "--build-arg", f"kafka_url={kafka_url}",
                            "--build-arg", f'build_date={date.today()}', "jvm"])
    if result.stderr:
        print(result.stdout)
        return
    shutil.rmtree("jvm/resources")

def run_jvm_tests(image, tag, kafka_url):
    subprocess.run(["wget", "-nv", "-O", "kafka.tgz", kafka_url])
    subprocess.run(["ls"])
    subprocess.run(["mkdir", "./test/fixtures/kafka"])
    subprocess.run(["tar", "xfz", "kafka.tgz", "-C", "./test/fixtures/kafka", "--strip-components", "1"])
    subprocess.run(["python3", "docker_sanity_test.py", f"{image}:{tag}", "jvm"], cwd="test")
    subprocess.run(["rm", "kafka.tgz"])
    shutil.rmtree("./test/fixtures/kafka")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("image", help="Image name that you want to keep for the Docker image")
    parser.add_argument("-tag", "--image-tag", default="latest", dest="tag", help="Image tag that you want to add to the image")
    parser.add_argument("-type", "--image-type", default="all", dest="image_type", help="Image type you want to build. By default it's all")
    parser.add_argument("-u", "--kafka-url", dest="kafka_url", help="Kafka url to be used to download kafka binary tarball in the docker image")
    parser.add_argument("-b", "--build", action="store_true", dest="build_only", default=False, help="Only build the image, don't run tests")
    parser.add_argument("-t", "--test", action="store_true", dest="test_only", default=False, help="Only run the tests, don't build the image")
    args = parser.parse_args()

    if args.image_type in ("all", "jvm") and (args.build_only or not (args.build_only or args.test_only)):
        if args.kafka_url:
            build_jvm(args.image, args.tag, args.kafka_url)
        else:
            raise ValueError("--kafka-url is a required argument for jvm image")
    
    if args.image_type in ("all", "jvm") and (args.test_only or not (args.build_only or args.test_only)):
        run_jvm_tests(args.image, args.tag, args.kafka_url)