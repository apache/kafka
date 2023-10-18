#!/usr/bin/env python

#
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
#

"""
Python script to build and push docker image
Usage: docker_release.py

Interactive utility to push the docker image to dockerhub
"""

import subprocess
from distutils.dir_util import copy_tree
from datetime import date
import shutil

def push_jvm(docker_account, image_name, image_tag, kafka_url):
    copy_tree("resources", "jvm/resources")
    subprocess.run(["docker", "buildx", "build", "-f", "jvm/Dockerfile", "--build-arg", f"kafka_url={kafka_url}", "--build-arg", f"build_date={date.today()}",
    "--push",
    "--platform", "linux/amd64,linux/arm64",
    "--tag", f"{docker_account}/{image_name}:{image_tag}", "jvm"])
    shutil.rmtree("jvm/resources")

def login():
    subprocess.run(["docker", "login"])

def create():
    subprocess.run(["docker", "buildx", "create", "--name", "kafka-builder", "--use"])

def remove():
    subprocess.run(["docker", "buildx", "rm", "kafka-builder"])

if __name__ == "__main__":
    print("\
          This script will build and push docker images of apache kafka.\n\
          Please ensure that image has been sanity tested before pushing the image")
    login()
    create()
    docker_account = input("Enter the dockerhub account you want to push the image to: ")
    image_name = input("Enter the image name: ")
    image_tag = input("Enter the image tag for the image: ")
    kafka_url = input("Enter the url for kafka binary tarball: ")
    push_jvm(docker_account, image_name, image_tag, kafka_url)
    remove()