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
Python script to build and push docker image
This script is used to prepare and publish docker release candidate

Usage: docker_release.py

Interactive utility to push the docker image to dockerhub
"""

from distutils.dir_util import copy_tree
from datetime import date

from common import execute, get_input, jvm_image

def build_push_jvm(image, kafka_url):
    jvm_image(f"docker buildx build -f $DOCKER_FILE --build-arg kafka_url={kafka_url} --build-arg build_date={date.today()} --push \
              --platform linux/amd64,linux/arm64 --tag {image} $DOCKER_DIR")

def login():
    execute(["docker", "login"])

def create_builder():
    execute(["docker", "buildx", "create", "--name", "kafka-builder", "--use"])

def remove_builder():
    execute(["docker", "buildx", "rm", "kafka-builder"])

def get_input(message):
    value = input(message)
    if value == "":
        raise ValueError("This field cannot be empty")
    return value

if __name__ == "__main__":
    print("\
          This script will build and push docker images of apache kafka.\n\
          Please ensure that image has been sanity tested before pushing the image")
    docker_registry = input("Enter the docker registry you want to push the image to [docker.io]: ")
    if docker_registry == "":
        docker_registry = "docker.io"
    if docker_registry == "docker.io":
        login()
    else:
        print("Please make sure you are logged in to your docker registry and continue")
    docker_namespace = input("Enter the docker namespace you want to push the image to: ")
    image_name = get_input("Enter the image name: ")
    image_tag = get_input("Enter the image tag for the image: ")
    kafka_url = get_input("Enter the url for kafka binary tarball: ")
    image = f"{docker_registry}/{docker_namespace}/{image_name}:{image_tag}"
    print(f"Docker image containing kafka downloaded from {kafka_url} will be pushed to {image}")
    proceed = input("Should we proceed? [y/N]: ")
    if proceed == "y":
        print("Building and pushing the image")
        create_builder()
        build_push_jvm(image, kafka_url)
        remove_builder()
        print(f"Image has been pushed to {image}")
    else:
        print("Image push aborted")
