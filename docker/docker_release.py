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
Python script to build and push a multiarch docker image
This script is used to prepare and publish docker release candidate

Pre requisites:
    Ensure that you are logged in the docker registry and you have access to push to that registry.
    Ensure that docker buildx is enabled for you.

Usage:
    docker_release.py --help
        Get detailed description of argument

    Example command:-
        docker_release <image> --kafka-url <kafka_url> --image-type <type>

        This command will build the multiarch image of type <type> (jvm by default),
        named <image> using <kafka_url> to download kafka and push it to the docker image name <image> provided.
        Make sure image is in the format of <registry>/<namespace>/<image_name>:<image_tag>.
"""

from datetime import date
import argparse

from common import execute, jvm_image

def build_push_jvm(image, kafka_url):
    try:
        create_builder()
        jvm_image(f"docker buildx build -f $DOCKER_FILE --build-arg kafka_url={kafka_url} --build-arg build_date={date.today()} --push \
              --platform linux/amd64,linux/arm64 --tag {image} $DOCKER_DIR")
    except:
        raise SystemError("Docker image push failed")
    finally:
        remove_builder()

def create_builder():
    execute(["docker", "buildx", "create", "--name", "kafka-builder", "--use"])

def remove_builder():
    execute(["docker", "buildx", "rm", "kafka-builder"])

if __name__ == "__main__":
    print("\
          This script will build and push docker images of apache kafka.\n \
          Please ensure that image has been sanity tested before pushing the image. \n \
          Please ensure you are logged in the docker registry that you are trying to push to.")
    parser = argparse.ArgumentParser()
    parser.add_argument("image", help="Dockerhub image that you want to push to (in the format <registry>/<namespace>/<image_name>:<image_tag>)")
    parser.add_argument("--image-type", "-type", choices=["jvm"], default="jvm", dest="image_type", help="Image type you want to build")
    parser.add_argument("--kafka-url", "-u", dest="kafka_url", help="Kafka url to be used to download kafka binary tarball in the docker image")
    args = parser.parse_args()

    print(f"Docker image of type {args.image_type} containing kafka downloaded from {args.kafka_url} will be pushed to {args.image}")

    print("Building and pushing the image")
    if args.image_type == "jvm":
        build_push_jvm(args.image, args.kafka_url)
    print(f"Image has been pushed to {args.image}")
