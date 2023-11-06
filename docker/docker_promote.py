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
Python script to promote an rc image.

Follow the interactive guide to pull an RC image and promote it desired dockerhub repository.

Usage: docker_promote.py

Interactive utility to promote a docker image
"""

import subprocess
import requests
from getpass import getpass

def execute(command):
    if subprocess.run(command).returncode != 0:
        raise SystemError("Failure in executing following command:- ", " ".join(command))

def login():
    execute(["docker", "login"])

def pull(rc_image, promotion_image):
    execute(["docker", "pull", "--platform=linux/amd64", rc_image])
    execute(["docker", "tag", rc_image, f"{promotion_image}-amd64"])
    execute(["docker", "pull", "--platform=linux/arm64", rc_image])
    execute(["docker", "tag", rc_image, f"{promotion_image}-arm64"])

def push(promotion_image):
    execute(["docker", "push", f"{promotion_image}-amd64"])
    execute(["docker", "push", f"{promotion_image}-arm64"])

def push_manifest(promotion_image):
    execute(["docker", "manifest", "create", promotion_image, 
        "--amend", f"{promotion_image}-amd64",
        "--amend", f"{promotion_image}-arm64"])
    
    execute(["docker", "manifest", "push", promotion_image])

def remove(promotion_image_namespace, promotion_image_name, promotion_image_tag, token):
    if requests.delete(f"https://hub.docker.com/v2/repositories/{promotion_image_namespace}/{promotion_image_name}/tags/{promotion_image_tag}-amd64", headers={"Authorization": f"JWT {token}"}).status_code != 204:
        raise SystemError(f"Failed to delete redundant images from dockerhub. Please make sure {promotion_image_namespace}/{promotion_image_name}:{promotion_image_tag}-amd64 is removed from dockerhub")
    if requests.delete(f"https://hub.docker.com/v2/repositories/{promotion_image_namespace}/{promotion_image_name}/tags/{promotion_image_tag}-arm64", headers={"Authorization": f"JWT {token}"}).status_code != 204:
        raise SystemError(f"Failed to delete redundant images from dockerhub. Please make sure {promotion_image_namespace}/{promotion_image_name}:{promotion_image_tag}-arm64 is removed from dockerhub")
    subprocess.run(["docker", "rmi", f"{promotion_image_namespace}/{promotion_image_name}:{promotion_image_tag}-amd64"])
    subprocess.run(["docker", "rmi", f"{promotion_image_namespace}/{promotion_image_name}:{promotion_image_tag}-arm64"])

if __name__ == "__main__":
    login()
    username = input("Enter dockerhub username: ")
    password = getpass("Enter dockerhub password: ")

    token = (requests.post("https://hub.docker.com/v2/users/login/", json={"username": username, "password": password})).json()['token']
    if len(token) == 0:
        raise PermissionError("Dockerhub login failed")

    rc_image = input("Enter the RC docker image that you want to pull (in the format <registry>:<image_name>:<image_tag>): ")
    promotion_image_namespace = input("Enter the dockerhub namespace that the rc image needs to be promoted to [example: apache]: ")
    promotion_image_name = input("Enter the dockerhub image name that the rc image needs to be promoted to [example: kafka]: ")
    promotion_image_tag = input("Enter the dockerhub image tag that the rc image needs to be promoted to [example: 4.0.0]: ")
    promotion_image = f"{promotion_image_namespace}/{promotion_image_name}:{promotion_image_tag}"

    pull(rc_image, promotion_image)
    push(promotion_image)
    push_manifest(promotion_image)
    remove(promotion_image_namespace, promotion_image_name, promotion_image_tag, token)
    print("The image has been promoted successfully. The promoted image should be accessible in dockerhub")