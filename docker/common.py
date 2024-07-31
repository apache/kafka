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
import tempfile
import os
from distutils.dir_util import copy_tree
import json
import shutil
import sys
import re

def load_version_gpg_keys():
    '''
    Loads the version-specific GPG keys from the 'version_gpg_keys.json' file.
    '''
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_file = os.path.join(script_dir, 'version_gpg_keys.json')
    with open(json_file, 'r') as f:
        version_gpg_keys = json.load(f)
    return version_gpg_keys

def get_gpg_key(kafka_version):
    """
    Retrieves the GPG key for the specified kafka version, if it exists, from docker/version_gpg_keys.py.
    """
    version_gpg_keys = load_version_gpg_keys()
    gpg_key = version_gpg_keys.get(kafka_version)
    if gpg_key is not None:
        return gpg_key
    else:
        print(f"No GPG Key data exists for kafka version {kafka_version}.")
        print("Please ensure an entry corresponding to it exists under docker/version_gpg_keys.py")
        sys.exit(1)

def get_kafka_version_from_url(kafka_url):
    """
    Retrives the major.minor.patch (x.x.x) version from the given Kafka URL.
    """
    match = re.search("\d+\.\d+\.\d+", kafka_url)
    if match:
        return match.group(0)
    else:
        print(f"No pattern found matching x.x.x in {kafka_url}. No version number extracted")
        sys.exit(1)

def execute(command):
    if subprocess.run(command).returncode != 0:
        raise SystemError("Failure in executing following command:- ", " ".join(command))

def get_input(message):
    value = input(message)
    if value == "":
        raise ValueError("This field cannot be empty")
    return value

def build_docker_image_runner(command, image_type):
    temp_dir_path = tempfile.mkdtemp()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    copy_tree(f"{current_dir}/{image_type}", f"{temp_dir_path}/{image_type}")
    copy_tree(f"{current_dir}/resources", f"{temp_dir_path}/{image_type}/resources")
    command = command.replace("$DOCKER_FILE", f"{temp_dir_path}/{image_type}/Dockerfile")
    command = command.replace("$DOCKER_DIR", f"{temp_dir_path}/{image_type}")
    try:
        execute(command.split())
    except:
        raise SystemError("Docker Image Build failed")
    finally:
        shutil.rmtree(temp_dir_path)
