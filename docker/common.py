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
import shutil

def execute(command):
    if subprocess.run(command).returncode != 0:
        raise SystemError("Failure in executing following command:- ", " ".join(command))

def get_input(message):
    value = input(message)
    if value == "":
        raise ValueError("This field cannot be empty")
    return value

def build_docker_image_runner(command, image_type, kafka_archive=None):
    temp_dir_path = tempfile.mkdtemp()
    current_dir = os.path.dirname(os.path.realpath(__file__))
    shutil.copytree(f"{current_dir}/{image_type}", f"{temp_dir_path}/{image_type}", dirs_exist_ok=True)
    shutil.copytree(f"{current_dir}/resources", f"{temp_dir_path}/{image_type}/resources", dirs_exist_ok=True)
    shutil.copy(f"{current_dir}/server.properties", f"{temp_dir_path}/{image_type}")
    if kafka_archive:
        shutil.copy(kafka_archive, f"{temp_dir_path}/{image_type}/kafka.tgz")
    command = command.replace("$DOCKER_FILE", f"{temp_dir_path}/{image_type}/Dockerfile")
    command = command.replace("$DOCKER_DIR", f"{temp_dir_path}/{image_type}")
    try:
        execute(command.split())
    except:
        raise SystemError("Docker Image Build failed")
    finally:
        shutil.rmtree(temp_dir_path)
