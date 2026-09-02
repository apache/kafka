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

from pathlib import Path
import subprocess
import tempfile
import os
import shutil

SUPPORTED_CONTAINER_RUNTIMES = ("docker", "podman")

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

    kafka_archive_path = Path(temp_dir_path) / image_type / "kafka.tgz"
    if kafka_archive:
        shutil.copy(kafka_archive, kafka_archive_path)
    else:
        # Podman requires the COPY source to exist before kafka_url is
        # downloaded by the Dockerfile.
        kafka_archive_path.touch()
    command = command.replace("$DOCKER_FILE", f"{temp_dir_path}/{image_type}/Dockerfile")
    command = command.replace("$DOCKER_DIR", f"{temp_dir_path}/{image_type}")
    try:
        execute(command.split())
    except Exception as e:
        raise SystemError("Container image build failed") from e
    finally:
        shutil.rmtree(temp_dir_path)

def detect_container_runtime():
    configured_runtime = os.environ.get("CONTAINER_RUNTIME")

    if configured_runtime:
        if configured_runtime not in SUPPORTED_CONTAINER_RUNTIMES:
            raise ValueError(
                f"Unsupported container runtime: {configured_runtime}. "
                f"Supported runtimes: {', '.join(SUPPORTED_CONTAINER_RUNTIMES)}"
            )
        if shutil.which(configured_runtime) is None:
            raise RuntimeError(
                f"Container runtime '{configured_runtime}' was not found"
            )
        return configured_runtime

    for runtime in SUPPORTED_CONTAINER_RUNTIMES:
        if shutil.which(runtime):
            return runtime

    raise RuntimeError(
        "No supported container runtime found. "
        "Please install Docker or Podman, or set CONTAINER_RUNTIME."
    )
