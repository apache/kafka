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
Python script to extract docker official images artifact and give it executable permissions
This script is used to extract docker official images artifact and give it executable permissions

Usage:
    extract_docker_official_image_artifact.py --help
        Get detailed description of each option

    Example command:-
        extract_docker_official_image_artifact.py --path_to_downloaded_artifact <artifact_path>

        This command will build an extract the downloaded artifact, and copy the contents to the 
        docker_official_images directory. If the extracted artifact contents already exist in the 
        docker_official_images directory , they will be overwritten, else they will be created.
        
"""
import os
import argparse
import zipfile
import shutil
from pathlib import Path

def set_executable_permissions(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            path = os.path.join(root, file)
            os.chmod(path, os.stat(path).st_mode | 0o111)


def extract_artifact(artifact_path):
    docker_official_images_dir = Path(os.path.dirname(os.path.realpath(__file__)), "docker_official_images")
    temp_dir = Path('temp_extracted')
    try:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)  
        temp_dir.mkdir()
        with zipfile.ZipFile(artifact_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        artifact_version_dirs = list(temp_dir.iterdir())
        if len(artifact_version_dirs) != 1:
            raise Exception("Unexpected contents in the artifact. Exactly one version directory is expected.")
        artifact_version_dir = artifact_version_dirs[0]
        target_version_dir =  Path(os.path.join(docker_official_images_dir, artifact_version_dir.name))
        target_version_dir.mkdir(parents=True, exist_ok=True)
        for image_type_dir in artifact_version_dir.iterdir():
            target_image_type_dir = Path(os.path.join(target_version_dir, image_type_dir.name))
            if target_image_type_dir.exists():
                shutil.rmtree(target_image_type_dir)            
            shutil.copytree(image_type_dir, target_image_type_dir)
            set_executable_permissions(target_image_type_dir)
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path_to_downloaded_artifact", "-artifact_path", required=True,
                        dest="artifact_path", help="Path to zipped artifacy downloaded from github actions workflow.")
    args = parser.parse_args()
    extract_artifact(args.artifact_path)