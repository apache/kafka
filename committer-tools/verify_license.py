#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

import os
import re
import sys
import tarfile
import tempfile
import subprocess

# Constant: Regex to extract dependency tokens from the LICENSE file.
# Matches lines that start with a dash and then a dependency token of the form:
#   DependencyName-x.y, DependencyName-x.y.z, or DependencyName-x.y.z.w
# Optionally, a trailing suffix (e.g., "-alpha") is captured.
LICENSE_DEP_PATTERN = re.compile(
    r'^\s*-\s*([A-Za-z0-9_.+-]+-[0-9]+\.[0-9]+(?:\.[0-9]+){0,2}(?:[-.][A-Za-z0-9]+)?)',
    re.MULTILINE
)

def run_gradlew(project_dir):
    print("Running './gradlew clean releaseTarGz'")
    subprocess.run(["./gradlew", "clean", "releaseTarGz"], check=True, cwd=project_dir)

def get_tarball_path(project_dir):
    distributions_dir = os.path.join(project_dir, "core", "build", "distributions")
    if not os.path.isdir(distributions_dir):
        print("Error: Distributions directory not found:", distributions_dir)
        sys.exit(1)
    
    pattern = re.compile(r'^kafka_2\.13-.+\.tgz$', re.IGNORECASE)
    candidates = [
        os.path.join(distributions_dir, f)
        for f in os.listdir(distributions_dir)
        if pattern.match(f)
    ]
    if not candidates:
        print("Error: No tarball matching 'kafka_2.13-*.tgz' found in:", distributions_dir)
        sys.exit(1)
    
    tarball_path = max(candidates, key=os.path.getmtime)
    return tarball_path

def extract_tarball(tarball, extract_dir):
    with tarfile.open(tarball, "r:gz") as tar:
        # Use a filter to avoid future deprecation warnings.
        tar.extractall(path=extract_dir, filter=lambda tarinfo, dest: tarinfo)
    print("Tarball extracted to:", extract_dir)

def get_libs_set(libs_dir):
    return {
        fname[:-4]
        for fname in os.listdir(libs_dir)
        if fname.endswith(".jar") and not re.search(r"(kafka|connect|trogdor)", fname, re.IGNORECASE)
    }

def get_license_deps(license_text):
    return set(LICENSE_DEP_PATTERN.findall(license_text))

def main():
    # Assume the current working directory is the project root.
    project_dir = os.getcwd()
    print("Using project directory:", project_dir)
    
    # Build the tarball.
    run_gradlew(project_dir)
    tarball = get_tarball_path(project_dir)
    print("Tarball created at:", tarball)
    
    # Extract the tarball into a temporary directory.
    with tempfile.TemporaryDirectory() as tmp_dir:
        extract_tarball(tarball, tmp_dir)
        extracted_dirs = os.listdir(tmp_dir)
        if not extracted_dirs:
            print("Error: No directory found after extraction.")
            sys.exit(1)
        extracted = os.path.join(tmp_dir, extracted_dirs[0])
        print("Tarball extracted to:", extracted)
        
        # Locate the LICENSE file and libs directory.
        license_path = os.path.join(extracted, "LICENSE")
        libs_dir = os.path.join(extracted, "libs")
        if not os.path.exists(license_path) or not os.path.exists(libs_dir):
            print("Error: LICENSE file or libs directory not found in the extracted project.")
            sys.exit(1)
        
        with open(license_path, "r", encoding="utf-8") as f:
            license_text = f.read()
        
        # Get dependency sets.
        libs = get_libs_set(libs_dir)
        license_deps = get_license_deps(license_text)

        print("\nDependencies from libs (extracted from jar names):")
        for dep in sorted(libs):
            print(" -", dep)
        
        print("\nDependencies extracted from LICENSE file:")
        for dep in sorted(license_deps):
            print(" -", dep)
        
        # Compare the sets.
        missing_in_license = libs - license_deps
        extra_in_license = license_deps - libs

        if missing_in_license:
            print("\nThe following libs (from ./libs) are missing in the LICENSE file. These should be added to the LICENSE-binary file:")
            for dep in sorted(missing_in_license):
                print(" -", dep)
        else:
            print("\nAll libs from ./libs are present in the LICENSE file.")
        
        if extra_in_license:
            print("\nThe following entries are in the LICENSE file but not present in ./libs. These should be removed from the LICENSE-binary file:")
            for dep in sorted(extra_in_license):
                print(" -", dep)
        else:
            print("\nNo extra dependencies in the LICENSE file.")

        if missing_in_license or extra_in_license:
            sys.exit(1)

if __name__ == "__main__":
    main()

