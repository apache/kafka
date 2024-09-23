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

import argparse
from collections import OrderedDict
from glob import glob
import logging
import os
import sys

import yaml


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


if __name__ == "__main__":
    """
    """
    parser = argparse.ArgumentParser(description="Convert tests from all-tests.txt into YAML.")
    parser.add_argument("--path",
                        required=False,
                        default="**/build/all-tests.txt",
                        help="Path to all-tests.txt files. Glob patterns are supported.")
    parser.add_argument("--yaml-output-dir",
                        required=False,
                        default="data/all-tests",
                        help="Directory to output YAML files")

    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    args = parser.parse_args()

    reports = glob(pathname=args.path, recursive=True)
    logger.debug(f"Found {len(reports)} all-tests.txt files")

    all_tests = {}
    for report in reports:
        with open(report, "r") as fp:
            logger.debug(f"Parsing {report}")
            for line in fp:
                toks = line.split()
                module = toks[0]
                if module not in all_tests:
                    all_tests[module] = OrderedDict()
                toks = toks[1].split("#")
                clazz = toks[0]
                if clazz not in all_tests[module]:
                    all_tests[module][clazz] = []
                method = toks[1].strip("()")
                all_tests[module][clazz].append(method)

    out_dir = args.yaml_output_dir
    if not os.path.exists(out_dir):
        logger.debug(f"Creating output directory {out_dir}.")
        os.makedirs(out_dir)

    for module, tests in all_tests.items():
        for test, methods in tests.items():
            methods.sort()
        out_path = os.path.join(out_dir, f"{module}-tests.yaml")
        logger.debug(f"Writing {len(tests)} tests for {module} into {out_path}.")
        stream = open(out_path, "w")
        yaml.dump(dict(tests), stream)
