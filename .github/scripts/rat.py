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

from glob import glob
import logging
import os
import sys
import xml.etree.ElementTree

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def get_env(key: str) -> str:
    value = os.getenv(key)
    logger.debug(f"Read env {key}: {value}")
    return value


def parse_rat_report(fp):
    file_count = 0
    unapproved_licenses = []
    approved_count = 0
    root = xml.etree.ElementTree.parse(fp).getroot()

    for resource in root.findall(".//resource"):
        file_name = resource.get("name")
        license_approval_elem = resource.find("license-approval")

        if license_approval_elem is not None and license_approval_elem.get("name") == "false":
            file_count += 1
            unapproved_licenses.append(file_name)
        else:
            approved_count += 1

    return approved_count, file_count, unapproved_licenses


if __name__ == "__main__":
    """
    Parse Apache Rat reports and generate GitHub annotations.
    """
    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    reports = glob(pathname="**/rat/*.xml", recursive=True)
    logger.debug(f"Found {len(reports)} Rat reports")

    total_unapproved_licenses = 0
    total_approved_licenses = 0
    all_unapproved_files = []

    workspace_path = get_env("GITHUB_WORKSPACE")

    for report in reports:
        with open(report, "r") as fp:
            logger.debug(f"Parsing Rat report file: {report}")
            approved_count, unapproved_count, unapproved_files = parse_rat_report(fp)

            total_approved_licenses += approved_count
            total_unapproved_licenses += unapproved_count
            all_unapproved_files.extend(unapproved_files)

    if total_unapproved_licenses == 0:
        print(f"All {total_approved_licenses} files have approved licenses. No unapproved licenses found.")
        exit(0)
    else:
        print(f"{total_approved_licenses} approved licenses")
        print(f"{total_unapproved_licenses} unapproved licenses")

        print("Files with unapproved licenses:")
        for file in all_unapproved_files:
            rel_path = os.path.relpath(file, workspace_path)
            message = f"File with unapproved license: {rel_path}"
            print(f"::notice file={rel_path},title=Unapproved License::{message}")

        exit(1)
