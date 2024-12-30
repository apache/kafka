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
import os.path
import sys
from typing import Tuple, Optional
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


def parse_report(workspace_path, fp) -> Tuple[int, int]:
    stack = []
    errors = []
    file_count = 0
    error_count = 0
    for (event, elem) in xml.etree.ElementTree.iterparse(fp, events=["start", "end"]):
        if event == "start":
            stack.append(elem)   
            if elem.tag == "file":
                file_count += 1
                errors.clear()
            if elem.tag == "error":
                logger.debug(f"Found checkstyle error: {elem.attrib}")
                errors.append(elem)
                error_count += 1
        elif event == "end":
            if elem.tag == "file" and len(errors) > 0:
                filename = elem.get("name")
                rel_path = os.path.relpath(filename, workspace_path)
                logger.debug(f"Outputting errors for file: {elem.attrib}")
                for error in errors:
                    line = error.get("line")
                    col = error.get("column")
                    severity = error.get("severity")
                    message = error.get('message')
                    title = f"Checkstyle {severity}"
                    print(f"::notice file={rel_path},line={line},col={col},title={title}::{message}")
            stack.pop()
        else:
            logger.error(f"Unhandled xml event {event}: {elem}")
    return file_count, error_count


if __name__ == "__main__":
    """
    Parse checkstyle XML reports and generate GitHub annotations.

    See: https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/workflow-commands-for-github-actions#setting-a-notice-message
    """
    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)
    
    reports = glob(pathname="**/checkstyle/*.xml", recursive=True)
    logger.debug(f"Found {len(reports)} checkstyle reports")
    total_file_count = 0
    total_error_count = 0

    workspace_path = get_env("GITHUB_WORKSPACE") # e.g., /home/runner/work/apache/kafka

    for report in reports:
        with open(report, "r") as fp:
            logger.debug(f"Parsing report file: {report}")
            file_count, error_count = parse_report(workspace_path, fp)
            if error_count == 1:
                logger.debug(f"Checked {file_count} files from {report} and found 1 error")
            else:
                logger.debug(f"Checked {file_count} files from {report} and found {error_count} errors")
            total_file_count += file_count
            total_error_count += error_count
    exit(0)
