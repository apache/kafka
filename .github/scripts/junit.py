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

import dataclasses
import datetime
from functools import partial
from glob import glob
import logging
import os
import os.path
import sys
from typing import Dict, Tuple, Optional, List, Iterable
import xml.etree.ElementTree


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

FAILED = "FAILED ❌"
FLAKY = "FLAKY ⚠️ "
SKIPPED = "SKIPPED ⚠️ "

def get_env(key: str) -> str:
    value = os.getenv(key)
    logger.debug(f"Read env {key}: {value}")
    return value


@dataclasses.dataclass
class TestCase:
    test_name: str
    class_name: str
    time: float
    failure_message: Optional[str]
    failure_class: Optional[str]
    failure_stack_trace: Optional[str]

    def key(self) -> Tuple[str, str]:
        return (self.class_name, self.test_name)


@dataclasses.dataclass
class TestSuite:
    name: str
    path: str
    tests: int
    skipped: int
    failures: int
    errors: int
    time: float
    failed_tests: List[TestCase]
    skipped_tests: List[TestCase]
    passed_tests: List[TestCase]

    def errors_and_failures(self) -> int:
        return self.errors + self.failures


def parse_report(workspace_path, report_path, fp) -> Iterable[TestSuite]:
    cur_suite: Optional[TestSuite] = None
    cur_test: Optional[Tuple[str, str]] = None  # (test class, test name)
    partial_test_failure = None
    test_case_failed = False
    for (event, elem) in xml.etree.ElementTree.iterparse(fp, events=["start", "end"]):
        if event == "start":
            if elem.tag == "testsuite":
                name = elem.get("name")
                tests = int(elem.get("tests", 0))
                skipped = int(elem.get("skipped", 0))
                failures = int(elem.get("failures", 0))
                errors = int(elem.get("errors", 0))
                suite_time = float(elem.get("time", 0.0))
                cur_suite = TestSuite(name, report_path, tests, skipped, failures, errors, suite_time, [], [], [])
            elif elem.tag == "testcase":
                test_name = elem.get("name")
                class_name = elem.get("classname")
                test_time = float(elem.get("time", 0.0))
                partial_test_case = partial(TestCase, test_name, class_name, test_time)
                cur_test = (class_name, test_name)
                test_case_failed = False
            elif elem.tag == "failure":
                failure_message = elem.get("message")
                failure_class = elem.get("type")
                failure_stack_trace = elem.text
                failure = partial_test_case(failure_message, failure_class, failure_stack_trace)
                cur_suite.failed_tests.append(failure)
                test_case_failed = True
            elif elem.tag == "skipped":
                skipped = partial_test_case(None, None, None)
                cur_suite.skipped_tests.append(skipped)
            else:
                pass
        elif event == "end":
            if elem.tag == "testcase":
                if not test_case_failed:
                    passed = partial_test_case(None, None, None)
                    cur_suite.passed_tests.append(passed)
                cur_test = None
                partial_test_failure = None
            elif elem.tag == "testsuite":
                yield cur_suite
                cur_suite = None
        else:
            logger.error(f"Unhandled xml event {event}: {elem}")


def pretty_time_duration(seconds: float) -> str:
    time_min, time_sec = divmod(int(seconds), 60)
    time_hour, time_min = divmod(time_min, 60)
    time_fmt = ""
    if time_hour > 0:
        time_fmt += f"{time_hour}h"
    if time_min > 0:
        time_fmt += f"{time_min}m"
    time_fmt += f"{time_sec}s"
    return time_fmt


if __name__ == "__main__":
    """
    Parse JUnit XML reports and generate GitHub job summary in Markdown format.

    Exits with status code 0 if no tests failed, 1 otherwise.
    """
    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    reports = glob(pathname="**/test-results/**/*.xml", recursive=True)
    logger.debug(f"Found {len(reports)} JUnit results")
    workspace_path = get_env("GITHUB_WORKSPACE") # e.g., /home/runner/work/apache/kafka

    total_file_count = 0
    total_tests = 0
    total_skipped = 0
    total_failures = 0
    total_flaky = 0
    total_errors = 0
    total_time = 0
    failed = []
    skipped = []
    for report in reports:
        with open(report, "r") as fp:
            logger.debug(f"Parsing {report}")
            for suite in parse_report(workspace_path, report, fp):
                total_tests += suite.tests
                total_skipped += suite.skipped
                total_failures += suite.failures
                total_errors += suite.errors
                total_time += suite.time

                # Due to how the Develocity Test Retry plugin interacts with our geneated ClusterTests, we can see
                # tests pass and then fail in the same run. Because of this, we need to capture all passed and all
                # failed for each suite. Then we can find flakes by taking the intersection of those two.
                all_suite_passed = {test.key() for test in suite.passed_tests}
                all_suite_failed = {test.key() for test in suite.failed_tests}
                flaky = all_suite_passed & all_suite_failed
                total_flaky += len(flaky)

                # Display failures first
                for test_failure in suite.failed_tests:
                    if test_failure.key() in flaky:
                        continue
                    logger.debug(f"Found test failure: {test_failure}")
                    simple_class_name = test_failure.class_name.split(".")[-1]
                    failed.append((simple_class_name, test_failure.test_name, FAILED, test_failure.failure_message, f"{test_failure.time:0.2f}s"))
                for test_failure in suite.failed_tests:
                    if test_failure.key() not in flaky:
                        continue
                    logger.debug(f"Found flaky test: {test_failure}")
                    simple_class_name = test_failure.class_name.split(".")[-1]
                    failed.append((simple_class_name, test_failure.test_name, FLAKY, test_failure.failure_message, f"{test_failure.time:0.2f}s"))
                for skipped_test in suite.skipped_tests:
                    simple_class_name = skipped_test.class_name.split(".")[-1]
                    logger.debug(f"Found skipped test: {skipped_test}")
                    skipped.append((simple_class_name, skipped_test.test_name))
    duration = pretty_time_duration(total_time)
    logger.info(f"Finished processing {len(reports)} reports")

    # Print summary
    report_url = get_env("REPORT_URL")
    report_md = f"Download [HTML report]({report_url})."
    summary = f"{total_tests} tests run in {duration}, {total_failures} {FAILED}, {total_flaky} {FLAKY}, {total_skipped} {SKIPPED}, and {total_errors} errors."
    print("## Test Summary")
    print(f"{summary} {report_md}")
    if len(failed) > 0:
        logger.info(f"Found {len(failed)} test failures:")
        print("## Failed Tests")
        print(f"| Module | Test | Result | Message | Time |")
        print(f"| ------ | ---- | ------ | ------- | ---- |")
        for row in failed:
            logger.info(f"{row[2]} {row[0]} > {row[1]}")
            row_joined = " | ".join(row)
            print(f"| {row_joined} |")
    print("\n")
    if len(skipped) > 0:
        print("<details>")
        print("<summary>Skipped Tests</summary>")
        print("\n")
        print(f"| Module | Test |")
        print(f"| ------ | ---- |")
        for row in skipped:
            row_joined = " | ".join(row)
            print(f"| {row_joined} |")
        print("</details>")

    logger.debug(summary)
    if total_failures > 0:
        logger.debug(f"Failing this step due to {total_failures} test failures")
        exit(1)
    elif total_errors > 0:
        logger.debug(f"Failing this step due to {total_errors} test errors")
        exit(1)
    else:
        exit(0)
